namespace GraphBLAS.FSharp.Benchmarks.RealData

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Objects.MatrixExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Benchmarks

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixShapeColumn("RowCount", (fun (matrix,_) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixShapeColumn("ColumnCount", (fun (matrix,_) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixShapeColumn(
                "NNZ",
                fun (matrix,_) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            MatrixShapeColumn(
                "SqrNNZ",
                fun (_,matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type EWiseAddBenchmarks<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix: Matrix.COO<_> -> Matrix<_>) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatricesProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader*MtxReader> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatricesProviderBuilder pathToConfig =
        let datasetFolder = "EWiseAdd"
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" ->
                    MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                    , MtxReader(Utils.getFullPathToMatrix datasetFolder ("squared_" + matrixFilename))
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.EWiseAddition() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    member this.ReadMatrices() =
        firstMatrixHost <- this.ReadMatrix <| fst this.InputMatrixReader
        secondMatrixHost <- this.ReadMatrix <| snd this.InputMatrixReader

    member this.LoadMatricesToGPU () =
        firstMatrix <- (buildMatrix firstMatrixHost).ToDevice this.OclContext
        secondMatrix <- (buildMatrix secondMatrixHost).ToDevice this.OclContext

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type EWiseAddBenchmarksWithoutDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices ()
        this.LoadMatricesToGPU ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputMatrices()

    [<Benchmark>]
    override this.Benchmark () =
        this.EWiseAddition()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type EWiseAddBenchmarksWithDataTransfer<'matrixT, 'elem when 'matrixT :> IDeviceMemObject and 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix,
        resultToHost) =

    inherit EWiseAddBenchmarks<'matrixT, 'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup () =
        this.ReadMatrices ()

    [<GlobalCleanup>]
    override this.GlobalCleanup () = ()

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearInputMatrices()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark () =
        this.LoadMatricesToGPU()
        this.EWiseAddition()
        this.Processor.PostAndReply Msg.MsgNotifyMe
        resultToHost this.ResultMatrix this.Processor |> ignore
        this.Processor.PostAndReply Msg.MsgNotifyMe

type Map2Float32COOWithoutTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> Matrix.elementwise context ArithmeticOperations.float32Sum wgSize HostInterop),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type Map2Float32COOWithTransfer() =

    inherit EWiseAddBenchmarksWithDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> Matrix.elementwise context ArithmeticOperations.float32Sum wgSize HostInterop),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO,
        (fun matrix -> matrix.ToHost)
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"


type Map2BoolCOOWithoutTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context wgSize -> Matrix.elementwise context ArithmeticOperations.boolSum wgSize HostInterop),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.COO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCOO.txt"


type Map2Float32CSRWithoutTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context wgSize -> Matrix.elementwise context ArithmeticOperations.float32Sum wgSize HostInterop),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32CSR.txt"


type Map2BoolCSRWithoutTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context wgSize -> Matrix.elementwise context ArithmeticOperations.boolSum wgSize HostInterop),
        (fun _ -> true),
        (fun _ -> true),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

// With AtLeastOne

type EWiseAddAtLeastOneBenchmarks4BoolCOOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<bool>,bool>(
        (fun context wgSize -> Matrix.elementwiseAtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize HostInterop),
        (fun _ -> true),
        (fun _ -> true),
        Matrix.COO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4BoolCSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<bool>,bool>(
        (fun context wgSize -> Matrix.elementwiseAtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize HostInterop),
        (fun _ -> true),
        (fun _ -> true),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_, _>.InputMatricesProviderBuilder "EWiseAddBenchmarks4BoolCSR.txt"

type EWiseAddAtLeastOneBenchmarks4Float32COOWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.COO<float32>,float32>(
        (fun context wgSize -> Matrix.elementwiseAtLeastOne context ArithmeticOperations.float32SumAtLeastOne wgSize HostInterop),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        Matrix.COO
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"

type EWiseAddAtLeastOneBenchmarks4Float32CSRWithoutDataTransfer() =

    inherit EWiseAddBenchmarksWithoutDataTransfer<ClMatrix.CSR<float32>,float32>(
        (fun context wgSize -> Matrix.elementwiseAtLeastOne context ArithmeticOperations.float32SumAtLeastOne wgSize HostInterop),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun matrix -> Matrix.CSR matrix.ToCSR)
        )

    static member InputMatricesProvider =
        EWiseAddBenchmarks<_,_>.InputMatricesProviderBuilder "EWiseAddBenchmarks4Float32COO.txt"
