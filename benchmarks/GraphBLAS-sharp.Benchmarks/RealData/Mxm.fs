namespace GraphBLAS.FSharp.Benchmarks.RealData

open System.IO
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type MxmBenchmarks<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    let mutable funToBenchmark = None
    let mutable funCSR2CSC = None
    let mutable funCSC2CSR = None

    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable mask = Unchecked.defaultof<ClMatrix<_>>

    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>
    let mutable maskHost = Unchecked.defaultof<Matrix<_>>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get, set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatrixProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader * MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatrixProviderBuilder pathToConfig =
        let datasetFolder = "Mxm"
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

    member this.FunCSR2CSC =
        match funCSR2CSC with
        | None ->
            let x = Matrix.toCSCInplace this.OclContext this.WorkGroupSize
            funCSR2CSC <- Some x
            x
        | Some x -> x

    member this.FunCSC2CSR =
        match funCSC2CSR with
        | None ->
            let x = Matrix.toCSRInplace this.OclContext this.WorkGroupSize
            funCSC2CSR <- Some x
            x
        | Some x -> x

    member this.ReadMatrix (reader: MtxReader) =
        let converter =
            match reader.Field with
            | Pattern -> converterBool
            | _ -> converter

        reader.ReadMatrix converter

    member this.Mxm() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix mask

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor
        mask.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    member this.ReadMask(maskReader) =
        maskHost <- Matrix.COO <| this.ReadMatrix maskReader

    member this.ReadMatrices() =
        let matrixReader, maskReader = this.InputMatrixReader
        firstMatrixHost <- this.ReadMatrix matrixReader
        secondMatrixHost <- this.ReadMatrix matrixReader
        this.ReadMask(maskReader)

    member this.LoadMatricesToGPU () =
        firstMatrix <- buildMatrix this.OclContext firstMatrixHost
        secondMatrix <- buildMatrix this.OclContext secondMatrixHost
        mask <- maskHost.ToDevice this.OclContext

    member this.ConvertSecondMatrixToCSC() =
        secondMatrix <- this.FunCSR2CSC this.Processor HostInterop secondMatrix

    member this.ConvertSecondMatrixToCSR() =
        secondMatrix <- this.FunCSC2CSR this.Processor HostInterop secondMatrix

    abstract member GlobalSetup : unit -> unit

    abstract member Benchmark : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

type MxmBenchmarksMultiplicationOnly<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit MxmBenchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices ()
        this.LoadMatricesToGPU ()
        this.ConvertSecondMatrixToCSC()

    [<Benchmark>]
    override this.Benchmark () =
        this.Mxm()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<IterationCleanup>]
    override this.IterationCleanup () =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup () =
        this.ClearInputMatrices()

type MxmBenchmarksWithTransposing<'elem when 'elem : struct>(
        buildFunToBenchmark,
        converter: string -> 'elem,
        converterBool,
        buildMatrix) =

    inherit MxmBenchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        converterBool,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrices()
        this.LoadMatricesToGPU ()

    [<Benchmark>]
    override this.Benchmark() =
        this.ConvertSecondMatrixToCSC()
        this.Mxm()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)


    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ConvertSecondMatrixToCSR()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrices()

type Mxm4Float32MultiplicationOnly() =

    inherit MxmBenchmarksMultiplicationOnly<float32>(
        Matrix.mxm (Operations.add ()) (Operations.mult ()),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type Mxm4Float32WithTransposing() =

    inherit MxmBenchmarksWithTransposing<float32>(
        Matrix.mxm (Operations.add ()) (Operations.mult ()),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type Mxm4BoolMultiplicationOnly() =

    inherit MxmBenchmarksMultiplicationOnly<bool>(
        (Matrix.mxm Operations.logicalOr Operations.logicalAnd),
        (fun _ -> true),
        (fun _ -> true),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Bool.txt"

type Mxm4BoolWithTransposing() =

    inherit MxmBenchmarksWithTransposing<bool>(
        (Matrix.mxm Operations.logicalOr Operations.logicalAnd),
        (fun _ -> true),
        (fun _ -> true),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Bool.txt"

type MxmBenchmarks4Float32MultiplicationOnlyWithZerosFilter() =

    inherit MxmBenchmarksMultiplicationOnly<float32>(
        (Matrix.mxm Operations.addWithFilter (Operations.mult ())),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"

type MxmBenchmarks4Float32WithTransposingWithZerosFilter() =

    inherit MxmBenchmarksWithTransposing<float32>(
        Matrix.mxm Operations.addWithFilter (Operations.mult ()),
        float32,
        (fun _ -> Utils.nextSingle (System.Random())),
        (fun context matrix -> ClMatrix.CSR <| matrix.ToCSR.ToDevice context)
        )

    static member InputMatrixProvider =
        MxmBenchmarks<_>.InputMatrixProviderBuilder "MxmBenchmarks4Float32.txt"
