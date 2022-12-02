namespace GraphBLAS.FSharp.Benchmarks

open System.IO
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Jobs
open BenchmarkDotNet.Toolchains.InProcess.Emit
open BenchmarkDotNet.Toolchains.InProcess.NoEmit
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Columns
open Brahma.FSharp
open Microsoft.FSharp.Control
open Backend.Common.StandardOperations
open Backend.Algorithms.BFS
open Microsoft.FSharp.Core

type BFSConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixColumn("RowCount", (fun matrix -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixColumn("ColumnCount", (fun matrix -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixColumn(
                "NNZ",
                fun matrix ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        //TODO()
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithToolchain(InProcessEmitToolchain.Instance)
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<BFSConfig>)>]
type BFSBenchmarks<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    binaryConverter,
    vertex: int)
    =

    let mutable funToBenchmark = None
    let mutable matrix = Unchecked.defaultof<Backend.CSRMatrix<'elem>>
    let mutable matrixHost = Unchecked.defaultof<_>

    member val ResultLevels = Unchecked.defaultof<ClVector<'elem>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<ParamsSource("InputMatrixProvider")>]
    member val InputMatrixReader = Unchecked.defaultof<MtxReader> with get, set

    member this.OclContext:ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    static member InputMatrixProviderBuilder pathToConfig =
        let datasetFolder = "BFS"
        pathToConfig
        |> Utils.getMatricesFilenames
        |> Seq.map
            (fun matrixFilename ->
                printfn "%A" matrixFilename

                match Path.GetExtension matrixFilename with
                | ".mtx" -> MtxReader(Utils.getFullPathToMatrix datasetFolder matrixFilename)
                | _ -> failwith "Unsupported matrix format")

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.RunBFS() =
        this.ResultLevels <- this.FunToBenchmark this.Processor matrix vertex

    member this.ClearInputMatrix() =
        (matrix :> IDeviceMemObject).Dispose this.Processor

    member this.ClearResult() = this.ResultLevels.Dispose this.Processor

    member this.ReadMatrix() =
        let converter =
            match this.InputMatrixReader.Field with
            | Pattern -> binaryConverter
            | _ -> converter

        matrixHost <- this.InputMatrixReader.ReadMatrix binaryConverter

    member this.LoadMatrixToGPU() =
        matrix <- GraphBLAS.FSharp.CSRMatrix<int>.ToBackend this.OclContext matrixHost

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit

type BFSBenchmarksWithoutDataTransfer<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    boolConverter,
    vertex) =

    inherit BFSBenchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        boolConverter,
        vertex)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()
        this.LoadMatrixToGPU()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrix()

    [<Benchmark>]
    override this.Benchmark() =
        this.RunBFS()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type BFSBenchmarksWithDataTransfer<'elem when 'elem : struct>(
    buildFunToBenchmark,
    converter: string -> 'elem,
    boolConverter,
    vertex) =

    inherit BFSBenchmarks<'elem>(
        buildFunToBenchmark,
        converter,
        boolConverter,
        vertex)

    [<GlobalSetup>]
    override this.GlobalSetup() =
        this.ReadMatrix()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearResult()

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearInputMatrix()
        this.ClearResult()

    [<Benchmark>]
    override this.Benchmark() =
        this.LoadMatrixToGPU()
        this.RunBFS()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)
        let _ = this.ResultLevels.ToHost this.Processor
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

type BFSBenchmarks4IntWithoutDataTransfer() =

    inherit BFSBenchmarksWithoutDataTransfer<int>(
        (fun context -> singleSource context intSum intMul),
        int32,
        (fun _ -> 1),
        0)

    static member InputMatrixProvider =
        BFSBenchmarks<_>.InputMatrixProviderBuilder "BFSBenchmarks.txt"

type BFSBenchmarks4IntWithDataTransfer() =

    inherit BFSBenchmarksWithDataTransfer<int>(
        (fun context -> singleSource context intSum intMul),
        int32,
        (fun _ -> 1),
        0)

    static member InputMatrixProvider =
        BFSBenchmarks<_>.InputMatrixProviderBuilder "BFSBenchmarks.txt"
