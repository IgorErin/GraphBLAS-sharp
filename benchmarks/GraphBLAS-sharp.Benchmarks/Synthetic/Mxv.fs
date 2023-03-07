namespace GraphBLAS.FSharp.Benchmarks.Synthetic

open FsCheck
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.MinMaxMeanConfig>)>]
type MxvBenchmark<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator: Gen<Matrix<_>*Vector<_>>) =

    let mutable funToBenchmark = None

    let mutable matrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable vector = Unchecked.defaultof<ClVector<'elem>>
    let mutable matrixHost = Unchecked.defaultof<_>
    let mutable vectorHost = Unchecked.defaultof<_>

    member val Result = Unchecked.defaultof<ClVector<'elem>> with get, set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<Params(1000)>]
    member val Size = Unchecked.defaultof<int> with get, set

    member this.OclContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    member this.CreateMatricesAndMask() =
        List.last (Gen.sample this.Size 1 generator)

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.CreatAnsSetMatrixAndVector() =
        let matrix, vector = this.CreateMatricesAndMask()

        // TODO  Empty matrix
        if matrix.NNZ > 0 && vector.NNZ > 0 then

            matrixHost <- matrix
            vectorHost <- vector
        else
            this.CreatAnsSetMatrixAndVector()

    member this.Mxv() =
        try

        this.Result <- this.FunToBenchmark this.Processor HostInterop matrix vector

        with
            | ex when ex.Message = "InvalidBufferSize" -> ()
            | ex -> raise ex

    member this.ClearInputMatrixAndVector() =
        matrix.Dispose this.Processor
        vector.Dispose this.Processor

    member this.ClearResult() =
        this.Result.Dispose this.Processor

    member this.LoadMatrixAndVectorToGPU() =
        matrix <-matrixHost.ToDevice this.OclContext
        vector <- vectorHost.ToDevice this.OclContext

    abstract member GlobalSetup: unit -> unit

    abstract member IterationSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit

type MxvMultiplicationOnly<'elem when 'elem : struct>(
        buildFunToBenchmark,
        buildMatrix) =

    inherit MxvBenchmark<'elem>(
        buildFunToBenchmark,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreatAnsSetMatrixAndVector()
        this.LoadMatrixAndVectorToGPU()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<Benchmark>]
    override this.Benchmark() =
        this.Mxv()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ClearInputMatrixAndVector()

    [<GlobalCleanup>]
    override this.GlobalCleanup() = ()

type MxvInt32Benchmark() =

    inherit MxvMultiplicationOnly<int32>(
        (fun wgSize -> SpMV.run wgSize ArithmeticOperations.intSum ArithmeticOperations.intMul),
        MatrixVectorGenerator.intPairOfCompatibleSizes CSR Dense
        )

type MxvFloatBenchmark() =

    inherit MxvMultiplicationOnly<float>(
        (fun wgSize -> SpMV.run wgSize ArithmeticOperations.floatSum ArithmeticOperations.floatMul),
        MatrixVectorGenerator.floatPairOfCompatibleSizes CSR Dense
        )
