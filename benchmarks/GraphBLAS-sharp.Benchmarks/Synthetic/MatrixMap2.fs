namespace GraphBLAS.FSharp.Benchmarks.RealData

open System.IO
open FsCheck
open GraphBLAS.FSharp.Backend.Quotes
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Backend.Objects.ClContext

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type MatrixMap2Benchmarks<'elem when 'elem : struct>
    (buildFunToBenchmark,
    generator: Gen<Matrix<_>*Matrix<_>>) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<Params(100, 1000, 10000)>] // TODO(choose compatible size)
    member val Size = Unchecked.defaultof<int> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf "%A" e)
        p

    static member AvaliableContexts = Utils.avaliableContexts

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.GenerateMatrices() =
         List.last (Gen.sample this.Size 1 generator)


    member this.CreateAndSetMatrices() =
        let matrixPair = this.GenerateMatrices()

        // TODO(empty Matrix)
        if (fst matrixPair).NNZ > 0
           && (snd matrixPair).NNZ > 0 then
            firstMatrixHost <- fst matrixPair
            secondMatrixHost <- snd matrixPair
        else
            this.CreateAndSetMatrices()

    member this.LoadMatricesToGPU() =
        firstMatrix <- firstMatrixHost.ToDevice this.OclContext
        secondMatrix <- secondMatrixHost.ToDevice this.OclContext

    member this.Map2() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor HostInterop firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    abstract member GlobalSetup: unit -> unit

    abstract member IterationSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit

type MatrixMap2SyntheticWithoutTransfer<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit MatrixMap2Benchmarks<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreateAndSetMatrices()
        this.LoadMatricesToGPU()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<Benchmark>]
    override this.Benchmark() =
        this.Map2()
        this.Processor.PostAndReply(Msg.MsgNotifyMe)

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ClearInputMatrices()

    [<GlobalCleanup>]
    override this.GlobalCleanup() = ()

type MatrixCOOIntMap2Synthetic() =

    inherit MatrixMap2SyntheticWithoutTransfer<int>(
        (fun clContext -> Matrix.map2 clContext ArithmeticOperations.intSum),
        MatrixGenerator.intPairOfEqualSizes COO)

type MatrixCSRIntMap2Synthetic() =

    inherit MatrixMap2SyntheticWithoutTransfer<int>(
        (fun clContext -> Matrix.map2 clContext ArithmeticOperations.intSum),
        MatrixGenerator.intPairOfEqualSizes CSR)

type MatrixCOOFloatMap2Synthetic() =

    inherit MatrixMap2SyntheticWithoutTransfer<float>(
        (fun clContext -> Matrix.map2 clContext ArithmeticOperations.floatSum),
        MatrixGenerator.floatPairOfEqualSizes COO)

type MatrixCSRFloatMap2Synthetic() =

    inherit MatrixMap2SyntheticWithoutTransfer<float>(
        (fun clContext -> Matrix.map2 clContext ArithmeticOperations.floatSum),
        MatrixGenerator.floatPairOfEqualSizes CSR)
