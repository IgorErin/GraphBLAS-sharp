namespace GraphBLAS.FSharp.Benchmarks.Synthetic

open FsCheck
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
type MxmSynthetic<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator: Gen<Matrix<_>*Matrix<_>*Matrix<_>>) =

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

    [<Params(100, 1000, 10000)>]
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

    member this.CreatAnsSetMatrices() =
        let firstMatrix, secondMatrix, mask = this.CreateMatricesAndMask()

        // TODO  Empty matrix
        if firstMatrix.NNZ > 0
            && secondMatrix.NNZ > 0
            && mask.NNZ > 0 then

            maskHost <- firstMatrix
            firstMatrixHost <- secondMatrix
            secondMatrixHost <- mask
        else
            this.CreatAnsSetMatrices()

    member this.Mxm() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix mask

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor
        mask.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    member this.LoadMatricesToGPU() =
        firstMatrix <-firstMatrixHost.ToDevice this.OclContext
        secondMatrix <- secondMatrixHost.ToDevice this.OclContext
        mask <- maskHost.ToDevice this.OclContext

    member this.ConvertSecondMatrixToCSC() =
        secondMatrix <- this.FunCSR2CSC this.Processor HostInterop secondMatrix

    member this.ConvertSecondMatrixToCSR() =
        secondMatrix <- this.FunCSC2CSR this.Processor HostInterop secondMatrix

    abstract member GlobalSetup: unit -> unit

    abstract member IterationSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit

type MxmSyntheticMultiplicationOnly<'elem when 'elem : struct>(
        buildFunToBenchmark,
        buildMatrix) =

    inherit MxmSynthetic<'elem>(
        buildFunToBenchmark,
        buildMatrix)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreatAnsSetMatrices()
        this.LoadMatricesToGPU()
        this.ConvertSecondMatrixToCSC()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<Benchmark>]
    override this.Benchmark() =
        this.Mxm()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrices()

type MxmSyntheticWithTransposing<'elem when 'elem : struct>(
        buildFunToBenchmark,
        generator) =

    inherit MxmSynthetic<'elem>(
        buildFunToBenchmark,
        generator)

    [<GlobalSetup>]
    override this.GlobalSetup() = ()

    [<IterationSetup>]
    override this.IterationSetup() =
        this.CreatAnsSetMatrices()
        this.LoadMatricesToGPU()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<Benchmark>]
    override this.Benchmark() =
        this.ConvertSecondMatrixToCSC()
        this.Mxm()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<IterationCleanup>]
    override this.IterationCleanup() =
        this.ClearResult()
        this.ConvertSecondMatrixToCSR()
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<GlobalCleanup>]
    override this.GlobalCleanup() =
        this.ClearInputMatrices()

type Mxm4Float32MultiplicationOnly() =

    inherit MxmSyntheticMultiplicationOnly<float>(
        (Matrix.mxm (Operations.add ()) (Operations.mult ())),
        MatrixGenerator.floatPairWithMaskOfEqualSizes CSR
        )

type Mxm4FloatWithTransposing() =

    inherit MxmSyntheticWithTransposing<float>(
        Matrix.mxm (Operations.add ()) (Operations.mult ()),
        MatrixGenerator.floatPairWithMaskOfEqualSizes CSR
        )
