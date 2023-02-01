namespace GraphBLAS.FSharp.Benchmarks.RealData

open System.IO
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Benchmarks

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Config>)>]
type Map2Benchmarks<'elem when 'elem : struct>(buildFunToBenchmark, generator) =

    let mutable funToBenchmark = None
    let mutable firstMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable secondMatrix = Unchecked.defaultof<ClMatrix<'elem>>
    let mutable firstMatrixHost = Unchecked.defaultof<_>
    let mutable secondMatrixHost = Unchecked.defaultof<_>

    member val ResultMatrix = Unchecked.defaultof<ClMatrix<'elem>> with get,set

    [<ParamsSource("AvaliableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    [<Params(100, 1000, 10000)>] // TODO(choose compatible size)
    member val Size = Unchecked.defaultof<MtxReader*MtxReader> with get, set

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

    member this.GenerateMatrices() = Unchecked.defaultof<Matrix<'elem>*Matrix<'elem>>

    member this.CreateAndSetMatrices() =
        let matrixPair = this.GenerateMatrices()

        // TODO(empty Matrix)
        if (fst matrixPair).NNZCount > 0
           && (snd matrixPair).NNZCount > 0 then
            firstMatrixHost <- fst matrixPair
            secondMatrixHost <- snd matrixPair
        else
            this.CreateAndSetMatrices()

    member this.LoadMatricesToGPU () =
        firstMatrix <- firstMatrixHost.ToDevice this.OclContext
        secondMatrix <- secondMatrixHost.ToDevice this.OclContext

    member this.EWiseAddition() =
        this.ResultMatrix <- this.FunToBenchmark this.Processor firstMatrix secondMatrix

    member this.ClearInputMatrices() =
        firstMatrix.Dispose this.Processor
        secondMatrix.Dispose this.Processor

    member this.ClearResult() =
        this.ResultMatrix.Dispose this.Processor

    abstract member GlobalSetup : unit -> unit

    abstract member IterationCleanup : unit -> unit

    abstract member GlobalCleanup : unit -> unit

    abstract member Benchmark : unit -> unit
