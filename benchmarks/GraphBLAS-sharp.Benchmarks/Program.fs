open System.Xml.Linq
open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher
            [| typeof<RealData.BFSIntWithoutTransferBenchmark>
               typeof<Synthetic.VectorSparseMap2Int32WithoutTransferBenchmark>
               typeof<Synthetic.MatrixCOOMap2IntWithoutTransferBenchmark>
               typeof<Synthetic.MatrixCSRMap2IntWithoutTransferBenchmark>
               typeof<Synthetic.MxmFloatMultiplicationOnlyBenchmark>
               typeof<Synthetic.MxvInt32Benchmark> |]

    benchmarks.RunAll |> ignore
    0
