open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
        let benchmarks =
            BenchmarkSwitcher
                [| typeof<Synthetic.MatrixCOOMap2IntWithoutTransferBenchmark>
                   typeof<Synthetic.MatrixCSRMap2IntWithoutTransferBenchmark>
                   typeof<Synthetic.VectorSparseMap2Int32WithoutTransferBenchmark>
                   typeof<Synthetic.MxmFloatMultiplicationOnlyBenchmark>
                   typeof<Synthetic.MxmFloatWithTransposingBenchmark>
                   typeof<Synthetic.MxvInt32Benchmark> |]

        benchmarks.Run argv |> ignore
        0
