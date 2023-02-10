open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher
            [| typeof<RealData.BFSIntWithoutTransferBenchmark>
               typeof<Synthetic.MxmFloatMultiplicationOnlyBenchmark>
               typeof<Synthetic.MxvInt32Benchmark> |]

    benchmarks.Run argv |> ignore
    0
