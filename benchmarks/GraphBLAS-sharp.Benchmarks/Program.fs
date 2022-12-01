open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<EWiseAddBenchmarks4Float32COOWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4Float32COOWithDataTransfer>
                             typeof<EWiseAddBenchmarks4Float32CSRWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4BoolCOOWithoutDataTransfer>
                             typeof<EWiseAddBenchmarks4BoolCSRWithoutDataTransfer>
                             typeof<MxmBenchmarks4Float32MultiplicationOnly>
                             typeof<MxmBenchmarks4Float32WithTransposing>
                             typeof<MxmBenchmarks4BoolMultiplicationOnly>
                             typeof<MxmBenchmarks4BoolWithTransposing>
                             //typeof<BFSBenchmarks>
                             //typeof<MxvBenchmarks>
                             //typeof<TransposeBenchmarks>
                             typeof<BFSBenchmarks4IntWithDataTransfer>
                             typeof<BFSBenchmarks4IntWithoutDataTransfer> |]

    benchmarks.Run argv |> ignore
    0
