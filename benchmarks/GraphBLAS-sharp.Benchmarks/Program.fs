open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<RealData.Map2Float32COOWithoutTransfer>
                             typeof<RealData.Map2Float32COOWithTransfer>
                             typeof<RealData.Map2Float32CSRWithoutTransfer>
                             typeof<RealData.Map2BoolCOOWithoutTransfer>
                             typeof<RealData.Map2BoolCSRWithoutTransfer>
                             typeof<RealData.Mxm4Float32MultiplicationOnly>
                             typeof<RealData.Mxm4Float32WithTransposing>
                             typeof<RealData.Mxm4BoolMultiplicationOnly>
                             typeof<RealData.Mxm4BoolWithTransposing> |]

    benchmarks.Run argv |> ignore
    0
