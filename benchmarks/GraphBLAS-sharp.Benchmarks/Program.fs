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
                             typeof<RealData.MxmFloat32MultiplicationOnly>
                             typeof<RealData.MxmFloat32WithTransposing>
                             typeof<RealData.MxmBoolMultiplicationOnly>
                             typeof<RealData.MxmBoolWithTransposing> |]

    benchmarks.Run argv |> ignore
    0
