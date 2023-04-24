open System
open System.IO
open GraphBLAS.FSharp.Backend.Matrix
open GraphBLAS.FSharp.Backend.Quotes

open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Algorithms
open GraphBLAS.FSharp.IO
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects

[<EntryPoint>]
let main _ =
    let context = Context.defaultContext
    let clContext = context.ClContext
    let processor = context.Queue
    let matrixName = "bcspwr01.mtx"

    let matrixReader = MtxReader(Path.Combine [| __SOURCE_DIRECTORY__ ; ".."; "GraphBLAS-sharp.Benchmarks"; "Datasets"; matrixName |])

    printfn "readMatrix"

    let matrix = matrixReader.ReadMatrix(fun _ -> Random().NextSingle()).ToCSR
    let clMatrix = ClMatrix.CSR <| matrix.ToDevice clContext

    printfn "before compile"

    let mul = Matrix.SpGeMM.expand (fst ArithmeticOperations.float32Add) (fst ArithmeticOperations.float32Mul) clContext 32

    printfn "before run"

    let result = mul processor DeviceOnly clMatrix clMatrix

    result.Dispose processor
    0
