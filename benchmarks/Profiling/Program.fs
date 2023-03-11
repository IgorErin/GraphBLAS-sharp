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
    let matrixName = "coPapersCiteseer.mtx"

    let matrixReader = MtxReader(Path.Combine [| __SOURCE_DIRECTORY__ ; "Dataset"; matrixName |])

    printfn "readMatrix"

    let matrix = matrixReader.ReadMatrix(fun _ -> 1)
    let clMatrix = matrix.ToDevice clContext

    let (ClMatrix.CSR csrMatrix) = Matrix.toCSR clContext 32 processor DeviceOnly clMatrix

    printfn "convert matrix"

    let bfs = BFS.singleSource clContext ArithmeticOperations.intSum ArithmeticOperations.intMul 32

    bfs processor csrMatrix 0 |> ignore

    0
