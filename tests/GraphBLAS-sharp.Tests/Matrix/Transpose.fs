module GraphBLAS.FSharp.Tests.Backend.Matrix.Transpose

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Tests.TestCases

let logger = Log.create "Transpose.Tests"

let config = defaultConfig
let wgSize = 32

let checkResult areEqual zero actual (expected2D: 'a [,]) =
    match actual with
    | MatrixCOO actual ->
        let expected =
            COOMatrix.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row arrays should be equal"
        |> compareArrays (=) actual.Rows expected.Rows

        "Column arrays should be equal"
        |> compareArrays (=) actual.Columns expected.Columns

        "Value arrays should be equal"
        |> compareArrays areEqual actual.Values expected.Values
    | MatrixCSR actual ->
        let expected =
            CSRMatrix.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row pointer arrays should be equal"
        |> compareArrays (=) actual.RowPointers expected.RowPointers

        "Column arrays should be equal"
        |> compareArrays (=) actual.ColumnIndices expected.ColumnIndices

        "Value arrays should be equal"
        |> compareArrays areEqual actual.Values expected.Values
    | MatrixCSC actual ->
        let expected =
            CSCMatrix.FromArray2D(expected2D, areEqual zero)

        "The number of rows should be the same"
        |> Expect.equal actual.RowCount expected.RowCount

        "The number of columns should be the same"
        |> Expect.equal actual.ColumnCount expected.ColumnCount

        "Row arrays should be equal"
        |> compareArrays (=) actual.RowIndices expected.RowIndices

        "Column pointer arrays should be equal"
        |> compareArrays (=) actual.ColumnPointers expected.ColumnPointers

        "Value arrays should be equal"
        |> compareArrays areEqual actual.Values expected.Values

let makeTestRegular context q transposeFun areEqual zero case (array: 'a [,]) =
    let mtx =
        createMatrixFromArray2D case.Format array (areEqual zero)

    if mtx.NNZCount > 0 then
        let actual =
            let m = mtx.ToBackend context
            let mT = transposeFun q m
            let res = Matrix.FromBackend q mT
            m.Dispose q
            mT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        let expected2D =
            Array2D.create (Array2D.length2 array) (Array2D.length1 array) zero

        for i in 0 .. Array2D.length1 expected2D - 1 do
            for j in 0 .. Array2D.length2 expected2D - 1 do
                expected2D.[i, j] <- array.[j, i]

        checkResult areEqual zero actual expected2D

let makeTestTwiceTranspose context q transposeFun areEqual zero case (array: 'a [,]) =
    let mtx =
        createMatrixFromArray2D case.Format array (areEqual zero)

    if mtx.NNZCount > 0 then
        let actual =
            let m = mtx.ToBackend context
            let mT = transposeFun q m
            let mTT = transposeFun q mT
            let res = Matrix.FromBackend q mTT
            m.Dispose q
            mT.Dispose q
            mTT.Dispose q
            res

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" (sprintf "%A" actual)
        )

        checkResult areEqual zero actual array

let testFixtures case =
    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A, %A" datatype case.Format case.ClContext

    let areEqualFloat x y =
        System.Double.IsNaN x && System.Double.IsNaN y
        || x = y

    let context = case.ClContext.ClContext
    let q = case.ClContext.Queue
    q.Error.Add(fun e -> failwithf "%A" e)

    [ let transposeFun = Matrix.transpose context wgSize

      case
      |> makeTestRegular context q transposeFun (=) 0
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      case
      |> makeTestTwiceTranspose context q transposeFun (=) 0
      |> testPropertyWithConfig config (getCorrectnessTestName "int (twice transpose)")


      let transposeFun = Matrix.transpose context wgSize

      case
      |> makeTestRegular context q transposeFun areEqualFloat 0.0
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      case
      |> makeTestTwiceTranspose context q transposeFun areEqualFloat 0.0
      |> testPropertyWithConfig config (getCorrectnessTestName "float (twice transpose)")

      let transposeFun = Matrix.transpose context wgSize

      case
      |> makeTestRegular context q transposeFun (=) 0uy
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      case
      |> makeTestTwiceTranspose context q transposeFun (=) 0uy
      |> testPropertyWithConfig config (getCorrectnessTestName "byte (twice transpose)")

      let transposeFun = Matrix.transpose context wgSize

      case
      |> makeTestRegular context q transposeFun (=) false
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      case
      |> makeTestTwiceTranspose context q transposeFun (=) false
      |> testPropertyWithConfig config (getCorrectnessTestName "bool (twice transpose)") ]

let tests =
    operationGPUTests "Transpose tests" testFixtures
