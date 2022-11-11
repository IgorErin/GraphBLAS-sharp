module Backend.Vector.Copy

open Expecto
open Expecto.Logging

open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils

let logger = Log.create "Vector.copy.Tests"

let clContext = defaultContext.ClContext

let checkResult (isEqual: 'a -> 'a -> bool) (actual: Vector<'a>) (expected: Vector<'a>) =

    Expect.equal actual.Size expected.Size "The size should be the same"

    match actual, expected with
    | VectorDense actual, VectorDense expected ->
        let isEqual left right =
            match left, right with
            | Some left, Some right -> isEqual left right
            | None, None -> true
            | _, _ -> false

        compareArrays isEqual actual expected "The values array must contain the default value"
    | VectorSparse actual, VectorSparse expected ->
        compareArrays isEqual actual.Values expected.Values "The values array must contain the default value"
        compareArrays (=) actual.Indices expected.Indices "The index array must contain the 0"
    | _, _ -> failwith "Copy format must be the same"

let correctnessGenericTest<'a when 'a: struct>
    filter
    isEqual
    (isZero: 'a -> bool)
    (copy: MailboxProcessor<Brahma.FSharp.Msg> -> ClVector<'a> -> ClVector<'a>)
    (case: OperationCase<VectorFormat>)
    (array: 'a [])
    =
    if array.Length > 0 then
        let array = filter array

        let q = case.ClContext.Queue
        let context = case.ClContext.ClContext

        let expected =
            createVectorFromArray case.Format array isZero

        let clVector = expected.ToDevice context
        let clVectorCopy = copy q clVector
        let actual = clVectorCopy.ToHost q

        clVector.Dispose q
        clVectorCopy.Dispose q

        checkResult isEqual actual expected

let testFixtures (case: OperationCase<VectorFormat>) =
    let filterFloats =
        Array.filter (System.Double.IsNaN >> not)

    let config = defaultConfig

    let getCorrectnessTestName datatype =
        sprintf "Correctness on %s, %A" datatype case.Format

    let wgSize = 32
    let context = case.ClContext.ClContext

    [ let intCopy = Vector.copy context wgSize
      let isZero item = item = 0

      case
      |> correctnessGenericTest<int> id (=) isZero intCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let floatCopy = Vector.copy context wgSize
      let isZero item = item = 0.0

      case
      |> correctnessGenericTest<float> filterFloats (=) isZero floatCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "float")

      let boolCopy = Vector.copy context wgSize
      let isZero item = item = true

      case
      |> correctnessGenericTest<bool> id (=) isZero boolCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "bool")

      let floatCopy = Vector.copy context wgSize
      let isZero item = item = 0uy

      case
      |> correctnessGenericTest<byte> id (=) isZero floatCopy
      |> testPropertyWithConfig config (getCorrectnessTestName "byte") ]

let tests =
    testsWithFixtures<VectorFormat> testFixtures "Backend.Vector.copy tests"
