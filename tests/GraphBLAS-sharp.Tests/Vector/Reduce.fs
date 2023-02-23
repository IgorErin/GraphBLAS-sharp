module GraphBLAS.FSharp.Tests.Backend.Vector.Reduce

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests
open Brahma.FSharp
open FSharp.Quotations
open TestCases
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector

let logger = Log.create "Vector.reduce.Tests"

let wgSize = 32

let config = Utils.defaultConfig

let checkResult zero op (actual: 'a) (vector: 'a []) =
    let expected = Array.fold op zero vector

    "Results should be the same"
    |> Expect.equal actual expected

let correctnessGenericTest
    isEqual
    zero
    op
    opQ
    (reduce: Expr<'a -> 'a -> 'a> -> MailboxProcessor<_> -> ClVector<'a> -> ClCell<'a>)
    case
    (array: 'a [])
    =

    let vector =
        Utils.createVectorFromArray case.Format array (isEqual zero)

    if vector.NNZ > 0 then
        let q = case.TestContext.Queue
        let context = case.TestContext.ClContext

        let clVector = vector.ToDevice context

        let resultCell = reduce opQ q clVector

        let result = Array.zeroCreate 1

        let result =
            let res =
                q.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(resultCell, result, ch))

            q.Post(Msg.CreateFreeMsg<_>(resultCell))

            res.[0]

        checkResult zero op result array

let testFixtures (case: OperationCase<VectorFormat>) =

    let getCorrectnessTestName dataType =
        $"Correctness on %A{dataType}, %A{case.Format}"

    let context = case.TestContext.ClContext
    let q = case.TestContext.Queue

    q.Error.Add(fun e -> failwithf "%A" e)

    [ let intReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0 (+) <@ (+) @> intReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "int")

      let byteReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) 0uy (+) <@ (+) @> byteReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "byte")

      let intMaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Int32.MinValue max <@ max @> intMaxReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "int max")

      if Utils.isFloat64Available context.ClDevice then
          let floatMaxReduce = Vector.reduce context wgSize

          case
          |> correctnessGenericTest Utils.floatIsEqual System.Double.MinValue max <@ max @> floatMaxReduce
          |> testPropertyWithConfig config (getCorrectnessTestName "float max")

      let float32MaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest Utils.float32IsEqual System.Single.MinValue max <@ max @> float32MaxReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "float32 max")

      let byteMaxReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Byte.MinValue max <@ max @> byteMaxReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "byte max")

      let intMinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Int32.MaxValue min <@ min @> intMinReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "int min")

      if Utils.isFloat64Available context.ClDevice then
          let floatMinReduce = Vector.reduce context wgSize

          case
          |> correctnessGenericTest Utils.floatIsEqual System.Double.MaxValue min <@ min @> floatMinReduce
          |> testPropertyWithConfig config (getCorrectnessTestName "float min")

      let float32MinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest Utils.float32IsEqual System.Single.MaxValue min <@ min @> float32MinReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "float32 min")

      let byteMinReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) System.Byte.MaxValue min <@ min @> byteMinReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "byte min")

      let boolOrReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) false (||) <@ (||) @> boolOrReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "bool or")

      let boolAndReduce = Vector.reduce context wgSize

      case
      |> correctnessGenericTest (=) true (&&) <@ (&&) @> boolAndReduce
      |> testPropertyWithConfig config (getCorrectnessTestName "bool and") ]

let tests =
    operationGPUTests "Backend.Vector.reduce tests" testFixtures
