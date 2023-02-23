module GraphBLAS.FSharp.Tests.Backend.Vector.Map2

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Tests.TestCases
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Vector
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClVectorExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext

let logger = Log.create "Vector.ElementWise.Tests"

let config = Utils.defaultConfig

let getCorrectnessTestName<'a> (case: OperationCase<'a>) fstType sndType thrType =
    $"Correctness on '{fstType} option -> '{sndType} option -> '{thrType} option, {case.Format}"

let checkResult isEqual resultZero (op: 'a -> 'b -> 'c) (actual: Vector<'c>) (leftArray: 'a []) (rightArray: 'b []) =

    let expectedArrayLength = leftArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op leftArray.[i] rightArray.[i]

    let expected =
        Utils.createVectorFromArray Dense expectedArray (isEqual resultZero)
        |> Utils.vectorToDenseVector

    match actual with
    | Vector.Dense actual ->
        "arrays must have the same values"
        |> Expect.equal actual expected
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    isEqual
    zero
    op
    (addFun: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a> -> ClVector<'a>)
    (toDense: MailboxProcessor<_> -> AllocationFlag -> ClVector<'a> -> ClVector<'a>)
    case
    (leftArray: 'a [], rightArray: 'a [])
    =

    let isZero = (isEqual zero)

    let firstVectorHost =
        Utils.createVectorFromArray case.Format leftArray isZero

    let secondVectorHost =
        Utils.createVectorFromArray case.Format rightArray isZero

    if firstVectorHost.NNZ > 0
       && secondVectorHost.NNZ > 0 then

        let context = case.TestContext.ClContext
        let q = case.TestContext.Queue

        let firstVector = firstVectorHost.ToDevice context
        let secondVector = secondVectorHost.ToDevice context

        try
            let res =
                addFun q HostInterop firstVector secondVector

            firstVector.Dispose q
            secondVector.Dispose q

            let denseActual = toDense q HostInterop res

            let actual = denseActual.ToHost q

            res.Dispose q
            denseActual.Dispose q

            checkResult isEqual zero op actual leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let addTestFixtures case =

    let wgSize = 32

    let context = case.TestContext.ClContext

    [ let intAddFun =
          Vector.map2 context ArithmeticOperations.intSum wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0 (+) intAddFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "int" "int" "int")

      let floatAddFun =
          Vector.map2 context ArithmeticOperations.floatSum wgSize

      let floatToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest Utils.floatIsEqual 0.0 (+) floatAddFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "float" "float" "float")

      let boolAddFun =
          Vector.map2 context ArithmeticOperations.boolSum wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) false (||) boolAddFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "bool" "bool" "bool")

      let byteAddFun =
          Vector.map2 context ArithmeticOperations.byteSum wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0uy (+) byteAddFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "byte" "byte" "byte") ]

let addTests =
    operationGPUTests "Backend.Vector.ElementWiseAdd tests" addTestFixtures

let mulTestFixtures case =
    let wgSize = 32

    let context = case.TestContext.ClContext

    [ let intMulFun =
          Vector.map2 context ArithmeticOperations.intMul wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0 (*) intMulFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "int" "int" "int")

      let floatMulFun =
          Vector.map2 context ArithmeticOperations.floatMul wgSize

      let floatIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      let floatToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest floatIsEqual 0.0 (*) floatMulFun floatToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "float" "float" "float")

      let boolMulFun =
          Vector.map2 context ArithmeticOperations.boolMul wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) false (&&) boolMulFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "bool" "bool" "bool")

      let byteMulFun =
          Vector.map2 context ArithmeticOperations.byteMul wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0uy (*) byteMulFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName case "byte" "byte" "byte") ]

let mulTests =
    operationGPUTests "Backend.Vector.ElementWiseMul tests" addTestFixtures

let addAtLeastOneTestFixtures case =
    let config = Utils.defaultConfig

    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.Format}"

    let wgSize = 32
    let context = case.TestContext.ClContext

    [ let toCoo = Vector.toSparse context wgSize

      let intAddFun =
          Vector.map2AtLeastOne context ArithmeticOperations.intSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) 0 (+) intAddFun toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatToCoo = Vector.toSparse context wgSize

      let floatAddFun =
          Vector.map2AtLeastOne context ArithmeticOperations.floatSumAtLeastOne wgSize

      case
      |> correctnessGenericTest Utils.floatIsEqual 0.0 (+) floatAddFun floatToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toSparse context wgSize

      let boolAddFun =
          Vector.map2AtLeastOne context ArithmeticOperations.boolSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) false (||) boolAddFun boolToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toSparse context wgSize

      let byteAddFun =
          Vector.map2AtLeastOne context ArithmeticOperations.byteSumAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) 0uy (+) byteAddFun byteToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addAtLeastOneTests =
    operationGPUTests "Backend.Vector.ElementWiseAtLeasOneAdd tests" addTestFixtures

let mulAtLeastOneTestFixtures case =
    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, {case.Format}"

    let wgSize = 32

    let context = case.TestContext.ClContext

    [ let toCoo = Vector.toSparse context wgSize

      let intMulFun =
          Vector.map2AtLeastOne context ArithmeticOperations.intMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) 0 (*) intMulFun toCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatToCoo = Vector.toSparse context wgSize

      let floatMulFun =
          Vector.map2AtLeastOne context ArithmeticOperations.floatMulAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      case
      |> correctnessGenericTest fIsEqual 0.0 (*) floatMulFun floatToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolToCoo = Vector.toSparse context wgSize

      let boolMulFun =
          Vector.map2AtLeastOne context ArithmeticOperations.boolMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) false (&&) boolMulFun boolToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteToCoo = Vector.toSparse context wgSize

      let byteMulFun =
          Vector.map2AtLeastOne context ArithmeticOperations.byteMulAtLeastOne wgSize

      case
      |> correctnessGenericTest (=) 0uy (*) byteMulFun byteToCoo
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulAtLeastOneTests =
    operationGPUTests "Backend.Vector.ElementWiseAtLeasOneMul tests" mulTestFixtures

let addGeneralTestFixtures (case: OperationCase<VectorFormat>) =
    let wgSize = 32

    let context = case.TestContext.ClContext

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intAddFun =
          Vector.map2General context ArithmeticOperations.intSum wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0 (+) intAddFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      if Utils.isFloat64Available context.ClDevice then
          let floatAddFun =
              Vector.map2General context ArithmeticOperations.floatSum wgSize

          let floatToDense = Vector.toDense context wgSize

          case
          |> correctnessGenericTest Utils.floatIsEqual 0.0 (+) floatAddFun floatToDense
          |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolAddFun =
          Vector.map2General context ArithmeticOperations.boolSum wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) false (||) boolAddFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteAddFun =
          Vector.map2General context ArithmeticOperations.byteSum wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0uy (+) byteAddFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addGeneralTests =
    operationGPUTests "Backend.Vector.ElementWiseAddGen tests" addGeneralTestFixtures

let fillSubVectorComplementedQ<'a, 'b> value =
    <@ fun (left: 'a option) (right: 'b option) ->
        match left with
        | None -> Some value
        | _ -> right @>

let fillSubVectorFun value zero isEqual =
    fun left right ->
        if isEqual left zero then
            value
        else
            right

let mulGeneralTestFixtures case =
    let wgSize = 32

    let context = case.TestContext.ClContext

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intMulFun =
          Vector.map2General context ArithmeticOperations.intMul wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0 (*) intMulFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      if Utils.isFloat64Available context.ClDevice then
          let floatMulFun =
              Vector.map2General context ArithmeticOperations.floatMul wgSize

          let floatToDense = Vector.toDense context wgSize

          case
          |> correctnessGenericTest Utils.floatIsEqual 0.0 (*) floatMulFun floatToDense
          |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMulFun =
          Vector.map2General context ArithmeticOperations.boolMul wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) false (&&) boolMulFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMulFun =
          Vector.map2General context ArithmeticOperations.byteMul wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0uy (*) byteMulFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulGeneralTests =
    operationGPUTests "Backend.Vector.SparseVector.ElementWiseMulGen tests" mulGeneralTestFixtures

let complementedGeneralTestFixtures case =
    let wgSize = 32

    let context = case.TestContext.ClContext

    let getCorrectnessTestName = getCorrectnessTestName case

    [ let intMaskFun =
          Vector.map2General context (fillSubVectorComplementedQ 1) wgSize

      let intToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0 (fillSubVectorFun 1 0 (=)) intMaskFun intToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatMaskFun =
          Vector.map2General context (fillSubVectorComplementedQ 1.0) wgSize

      if Utils.isFloat64Available context.ClDevice then
          let floatToDense = Vector.toDense context wgSize

          case
          |> correctnessGenericTest
              Utils.floatIsEqual
              0.0
              (fillSubVectorFun 1.0 0.0 Utils.floatIsEqual)
              floatMaskFun
              floatToDense
          |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMaskFun =
          Vector.map2General context (fillSubVectorComplementedQ true) wgSize

      let boolToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) false (fillSubVectorFun true false (=)) boolMaskFun boolToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMaskFun =
          Vector.map2General context (fillSubVectorComplementedQ 1uy) wgSize

      let byteToDense = Vector.toDense context wgSize

      case
      |> correctnessGenericTest (=) 0uy (fillSubVectorFun 1uy 0uy (=)) byteMaskFun byteToDense
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let complementedGeneralTests =
    operationGPUTests "Backend.Vector.ElementWiseGen mask tests" complementedGeneralTestFixtures
