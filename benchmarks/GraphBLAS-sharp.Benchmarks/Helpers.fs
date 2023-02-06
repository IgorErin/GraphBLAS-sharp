namespace GraphBLAS.FSharp.Benchmarks

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open BenchmarkDotNet.Toolchains.InProcess.Emit
open Brahma.FSharp
open Brahma.FSharp.OpenCL.Translator
open Brahma.FSharp.OpenCL.Translator.QuotationTransformers
open GraphBLAS.FSharp.Backend.Objects
open MathNet.Numerics.LinearAlgebra
open OpenCL.Net
open GraphBLAS.FSharp.IO
open System.IO
open System.Text.RegularExpressions
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp.Tests
open FsCheck
open Expecto

type TEPSColumn() =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Statistics
        member this.ColumnName: string = "TEPS"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrixReader =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader
                |> fst

            let matrixShape = inputMatrixReader.ReadMatrixShape()

            let (nrows, ncols) =
                matrixShape.RowCount, matrixShape.ColumnCount

            let (vertices, edges) =
                match inputMatrixReader.Format with
                | Coordinate ->
                    if nrows = ncols then
                        (nrows, matrixShape.Nnz)
                    else
                        (ncols, nrows)
                | _ -> failwith "Unsupported"

            if isNull summary.[benchmarkCase].ResultStatistics then
                "NA"
            else
                let meanTime =
                    summary.[benchmarkCase].ResultStatistics.Mean

                sprintf "%f" <| float edges / (meanTime * 1e-6)

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle) : string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string = "TEPSColumn"
        member this.IsAvailable(summary: Summary) : bool = true
        member this.IsDefault(summary: Summary, benchmarkCase: BenchmarkCase) : bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = "Traversed edges per second"
        member this.PriorityInCategory: int = 0
        member this.UnitType: UnitType = UnitType.Dimensionless

type MatrixColumn(columnName: string, getShape: MtxReader -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader
            |> getShape
            |> sprintf "%i"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle) : string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string =
            sprintf "%s.%s" "MatrixColumn" columnName

        member this.IsAvailable(_: Summary) : bool = true
        member this.IsDefault(_: Summary, _: BenchmarkCase) : bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = sprintf "%s of input matrix" columnName
        member this.PriorityInCategory: int = 1
        member this.UnitType: UnitType = UnitType.Size

type MatrixPairColumn(columnName: string, getShape: (MtxReader * MtxReader) -> int) =
    interface IColumn with
        member this.AlwaysShow: bool = true
        member this.Category: ColumnCategory = ColumnCategory.Params
        member this.ColumnName: string = columnName

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase) : string =
            let inputMatrix =
                benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader

            sprintf "%i" <| getShape inputMatrix

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, style: SummaryStyle) : string =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id: string =
            sprintf "%s.%s" "MatrixShapeColumn" columnName

        member this.IsAvailable(_: Summary) : bool = true
        member this.IsDefault(_: Summary, _: BenchmarkCase) : bool = false
        member this.IsNumeric: bool = true
        member this.Legend: string = sprintf "%s of input matrix" columnName
        member this.PriorityInCategory: int = 1
        member this.UnitType: UnitType = UnitType.Size

type CommonConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixPairColumn("RowCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixPairColumn("ColumnCount", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixPairColumn("NNZ", (fun (mtxReader, _) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            MatrixPairColumn("SqrNNZ", (fun (_, mtxReader) -> mtxReader.ReadMatrixShape().Nnz)) :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

type Config() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixPairColumn("RowCount", (fun (matrix,_) -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixPairColumn("ColumnCount", (fun (matrix,_) -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixPairColumn(
                "NNZ",
                fun (matrix,_) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            MatrixPairColumn(
                "SqrNNZ",
                fun (_,matrix) ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            TEPSColumn() :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

type SingleMatrixConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            MatrixColumn("RowCount", (fun matrix -> matrix.ReadMatrixShape().RowCount)) :> IColumn,
            MatrixColumn("ColumnCount", (fun matrix -> matrix.ReadMatrixShape().ColumnCount)) :> IColumn,
            MatrixColumn(
                "NNZ",
                fun matrix ->
                    match matrix.Format with
                    | Coordinate -> matrix.ReadMatrixShape().Nnz
                    | Array -> 0
            )
            :> IColumn,
            StatisticColumn.Min,
            StatisticColumn.Max
        )
        |> ignore

        base.AddJob(
            Job
                .Dry
                .WithToolchain(InProcessEmitToolchain.Instance)
                .WithWarmupCount(3)
                .WithIterationCount(10)
                .WithInvocationCount(3)
        )
        |> ignore

module Utils =
    type BenchmarkContext =
        { ClContext: Brahma.FSharp.ClContext
          Queue: MailboxProcessor<Msg> }

    let getMatricesFilenames configFilename =
        let getFullPathToConfig filename =
            Path.Combine [| __SOURCE_DIRECTORY__
                            "Configs"
                            filename |]
            |> Path.GetFullPath


        configFilename
        |> getFullPathToConfig
        |> File.ReadLines
        |> Seq.filter (fun line -> not <| line.StartsWith "!")

    let getFullPathToMatrix datasetsFolder matrixFilename =
        Path.Combine [| __SOURCE_DIRECTORY__
                        "Datasets"
                        datasetsFolder
                        matrixFilename |]

    let avaliableContexts =
        let pathToConfig =
            Path.Combine [| __SOURCE_DIRECTORY__
                            "Configs"
                            "Context.txt" |]
            |> Path.GetFullPath

        use reader = new StreamReader(pathToConfig)
        let platformRegex = Regex <| reader.ReadLine()

        let deviceType =
            match reader.ReadLine() with
            | "Cpu" -> DeviceType.Cpu
            | "Gpu" -> DeviceType.Gpu
            | "Default" -> DeviceType.Default
            | _ -> failwith "Unsupported"

        let workGroupSizes =
            reader.ReadLine()
            |> (fun s -> s.Split ' ')
            |> Seq.map int

        let mutable e = ErrorCode.Unknown

        let contexts =
            Cl.GetPlatformIDs &e
            |> Array.collect (fun platform -> Cl.GetDeviceIDs(platform, deviceType, &e))
            |> Seq.ofArray
            |> Seq.distinctBy
                (fun device ->
                    Cl
                        .GetDeviceInfo(device, DeviceInfo.Name, &e)
                        .ToString())
            |> Seq.filter
                (fun device ->
                    let platform =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                            .CastTo<Platform>()

                    let platformName =
                        Cl
                            .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                            .ToString()

                    platformRegex.IsMatch platformName)
            |> Seq.map
                (fun device ->
                    let platform =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Platform, &e)
                            .CastTo<Platform>()

                    let clPlatform =
                        Cl
                            .GetPlatformInfo(platform, PlatformInfo.Name, &e)
                            .ToString()
                        |> Platform.Custom

                    let deviceType =
                        Cl
                            .GetDeviceInfo(device, DeviceInfo.Type, &e)
                            .CastTo<DeviceType>()

                    let clDeviceType =
                        match deviceType with
                        | DeviceType.Cpu -> ClDeviceType.Cpu
                        | DeviceType.Gpu -> ClDeviceType.Gpu
                        | DeviceType.Default -> ClDeviceType.Default
                        | _ -> failwith "Unsupported"

                    let device =
                        ClDevice.GetFirstAppropriateDevice(clPlatform)

                    let translator = FSQuotationToOpenCLTranslator device

                    let context =
                        Brahma.FSharp.ClContext(device, translator)

                    let queue = context.QueueProvider.CreateQueue()

                    { ClContext = context; Queue = queue })

        seq {
            for wgSize in workGroupSizes do
                for context in contexts do
                    yield (context, wgSize)
        }

    let nextSingle (random: System.Random) =
        let buffer = Array.zeroCreate<byte> 4
        random.NextBytes buffer
        System.BitConverter.ToSingle(buffer, 0)

    let normalFloatGenerator =
        (Arb.Default.NormalFloat()
        |> Arb.toGen
        |> Gen.map float)

    let fIsEqual x y = abs (x - y) < Accuracy.medium.absolute || x.Equals y

module Operations =
    let inline add () = <@ fun x y -> Some(x + y) @>

    let addWithFilter = <@ fun x y ->
        let res = x + y
        if abs res < 1e-8f then None else Some res
    @>

    let inline mult () = <@ fun x y -> Some <|x * y @>

    let logicalOr = <@ fun x y ->
        let mutable res = None

        match x, y with
        | false, false -> res <- None
        | _            -> res <- Some true

        res @>

    let logicalAnd = <@ fun x y ->
        let mutable res = None

        match x, y with
        | true, true -> res <- Some true
        | _          -> res <- None

        res @>

module VectorGenerator =
    let private pairOfVectorsOfEqualSize (valuesGenerator: Gen<'a>) createVector =
        gen {
            let! length = Gen.sized <| fun size -> Gen.constant size

            let! leftArray = Gen.arrayOfLength length valuesGenerator

            let! rightArray = Gen.arrayOfLength length valuesGenerator

            return (createVector leftArray, createVector rightArray)
        }

    let intPair format =
        fun array -> Utils.createVectorFromArray format array ((=) 0)
        |> pairOfVectorsOfEqualSize Arb.generate<int32>

    let floatPair format =
        let fIsEqual x y = abs (x - y) < Accuracy.medium.absolute || x = y

        let createVector array = Utils.createVectorFromArray format array (fIsEqual 0.0)

        pairOfVectorsOfEqualSize Utils.normalFloatGenerator createVector


module MatrixGenerator =
    let private pairOfMatricesOfEqualSizeGenerator (valuesGenerator: Gen<'a>) createMatrix =
        gen {
            let! rowsCount, columnsCount = Generators.dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)
            return (createMatrix matrixA, createMatrix matrixB)
        }

    let intPairOfEqualSizes format =
        fun array -> Utils.createMatrixFromArray2D format array ((=) 0)
        |> pairOfMatricesOfEqualSizeGenerator Arb.generate<int32>

    let floatPairOfEqualSizes format =
        fun array -> Utils.createMatrixFromArray2D format array (Utils.fIsEqual 0.0)
        |> pairOfMatricesOfEqualSizeGenerator Utils.normalFloatGenerator

    let private pairOfMatricesWithMaskOfEqualSizeGenerator (valuesGenerator: Gen<'a>) format createMatrix =
        gen {
            let! rowsCount, columnsCount = Generators.dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)
            let! matrixB = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)
            let! mask = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)

            return (createMatrix format matrixA,
                    createMatrix format matrixB,
                    createMatrix COO mask)
        }

    let intPairWithMaskOfEqualSizes format =
        fun format array -> Utils.createMatrixFromArray2D format array ((=) 0)
        |> pairOfMatricesWithMaskOfEqualSizeGenerator Arb.generate<int32> format

    let floatPairWithMaskOfEqualSizes format =
        fun format array -> Utils.createMatrixFromArray2D format array (Utils.fIsEqual 0.0)
        |> pairOfMatricesWithMaskOfEqualSizeGenerator Utils.normalFloatGenerator format

module MatrixVectorGenerator =
    let private pairOfMatricesAndVectorGenerator (valuesGenerator: Gen<'a>) createVector createMatrix =
        gen {
            let! rowsCount, columnsCount = Generators.dimension2DGenerator
            let! matrixA = valuesGenerator |> Gen.array2DOfDim (rowsCount, columnsCount)
            let! vector = valuesGenerator |> Gen.arrayOfLength columnsCount

            return (createMatrix matrixA, createVector vector)
        }

    let intPairOfCompatibleSizes matrixFormat vectorFormat =
        let createVector array = Utils.createVectorFromArray vectorFormat array ((=) 0)
        let createMatrix array = Utils.createMatrixFromArray2D matrixFormat array ((=) 0)

        pairOfMatricesAndVectorGenerator Arb.generate<int32> createVector createMatrix

    let floatPairOfCompatibleSizes matrixFormat vectorFormat =
        let createVector array = Utils.createVectorFromArray vectorFormat array (Utils.floatIsEqual 0.0)
        let createMatrix array = Utils.createMatrixFromArray2D matrixFormat array (Utils.floatIsEqual 0.0)

        pairOfMatricesAndVectorGenerator Utils.normalFloatGenerator createVector createMatrix


