namespace GraphBLAS.FSharp.Benchmarks.Columns

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Running
open GraphBLAS.FSharp.IO

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

            let rowsCount, columnsCount =
                matrixShape.RowCount, matrixShape.ColumnCount

            let _, edges =
                match inputMatrixReader.Format with
                | Coordinate ->
                    if rowsCount = columnsCount then
                        (rowsCount, matrixShape.Nnz)
                    else
                        (columnsCount, rowsCount)
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

type CommonColumn<'a>(benchmarkCaseConvert, columnName: string, getShape: 'a -> 'b) =
    interface IColumn with
        member this.AlwaysShow = true
        member this.Category = ColumnCategory.Params
        member this.ColumnName = columnName

        member this.GetValue(_: Summary, benchmarkCase: BenchmarkCase) =
            benchmarkCaseConvert benchmarkCase
            |> getShape
            |> sprintf "%A"

        member this.GetValue(summary: Summary, benchmarkCase: BenchmarkCase, _: SummaryStyle) =
            (this :> IColumn).GetValue(summary, benchmarkCase)

        member this.Id = sprintf $"%s{columnName}"

        member this.IsAvailable(_: Summary) = true
        member this.IsDefault(_: Summary, _: BenchmarkCase) = false
        member this.IsNumeric = true
        member this.Legend = sprintf $"%s{columnName}"
        member this.PriorityInCategory = 1
        member this.UnitType = UnitType.Size

type MatrixPairColumn(name, getShape) =
    inherit CommonColumn<MtxReader*MtxReader>(
        (fun benchmarkCase -> benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader * MtxReader),
        name,
        getShape)

type MatrixColumn(name, getShape) =
    inherit CommonColumn<MtxReader>(
        (fun benchmarkCase -> benchmarkCase.Parameters.["InputMatrixReader"] :?> MtxReader),
        name,
        getShape)
