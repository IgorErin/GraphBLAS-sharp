module GraphBLAS.FSharp.Benchmarks.Configs

open BenchmarkDotNet.Columns
open BenchmarkDotNet.Toolchains.InProcess.Emit
open GraphBLAS.FSharp.IO
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open GraphBLAS.FSharp.Benchmarks.Columns

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

type MinMaxMeanConfig() =
    inherit ManualConfig()

    do
        base.AddColumn(
            StatisticColumn.Min,
            StatisticColumn.Max,
            StatisticColumn.Mean
        )
        |> ignore
