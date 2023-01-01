namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Predefined
open Microsoft.FSharp.Quotations

module COOMatrix =
    let binSearch<'a> =
        <@ fun lenght sourceIndex (rowIndices: ClArray<int>) (columnIndices: ClArray<int>) (values: ClArray<'a>) ->

            let mutable leftEdge = 0
            let mutable rightEdge = lenght

            let mutable result = None

            while leftEdge <= rightEdge do
                let middleIdx = (leftEdge + rightEdge) / 2

                let currentIndex: uint64 =
                    ((uint64 rowIndices.[middleIdx]) <<< 32)
                    ||| (uint64 columnIndices.[middleIdx])

                if sourceIndex = currentIndex then
                    result <- Some values[middleIdx]

                    rightEdge <- leftEdge - 1
                elif sourceIndex < currentIndex then
                    rightEdge <- middleIdx - 1
                else
                    leftEdge <- middleIdx + 1

            result @>

    let preparePositions<'a, 'b, 'c> (clContext: ClContext) workGroupSize opAdd =

        let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
            <@ fun (ndRange: Range1D) rowCount columnCount length (leftValues: ClArray<'a>) (leftRows: ClArray<int>) (leftColumns: ClArray<int>) (rightValues: ClArray<'b>) (rightRows: ClArray<int>) (rightColumn: ClArray<int>) (resultBitmap: ClArray<int>) (resultValues: ClArray<'c>) (resultRows: ClArray<int>) (resultColumns: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < rowCount * columnCount then

                        let rowInd = gid / rowCount
                        let columnInd = gid % rowCount

                        let index = (uint64 rowInd <<< 32) ||| (uint64 columnInd)

                        let leftValue =
                            (%binSearch) length index leftRows leftColumns leftValues

                        let rightValue =
                            (%binSearch) length index rightRows rightColumn rightValues

                        match (%op) leftValue rightValue with
                        | Some value ->
                            resultValues.[gid] <- value
                            resultRows.[gid] <- rowInd
                            resultColumns.[gid] <- columnInd

                            resultBitmap.[gid] <- 1
                        | None ->
                            resultBitmap.[gid] <- 0 @>

        let kernel = clContext.Compile <| preparePositions opAdd

        fun (processor: MailboxProcessor<_>) rowCount columnCount (leftValues: ClArray<'a>) (leftRows: ClArray<int>) (leftColumns: ClArray<int>) (rightValues: ClArray<'b>) (rightRows: ClArray<int>) (rightColumns: ClArray<int>) ->

            let (resultLength: int) = columnCount * rowCount

            let resultBitmap =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultRows =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultColumns =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultValues =
                clContext.CreateClArray<'c>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(resultLength, workGroupSize)

            let kernel = kernel.GetKernel()

            processor.Post(
                Msg.MsgSetArguments(
                fun () ->
                    kernel.KernelFunc
                        ndRange
                        rowCount
                        columnCount
                        leftValues.Length
                        leftValues
                        leftRows
                        leftColumns
                        rightValues
                        rightRows
                        rightColumns
                        resultBitmap
                        resultValues
                        resultRows
                        resultColumns))

            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            resultBitmap, resultValues, resultRows, resultColumns

    ///<param name="clContext">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let private setPositions<'a when 'a: struct> (clContext: ClContext) workGroupSize =

        let indicesScatter =
            Scatter.runInplace clContext workGroupSize

        let valuesScatter =
            Scatter.runInplace clContext workGroupSize

        let sum =
            PrefixSum.standardExcludeInplace clContext workGroupSize

        let resultLength = Array.zeroCreate 1

        fun (processor: MailboxProcessor<_>) (allRows: ClArray<int>) (allColumns: ClArray<int>) (allValues: ClArray<'a>) (positions: ClArray<int>) ->
            let resultLengthGpu = clContext.CreateClCell 0

            let _, r = sum processor positions resultLengthGpu

            let resultLength =
                let res =
                    processor.PostAndReply(fun ch -> Msg.CreateToHostMsg<_>(r, resultLength, ch))

                processor.Post(Msg.CreateFreeMsg<_>(r))

                res.[0]

            let resultRows =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultColumns =
                clContext.CreateClArray<int>(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let resultValues =
                clContext.CreateClArray(
                    resultLength,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            indicesScatter processor positions allRows resultRows

            indicesScatter processor positions allColumns resultColumns

            valuesScatter processor positions allValues resultValues

            resultRows, resultColumns, resultValues, resultLength

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let elementwise<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<'a option -> 'b option -> 'c option>)
        workGroupSize
        =

        let ewise = preparePositions clContext workGroupSize opAdd

        let setPositions = setPositions<'c> clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrixLeft: COOMatrix<'a>) (matrixRight: COOMatrix<'b>) ->

            let bitmap, values, rows, columns =
                ewise queue matrixLeft.RowCount matrixLeft.ColumnCount matrixLeft.Values matrixLeft.Rows matrixLeft.Columns matrixRight.Values matrixRight.Rows matrixRight.Columns

            let resultRows, resultColumns, resultValues, resultLength =
                setPositions queue rows columns values bitmap

            queue.Post(Msg.CreateFreeMsg<_>(bitmap))
            queue.Post(Msg.CreateFreeMsg<_>(values))
            queue.Post(Msg.CreateFreeMsg<_>(rows))
            queue.Post(Msg.CreateFreeMsg<_>(columns))

            { Context = clContext
              RowCount = matrixLeft.RowCount
              ColumnCount = matrixLeft.ColumnCount
              Rows = resultRows
              Columns = resultColumns
              Values = resultValues }

    let getTuples (clContext: ClContext) workGroupSize =

        let copy =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        let copyData =
            GraphBLAS.FSharp.Backend.ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->

            let resultRows = copy processor matrix.Rows

            let resultColumns = copy processor matrix.Columns

            let resultValues = copyData processor matrix.Values

            { Context = clContext
              RowIndices = resultRows
              ColumnIndices = resultColumns
              Values = resultValues }

    let private compressRows (clContext: ClContext) workGroupSize =

        let compressRows =
            <@ fun (ndRange: Range1D) (rows: ClArray<int>) (nnz: int) (rowPointers: ClArray<int>) ->

                let i = ndRange.GlobalID0

                if i < nnz then
                    let row = rows.[i]

                    if i = 0 || row <> rows.[i - 1] then
                        rowPointers.[row] <- i @>

        let program = clContext.Compile(compressRows)

        let create = ClArray.create clContext workGroupSize

        let scan =
            ClArray.prefixSumBackwardsIncludeInplace <@ min @> clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (rowIndices: ClArray<int>) rowCount ->

            let nnz = rowIndices.Length
            let rowPointers = create processor (rowCount + 1) nnz

            let kernel = program.GetKernel()

            let ndRange = Range1D.CreateValid(nnz, workGroupSize)
            processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange rowIndices nnz rowPointers))
            processor.Post(Msg.CreateRunMsg<_, _> kernel)

            let total = clContext.CreateClCell()
            let _ = scan processor rowPointers total nnz
            processor.Post(Msg.CreateFreeMsg(total))

            rowPointers

    let toCSR (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            let rowPointers =
                prepare processor matrix.Rows matrix.RowCount

            let cols = copy processor matrix.Columns
            let vals = copyData processor matrix.Values

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = cols
              Values = vals }

    let toCSRInplace (clContext: ClContext) workGroupSize =
        let prepare = compressRows clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            let rowPointers =
                prepare processor matrix.Rows matrix.RowCount

            processor.Post(Msg.CreateFreeMsg(matrix.Rows))

            { Context = clContext
              RowCount = matrix.RowCount
              ColumnCount = matrix.ColumnCount
              RowPointers = rowPointers
              Columns = matrix.Columns
              Values = matrix.Values }

    ///<param name="clContext">.</param>
    ///<param name="opAdd">.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let elementwiseAtLeastOne<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct and 'c: equality>
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        workGroupSize
        =

        elementwise clContext (StandardOperations.atLeastOneToOption opAdd) workGroupSize

    let transposeInplace (clContext: ClContext) workGroupSize =

        let sort =
            BitonicSort.sortKeyValuesInplace clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            sort queue matrix.Columns matrix.Rows matrix.Values

            { Context = clContext
              RowCount = matrix.ColumnCount
              ColumnCount = matrix.RowCount
              Rows = matrix.Columns
              Columns = matrix.Rows
              Values = matrix.Values }

    let transpose (clContext: ClContext) workGroupSize =

        let transposeInplace = transposeInplace clContext workGroupSize
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (queue: MailboxProcessor<_>) (matrix: COOMatrix<'a>) ->
            let copiedMatrix =
                { Context = clContext
                  RowCount = matrix.RowCount
                  ColumnCount = matrix.ColumnCount
                  Rows = copy queue matrix.Rows
                  Columns = copy queue matrix.Columns
                  Values = copyData queue matrix.Values }

            transposeInplace queue copiedMatrix
