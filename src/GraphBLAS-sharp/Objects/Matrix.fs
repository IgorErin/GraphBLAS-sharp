namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module Matrix =
    type CSR<'a when 'a: struct> =
        { RowCount: int
          ColumnCount: int
          RowPointers: int []
          ColumnIndices: int []
          Values: 'a [] }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let rowsCount = array |> Array2D.length1
            let columnsCount = array |> Array2D.length2

            let convertedMatrix =
                [ for i in 0 .. rowsCount - 1 -> array.[i, *] |> List.ofArray ]
                |> List.map
                    (fun row ->
                        row
                        |> List.mapi (fun i x -> (x, i))
                        |> List.filter (fun pair -> not <| isZero (fst pair)))
                |> List.fold
                    (fun (rowPtrs, valueInx) row -> ((rowPtrs.Head + row.Length) :: rowPtrs), valueInx @ row)
                    ([ 0 ], [])

            { Values =
                  convertedMatrix
                  |> (snd >> List.unzip >> fst)
                  |> List.toArray
              ColumnIndices =
                  convertedMatrix
                  |> (snd >> List.unzip >> snd)
                  |> List.toArray
              RowPointers = convertedMatrix |> fst |> List.rev |> List.toArray
              RowCount = rowsCount
              ColumnCount = columnsCount }

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              RowPointers = context.CreateClArray this.RowPointers
              Columns = context.CreateClArray this.ColumnIndices
              Values = context.CreateClArray this.Values }

    type COO<'a when 'a: struct> =
        { RowCount: int
          ColumnCount: int
          Rows: int []
          Columns: int []
          Values: 'a [] }

        override this.ToString() =
            [ sprintf "COO Matrix     %ix%i \n" this.RowCount this.ColumnCount
              sprintf "RowIndices:    %A \n" this.Rows
              sprintf "ColumnIndices: %A \n" this.Columns
              sprintf "Values:        %A \n" this.Values ]
            |> String.concat ""

        static member FromTuples(rowCount: int, columnCount: int, rows: int [], columns: int [], values: 'a []) =
            { RowCount = rowCount
              ColumnCount = columnCount
              Rows = rows
              Columns = columns
              Values = values }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let rows, cols, vals =
                array
                |> Seq.cast<'a>
                |> Seq.mapi (fun idx v -> (idx / Array2D.length2 array, idx % Array2D.length2 array, v))
                |> Seq.filter (fun (_, _, v) -> not <| isZero v)
                |> Array.ofSeq
                |> Array.unzip3

            COO.FromTuples(Array2D.length1 array, Array2D.length2 array, rows, cols, vals)

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = context.CreateClArray this.Rows
              Columns = context.CreateClArray this.Columns
              Values = context.CreateClArray this.Values }

        member this.ToCSR =
            let rowIndices2rowPointers (rowIndices: int []) rowCount =
                let nnzPerRow = Array.zeroCreate rowCount
                let rowPointers = Array.zeroCreate rowCount

                Array.iter (fun rowIndex -> nnzPerRow.[rowIndex] <- nnzPerRow.[rowIndex] + 1) rowIndices

                for i in 1 .. rowCount - 1 do
                    rowPointers.[i] <- rowPointers.[i - 1] + nnzPerRow.[i - 1]

                rowPointers

            { CSR.RowCount = this.RowCount
              CSR.ColumnCount = this.ColumnCount
              CSR.RowPointers = rowIndices2rowPointers this.Rows this.RowCount
              CSR.ColumnIndices = this.Columns
              CSR.Values = this.Values }

    type CSC<'a when 'a: struct> =
        { RowCount: int
          ColumnCount: int
          RowIndices: int []
          ColumnPointers: int []
          Values: 'a [] }

        static member FromArray2D(array: 'a [,], isZero: 'a -> bool) =
            let rowsCount = array |> Array2D.length1
            let columnsCount = array |> Array2D.length2

            let convertedMatrix =
                [ for i in 0 .. columnsCount - 1 -> array.[*, i] |> List.ofArray ]
                |> List.map
                    (fun col ->
                        col
                        |> List.mapi (fun i x -> (x, i))
                        |> List.filter (fun pair -> not <| isZero (fst pair)))
                |> List.fold
                    (fun (colPtrs, valueInx) col -> ((colPtrs.Head + col.Length) :: colPtrs), valueInx @ col)
                    ([ 0 ], [])

            { Values =
                  convertedMatrix
                  |> (snd >> List.unzip >> fst)
                  |> List.toArray
              RowIndices =
                  convertedMatrix
                  |> (snd >> List.unzip >> snd)
                  |> List.toArray
              ColumnPointers = convertedMatrix |> fst |> List.rev |> List.toArray
              RowCount = rowsCount
              ColumnCount = columnsCount }

        member this.ToDevice(context: ClContext) =
            { Context = context
              RowCount = this.RowCount
              ColumnCount = this.ColumnCount
              Rows = context.CreateClArray this.RowIndices
              ColumnPointers = context.CreateClArray this.ColumnPointers
              Values = context.CreateClArray this.Values }

    type Tuples<'a> =
        { RowIndices: int []
          ColumnIndices: int []
          Values: 'a [] }

[<RequireQualifiedAccess>]
type Matrix<'a when 'a: struct> =
    | CSR of Matrix.CSR<'a>
    | COO of Matrix.COO<'a>
    | CSC of Matrix.CSC<'a>

    member this.RowCount =
        match this with
        | CSR matrix -> matrix.RowCount
        | COO matrix -> matrix.RowCount
        | CSC matrix -> matrix.RowCount

    member this.ColumnCount =
        match this with
        | CSR matrix -> matrix.ColumnCount
        | COO matrix -> matrix.ColumnCount
        | CSC matrix -> matrix.ColumnCount

    member this.NNZCount =
        match this with
        | COO m -> m.Values.Length
        | CSR m -> m.Values.Length
        | CSC m -> m.Values.Length

    member this.ToDevice(context: ClContext) =
        match this with
        | COO matrix -> ClMatrix.COO <| matrix.ToDevice context
        | CSR matrix -> ClMatrix.CSR <| matrix.ToDevice context
        | CSC matrix -> ClMatrix.CSC <| matrix.ToDevice context
