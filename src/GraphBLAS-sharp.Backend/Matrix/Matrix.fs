namespace GraphBLAS.FSharp.Backend.Matrix

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Matrix.COO
open GraphBLAS.FSharp.Backend.Matrix.CSR
open GraphBLAS.FSharp.Backend.Objects
open GraphBLAS.FSharp.Backend.Objects.ClMatrix

module Matrix =
    let copy (clContext: ClContext) workGroupSize flag =
        let copy = ClArray.copy clContext workGroupSize flag

        let copyData = ClArray.copy clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m ->
                ClMatrix.COO
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }
            | ClMatrix.CSR m ->
                ClMatrix.CSR
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      RowPointers = copy processor m.RowPointers
                      Columns = copy processor m.Columns
                      Values = copyData processor m.Values }
            | ClMatrix.CSC m ->
                ClMatrix.CSC
                    { Context = clContext
                      RowCount = m.RowCount
                      ColumnCount = m.ColumnCount
                      Rows = copy processor m.Rows
                      ColumnPointers = copy processor m.ColumnPointers
                      Values = copyData processor m.Values }

    /// <summary>
    /// Creates a new matrix, represented in CSR format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSR (clContext: ClContext) workGroupSize flag =
        let toCSR = COOMatrix.toCSR clContext workGroupSize flag
        let copy = copy clContext workGroupSize flag

        let transpose =
            CSRMatrix.transpose clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m -> toCSR processor m |> ClMatrix.CSR
            | ClMatrix.CSR _ -> copy processor matrix
            | ClMatrix.CSC m ->

                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }

                |> transpose processor
                |> ClMatrix.CSR

    /// <summary>
    /// Returns the matrix, represented in CSR format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSRInplace (clContext: ClContext) workGroupSize flag =
        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize flag

        let transposeInplace =
            CSRMatrix.transposeInplace clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO m -> toCSRInplace processor m |> ClMatrix.CSR
            | ClMatrix.CSR _ -> matrix
            | ClMatrix.CSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }

                |> transposeInplace processor
                |> ClMatrix.CSR

    /// <summary>
    /// Creates a new matrix, represented in COO format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOO (clContext: ClContext) workGroupSize flag =
        let toCOO = CSRMatrix.toCOO clContext workGroupSize flag
        let copy = copy clContext workGroupSize flag

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> copy processor matrix
            | ClMatrix.CSR m -> toCOO processor m |> ClMatrix.COO
            | ClMatrix.CSC m ->

                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }

                |> toCOO processor
                |> transposeInplace processor
                |> ClMatrix.COO

    /// <summary>
    /// Returns the matrix, represented in COO format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCOOInplace (clContext: ClContext) workGroupSize flag =
        let toCOOInplace =
            CSRMatrix.toCOOInplace clContext workGroupSize flag

        let transposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.COO _ -> matrix
            | ClMatrix.CSR m -> toCOOInplace processor m |> ClMatrix.COO
            | ClMatrix.CSC m ->

                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }

                |> toCOOInplace processor
                |> transposeInplace processor
                |> ClMatrix.COO

    /// <summary>
    /// Creates a new matrix, represented in CSC format, that is equal to the given one.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSC (clContext: ClContext) workGroupSize flag =
        let toCSR = COOMatrix.toCSR clContext workGroupSize flag
        let copy = copy clContext workGroupSize flag

        let transposeCSR =
            CSRMatrix.transpose clContext workGroupSize flag

        let transposeCOO =
            COOMatrix.transpose clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> copy processor matrix
            | ClMatrix.CSR m ->
                let csrT = transposeCSR processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                let csrT =
                    transposeCOO processor m |> toCSR processor

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrix.CSC

    /// <summary>
    /// Returns the matrix, represented in CSC format, that is equal to the given one.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// </summary>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let toCSCInplace (clContext: ClContext) workGroupSize flag =
        let toCSRInplace =
            COOMatrix.toCSRInplace clContext workGroupSize flag

        let transposeCSRInplace =
            CSRMatrix.transposeInplace clContext workGroupSize flag

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (matrix: ClMatrix<'a>) ->
            match matrix with
            | ClMatrix.CSC _ -> matrix
            | ClMatrix.CSR m ->
                let csrT = transposeCSRInplace processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrix.CSC
            | ClMatrix.COO m ->
                let csrT =
                    toCSRInplace processor
                    <| transposeCOOInplace processor m

                { Context = csrT.Context
                  RowCount = csrT.ColumnCount
                  ColumnCount = csrT.RowCount
                  Rows = csrT.Columns
                  ColumnPointers = csrT.RowPointers
                  Values = csrT.Values }
                |> ClMatrix.CSC

    let elementwise (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize flag =
        let COOElementwise =
            COOMatrix.elementwise clContext opAdd workGroupSize flag

        let CSRElementwise =
            CSRMatrix.elementwise clContext opAdd workGroupSize flag

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 -> COOElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 -> CSRElementwise processor m1 m2 |> ClMatrix.CSR
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                let resT = CSRElementwise processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> ClMatrix.CSC
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseToCOO (clContext: ClContext) (opAdd: Expr<'a option -> 'b option -> 'c option>) workGroupSize flag =
        let COOElementwise =
            COOMatrix.elementwise clContext opAdd workGroupSize flag

        let CSRElementwise =
            CSRMatrix.elementwiseToCOO clContext opAdd workGroupSize flag

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 -> COOElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 -> CSRElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                CSRElementwise processor csrT1 csrT2
                |> transposeCOOInplace processor
                |> ClMatrix.COO
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseAtLeastOne (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize flag =
        let COOElementwise =
            COOMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize flag

        let CSRElementwise =
            CSRMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize flag

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 -> COOElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 -> CSRElementwise processor m1 m2 |> ClMatrix.CSR
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                let resT = CSRElementwise processor csrT1 csrT2

                { Context = resT.Context
                  RowCount = resT.ColumnCount
                  ColumnCount = resT.RowCount
                  Rows = resT.Columns
                  ColumnPointers = resT.RowPointers
                  Values = resT.Values }
                |> ClMatrix.CSC
            | _ -> failwith "Matrix formats are not matching"

    let elementwiseAtLeastOneToCOO (clContext: ClContext) (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>) workGroupSize flag =
        let COOElementwise =
            COOMatrix.elementwiseAtLeastOne clContext opAdd workGroupSize flag

        let CSRElementwise =
            CSRMatrix.elementwiseAtLeastOneToCOO clContext opAdd workGroupSize flag

        let transposeCOOInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix1 matrix2 ->
            match matrix1, matrix2 with
            | ClMatrix.COO m1, ClMatrix.COO m2 -> COOElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSR m1, ClMatrix.CSR m2 -> CSRElementwise processor m1 m2 |> ClMatrix.COO
            | ClMatrix.CSC m1, ClMatrix.CSC m2 ->
                let csrT1 =
                    { Context = m1.Context
                      RowCount = m1.ColumnCount
                      ColumnCount = m1.RowCount
                      RowPointers = m1.ColumnPointers
                      Columns = m1.Rows
                      Values = m1.Values }

                let csrT2 =
                    { Context = m2.Context
                      RowCount = m2.ColumnCount
                      ColumnCount = m2.RowCount
                      RowPointers = m2.ColumnPointers
                      Columns = m2.Rows
                      Values = m2.Values }

                let resT = CSRElementwise processor csrT1 csrT2
                ClMatrix.COO <| transposeCOOInplace processor resT
            | _ -> failwith "Matrix formats are not matching"

    /// <summary>
    /// Transposes the given matrix and returns result.
    /// The given matrix should neither be used afterwards nor be disposed.
    /// The storage format is not guaranteed to be preserved.
    /// </summary>
    /// <remarks>
    /// The format changes according to the following:
    /// * COO -> COO
    /// * CSR -> CSC
    /// * CSC -> CSR
    /// </remarks>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transposeInplace (clContext: ClContext) workGroupSize =
        let COOtransposeInplace =
            COOMatrix.transposeInplace clContext workGroupSize

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrix.COO m -> COOtransposeInplace processor m |> ClMatrix.COO
            | ClMatrix.CSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = m.Columns
                  ColumnPointers = m.RowPointers
                  Values = m.Values }
                |> ClMatrix.CSC
            | ClMatrix.CSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = m.ColumnPointers
                  Columns = m.Rows
                  Values = m.Values }
                |> ClMatrix.CSR

    /// <summary>
    /// Transposes the given matrix and returns result as a new matrix.
    /// The storage format is not guaranteed to be preserved.
    /// </summary>
    /// <remarks>
    /// The format changes according to the following:
    /// * COO -> COO
    /// * CSR -> CSC
    /// * CSC -> CSR
    /// </remarks>
    ///<param name="clContext">OpenCL context.</param>
    ///<param name="workGroupSize">Should be a power of 2 and greater than 1.</param>
    let transpose (clContext: ClContext) workGroupSize flag =
        let COOtranspose =
            COOMatrix.transpose clContext workGroupSize flag

        let copy = ClArray.copy clContext workGroupSize flag
        let copyData = ClArray.copy clContext workGroupSize flag

        fun (processor: MailboxProcessor<_>) matrix ->
            match matrix with
            | ClMatrix.COO m -> COOtranspose processor m |> ClMatrix.COO
            | ClMatrix.CSR m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  Rows = copy processor m.Columns
                  ColumnPointers = copy processor m.RowPointers
                  Values = copyData processor m.Values }
                |> ClMatrix.CSC
            | ClMatrix.CSC m ->
                { Context = m.Context
                  RowCount = m.ColumnCount
                  ColumnCount = m.RowCount
                  RowPointers = copy processor m.ColumnPointers
                  Columns = copy processor m.Rows
                  Values = copyData processor m.Values }
                |> ClMatrix.CSR

    let mxm
        (opAdd: Expr<'c -> 'c -> 'c option>)
        (opMul: Expr<'a -> 'b -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let runCSRnCSC =
            CSRMatrix.spgemmCSC clContext workGroupSize opAdd opMul

        fun (queue: MailboxProcessor<_>) (matrix1: ClMatrix<'a>) (matrix2: ClMatrix<'b>) (mask: ClMask2D) ->

            match matrix1, matrix2, mask.IsComplemented with
            | ClMatrix.CSR m1, ClMatrix.CSC m2, false -> runCSRnCSC queue m1 m2 mask |> ClMatrix.COO
            | _ -> failwith "Matrix formats are not matching"
