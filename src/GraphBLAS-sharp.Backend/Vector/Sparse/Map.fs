﻿namespace GraphBLAS.FSharp.Backend.Vector.Sparse

open FSharp.Quotations.Evaluator.QuotationEvaluationExtensions
open Microsoft.FSharp.Quotations
open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Vector.Sparse
open GraphBLAS.FSharp.Backend.Objects.ClVector
open GraphBLAS.FSharp.Backend.Common.ClArray
open GraphBLAS.FSharp.Backend.Objects.ClCell
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

module Map =
    module WithValueOption =
        let preparePositions<'a, 'b, 'c> opAdd (clContext: ClContext) workGroupSize =

            let preparePositions (op: Expr<'a option -> 'b option -> 'c option>) =
                <@ fun (ndRange: Range1D) (operand: ClCell<'a option>) size valuesLength (indices: ClArray<int>) (values: ClArray<'b>) (resultIndices: ClArray<int>) (resultValues: ClArray<'c>) (resultBitmap: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < size then

                        let value =
                            (%Search.Bin.byKey) valuesLength gid indices values

                        match (%op) operand.Value value with
                        | Some resultValue ->
                            resultValues.[gid] <- resultValue
                            resultIndices.[gid] <- gid
                            resultBitmap.[gid] <- 1
                        | None -> resultBitmap.[gid] <- 0 @>

            let kernel =
                clContext.Compile <| preparePositions opAdd

            fun (processor: MailboxProcessor<_>) (value: ClCell<'a option>) (vector: Sparse<'b>) ->

                let resultBitmap =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vector.Size)

                let resultIndices =
                    clContext.CreateClArrayWithSpecificAllocationMode<int>(DeviceOnly, vector.Size)

                let resultValues =
                    clContext.CreateClArrayWithSpecificAllocationMode<'c>(DeviceOnly, vector.Size)

                let ndRange =
                    Range1D.CreateValid(vector.Size, workGroupSize)

                let kernel = kernel.GetKernel()

                processor.Post(
                    Msg.MsgSetArguments
                        (fun () ->
                            kernel.KernelFunc
                                ndRange
                                value
                                vector.Size
                                vector.Values.Length
                                vector.Indices
                                vector.Values
                                resultIndices
                                resultValues
                                resultBitmap)
                )

                processor.Post(Msg.CreateRunMsg<_, _> kernel)

                resultIndices, resultValues, resultBitmap

        let run<'a, 'b, 'c when 'a: struct and 'b: struct and 'c: struct>
            (clContext: ClContext)
            workGroupSize
            (op: Expr<'a option -> 'b option -> 'c option>)
            =

            let map =
                preparePositions op clContext workGroupSize

            let opOnHost = op.Evaluate()

            let setPositions =
                Common.setPositionsOption<'c> clContext workGroupSize

            let create = create clContext workGroupSize

            let init = init <@ id @> clContext workGroupSize

            fun (queue: MailboxProcessor<_>) allocationMode (value: 'a option) size ->
                function
                | Some vector ->
                    let valueClCell = clContext.CreateClCell value

                    let indices, values, bitmap = map queue valueClCell vector

                    valueClCell.Free queue

                    let result =
                        setPositions queue allocationMode values indices bitmap

                    indices.Free queue
                    values.Free queue
                    bitmap.Free queue

                    result
                    |> Option.map
                        (fun (resultValues, resultIndices) ->
                            { Context = clContext
                              Size = size
                              Indices = resultIndices
                              Values = resultValues })
                | None ->
                    opOnHost value None
                    |> Option.map
                        (fun resultValue ->
                            { Context = clContext
                              Size = size
                              Indices = init queue allocationMode size
                              Values = create queue allocationMode size resultValue })
