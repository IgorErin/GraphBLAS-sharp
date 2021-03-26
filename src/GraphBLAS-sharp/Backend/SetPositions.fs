namespace GraphBLAS.FSharp.Backend

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Backend.Common

module internal SetPositions =
    let runForMatrix (allRows: int[]) (allColumns: int[]) (allValues: 'a[]) (positions: int[]) : OpenCLEvaluation<int[] * int[] * 'a[]> = opencl {
        let prefixSumArrayLength = positions.Length

        let setPositions =
            <@
                fun (ndRange: _1D)
                    (allRowsBuffer: int[])
                    (allColumnsBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (prefixSumArrayBuffer: int[])
                    (resultRowsBuffer: int[])
                    (resultColumnsBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i = prefixSumArrayLength - 1 || i < prefixSumArrayLength && prefixSumArrayBuffer.[i] <> prefixSumArrayBuffer.[i + 1] then
                        let index = prefixSumArrayBuffer.[i]

                        resultRowsBuffer.[index] <- allRowsBuffer.[i]
                        resultColumnsBuffer.[index] <- allColumnsBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultLength = Array.zeroCreate 1

        do! PrefixSum.run positions resultLength
        let! _ = ToHost resultLength
        let resultLength = resultLength.[0]

        let resultRows = Array.zeroCreate resultLength
        let resultColumns = Array.zeroCreate resultLength
        let resultValues = Array.create resultLength Unchecked.defaultof<'a>

        do! RunCommand setPositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize positions.Length, Utils.workGroupSize)
            kernelPrepare
                ndRange
                allRows
                allColumns
                allValues
                positions
                resultRows
                resultColumns
                resultValues

        return resultRows, resultColumns, resultValues
    }

    let runForVector (allIndices: int[]) (allValues: 'a[]) (positions: int[]) : OpenCLEvaluation<int[] * 'a[]> = opencl {
        let prefixSumArrayLength = positions.Length

        let setPositions =
            <@
                fun (ndRange: _1D)
                    (allIndicesBuffer: int[])
                    (allValuesBuffer: 'a[])
                    (prefixSumArrayBuffer: int[])
                    (resultIndicesBuffer: int[])
                    (resultValuesBuffer: 'a[]) ->

                    let i = ndRange.GlobalID0

                    if i = prefixSumArrayLength - 1 || i < prefixSumArrayLength && prefixSumArrayBuffer.[i] <> prefixSumArrayBuffer.[i + 1] then
                        let index = prefixSumArrayBuffer.[i]

                        resultIndicesBuffer.[index] <- allIndicesBuffer.[i]
                        resultValuesBuffer.[index] <- allValuesBuffer.[i]
            @>

        let resultLength = Array.zeroCreate 1

        do! PrefixSum.run positions resultLength
        let! _ = ToHost resultLength
        let resultLength = resultLength.[0]

        let resultIndices = Array.zeroCreate resultLength
        let resultValues = Array.create resultLength Unchecked.defaultof<'a>

        do! RunCommand setPositions <| fun kernelPrepare ->
            let ndRange = _1D(Utils.workSize positions.Length, Utils.workGroupSize)
            kernelPrepare
                ndRange
                allIndices
                allValues
                positions
                resultIndices
                resultValues

        return resultIndices, resultValues
    }
