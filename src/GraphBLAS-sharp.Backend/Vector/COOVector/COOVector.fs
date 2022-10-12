namespace GraphBLAS.FSharp.Backend

open Brahma.FSharp
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Backend.Common
open Microsoft.FSharp.Quotations

module COOVector =
    let zeroCreate (clContext: ClContext) =
        let resultIndices = clContext.CreateClArray [||]
        let resultValues = clContext.CreateClArray [||]

        { ClCooVector.Context = clContext
          Indices = resultIndices
          Values = resultValues
          Size = 0 }

    let ofList (clContext: ClContext) (elements: (int * 'a) list) =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        let resultSize = elements.Length

        let resultIndices = clContext.CreateClArray indices
        let resultValues = clContext.CreateClArray values

        { ClCooVector.Context = clContext
          Indices = resultIndices
          Values = resultValues
          Size = resultSize }

    let copy (clContext: ClContext) (workGroupSize: int) =
        let copy = ClArray.copy clContext workGroupSize
        let copyData = ClArray.copy clContext workGroupSize

        fun (processor: MailboxProcessor<_>) (vector: ClCooVector<'a>) ->
            let resultIndices = copy processor vector.Indices

            let resultValues = copyData processor vector.Values

            let resultSize = vector.Size

            { ClCooVector.Context = clContext
              Indices = resultIndices
              Values = resultValues
              Size = resultSize }

    let private merge (clContext: ClContext) (workGroupSize: int) =
        let merge =
            <@ fun (ndRange: Range1D) (firstSide: int) (secondSide: int) (sumOfSides: int) (firstIndicesBuffer: ClArray<int>) (firstValuesBuffer: ClArray<'a>) (secondIndicesBuffer: ClArray<int>) (secondValuesBuffer: ClArray<'b>) (allIndicesBuffer: ClArray<int>) (firstResultValues: ClArray<'a>) (secondResultValues: ClArray<'b>) (isLeftBitMap: ClArray<int>) ->

                    let i = ndRange.GlobalID0

                    let mutable beginIdxLocal = local ()
                    let mutable endIdxLocal = local ()
                    let localID = ndRange.LocalID0

                    if localID < 2 then
                        let mutable x = localID * (workGroupSize - 1) + i - 1

                        if x >= sumOfSides then
                            x <- sumOfSides - 1

                        let diagonalNumber = x

                        let mutable leftEdge = diagonalNumber + 1 - secondSide
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstSide - 1

                        if rightEdge > diagonalNumber then
                            rightEdge <- diagonalNumber

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = firstIndicesBuffer.[middleIdx]

                            let secondIndex =
                                secondIndicesBuffer.[diagonalNumber - middleIdx]

                            if firstIndex <= secondIndex then
                                leftEdge <- middleIdx + 1
                            else
                                rightEdge <- middleIdx - 1

                        // Here localID equals either 0 or 1
                        if localID = 0 then
                            beginIdxLocal <- leftEdge
                        else
                            endIdxLocal <- leftEdge

                    barrierLocal ()

                    let beginIdx = beginIdxLocal
                    let endIdx = endIdxLocal
                    let firstLocalLength = endIdx - beginIdx
                    let mutable x = workGroupSize - firstLocalLength

                    if endIdx = firstSide then
                        x <- secondSide - i + localID + beginIdx

                    let secondLocalLength = x

                    //First indices are from 0 to firstLocalLength - 1 inclusive
                    //Second indices are from firstLocalLength to firstLocalLength + secondLocalLength - 1 inclusive
                    let localIndices = localArray<int> workGroupSize

                    if localID < firstLocalLength then
                        localIndices.[localID] <- firstIndicesBuffer.[beginIdx + localID]

                    if localID < secondLocalLength then
                        localIndices.[firstLocalLength + localID] <- secondIndicesBuffer.[i - beginIdx]

                    barrierLocal ()

                    if i < sumOfSides then
                        let mutable leftEdge = localID + 1 - secondLocalLength
                        if leftEdge < 0 then leftEdge <- 0

                        let mutable rightEdge = firstLocalLength - 1

                        if rightEdge > localID then
                            rightEdge <- localID

                        while leftEdge <= rightEdge do
                            let middleIdx = (leftEdge + rightEdge) / 2
                            let firstIndex = localIndices.[middleIdx]

                            let secondIndex =
                                localIndices.[firstLocalLength + localID - middleIdx]

                            if firstIndex <= secondIndex then
                                leftEdge <- middleIdx + 1
                            else
                                rightEdge <- middleIdx - 1

                        let boundaryX = rightEdge
                        let boundaryY = localID - leftEdge

                        // boundaryX and boundaryY can't be off the right edge of array (only off the left edge)
                        let isValidX = boundaryX >= 0
                        let isValidY = boundaryY >= 0

                        let mutable fstIdx = 0

                        if isValidX then
                            fstIdx <- localIndices.[boundaryX]

                        let mutable sndIdx = 0

                        if isValidY then
                            sndIdx <- localIndices.[firstLocalLength + boundaryY]

                        if not isValidX || isValidY && fstIdx <= sndIdx then
                            allIndicesBuffer.[i] <- sndIdx
                            secondResultValues.[i] <- secondValuesBuffer.[i - localID - beginIdx + boundaryY]
                            isLeftBitMap.[i] <- 0
                        else
                            allIndicesBuffer.[i] <- fstIdx
                            firstResultValues.[i] <- firstValuesBuffer.[beginIdx + boundaryX]
                            isLeftBitMap.[i] <- 1 @>

        let kernel = clContext.Compile(merge)

        fun (processor: MailboxProcessor<_>) (firstIndices: ClArray<int>) (firstValues: ClArray<'a>) (secondIndices: ClArray<int>) (secondValues: ClArray<'b>) ->
            let firstSide = firstIndices.Length

            let secondSide = secondIndices.Length

            let sumOfSides = firstIndices.Length + secondIndices.Length

            let allIndices =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let firstResultValues =
                clContext.CreateClArray<'a>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let secondResultValues =
                clContext.CreateClArray<'b>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let isLeftBitmap =
                clContext.CreateClArray<int>(
                    sumOfSides,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(sumOfSides, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                            firstSide
                            secondSide
                            sumOfSides
                            firstIndices
                            firstValues
                            secondIndices
                            secondValues
                            allIndices
                            firstResultValues
                            secondResultValues
                            isLeftBitmap)
            )

            processor.Post(Msg.CreateRunMsg<_, _>(kernel))

            allIndices, firstResultValues, secondResultValues, isLeftBitmap

    let private preparePositionsAtLeasOne
        (clContext: ClContext)
        (opAdd: Expr<AtLeastOne<'a, 'b> -> 'c option>)
        (workGroupSize: int)
        =

        let preparePositions =
            <@  fun (ndRange: Range1D) length (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) (allValues: ClArray<'c>) (positions: ClArray<int>) ->

                    let gid = ndRange.GlobalID0

                    if gid < length - 1 && allIndices[gid] = allIndices[gid + 1] then
                        positions[gid] <- 0

                        match (%opAdd) (Both (leftValues[gid + 1], rightValues[gid])) with
                        | Some value ->
                            allValues[gid + 1] <- value
                            positions[gid + 1] <- 1
                        | None ->
                            positions[gid + 1] <- 1
                    elif (gid < length && gid > 0 && allIndices[gid - 1] <> allIndices[gid]) || gid = 0 then
                        if isLeft[gid] = 1 then
                            match (%opAdd) (Left leftValues[gid]) with
                            | Some value ->
                                allValues[gid] <- value
                                positions[gid] <- 1
                            | None ->
                                positions[gid] <- 0
                        else
                            match (%opAdd) (Right rightValues[gid]) with
                            | Some value ->
                                allValues[gid] <- value
                                positions[gid] <- 1
                            | None ->
                                positions[gid] <- 0
            @>

        let kernel = clContext.Compile(preparePositions)

        fun (processor: MailboxProcessor<_>) (allIndices: ClArray<int>) (leftValues: ClArray<'a>) (rightValues: ClArray<'b>) (isLeft: ClArray<int>) ->

            let length = allIndices.Length

            let allValues =
                clContext.CreateClArray(
                    length,
                    hostAccessMode = HostAccessMode.NotAccessible,
                    deviceAccessMode = DeviceAccessMode.WriteOnly,
                    allocationMode = AllocationMode.Default
                )

            let ndRange = Range1D.CreateValid(length, workGroupSize)

            let kernel = kernel.GetKernel ()

            processor.Post(
                Msg.MsgSetArguments(
                    fun () ->
                        kernel.KernelFunc
                            ndRange
                    )
                )


