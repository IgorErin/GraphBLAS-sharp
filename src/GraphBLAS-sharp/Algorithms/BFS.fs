namespace GraphBLAS.FSharp.Algorithms

open GraphBLAS.FSharp.Predefined
open GraphBLAS.FSharp

module BFS =
    let levelSingleSource (matrix: Matrix<bool>) (source: int) = graphblas {
        let vertexCount = Matrix.rowCount matrix
        let! levels = Vector.zeroCreate vertexCount
        let! frontier = Vector.ofList vertexCount [source, true]

        let mutable currentLevel = 1
        while currentLevel < vertexCount do
            let! frontierMask = Vector.mask frontier
            let! currentLevelScalar = Scalar.create currentLevel
            do! levels |> Vector.fillSubVector frontierMask currentLevelScalar

            let! levelsComplemented = Vector.complemented levels
            let! frontier = (frontier, matrix) ||> Vector.vxmWithMask AnyAll.bool levelsComplemented

            currentLevel <- currentLevel + 1

        return levels
    }
