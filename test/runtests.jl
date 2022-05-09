using Test
using ShiftShares

using CSV
using CodecZlib: GzipDecompressorStream
using DataFrames
using FixedEffectModels
using ShiftShares: datafile

function exampledata(name::Union{Symbol,String})
    open(datafile(name)) |> GzipDecompressorStream |> read |> CSV.File
end

const tests = [
    "adh"
]

printstyled("Running tests:\n", color=:blue, bold=true)

@time for test in tests
    include("$test.jl")
    println("\033[1m\033[32mPASSED\033[0m: $(test)")
end
