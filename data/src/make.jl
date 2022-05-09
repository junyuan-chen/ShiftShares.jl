# Convert the original example datasets to compressed CSV files

# See data/README.md for the sources of the input data files
# To regenerate the output files:
# 1) Have all input files ready in the data folder
# 2) Instantiate the package environment for data/src
# 3) Run this script with the root folder of the repository being the working directory

using CSV, CodecZlib, DataFrames, ReadStatTables

function adh()
    cols = [:czone, :year, :y, :x, :z, :wei, :l_sh_routine33, :t2, :Lsh_manuf]
    loc = DataFrame(readstat("data/location_level.dta", usecols=cols))
    loc.czone = convert(Vector{Int}, loc.czone)
    loc.year = convert(Vector{Int}, loc.year)
    open(GzipCompressorStream, "data/adh_location.csv.gz", "w") do stream
        CSV.write(stream, loc)
    end
    shares = DataFrame(readstat("data/Lshares.dta"))
    shares = shares[shares.ind_share.!=0,:]
    shares.czone = convert(Vector{Int}, shares.czone)
    shares.year = convert(Vector{Int}, shares.year)
    shares.sic87dd = convert(Vector{Int}, shares.sic87dd)
    open(GzipCompressorStream, "data/adh_share.csv.gz", "w") do stream
        CSV.write(stream, shares)
    end
    shocks = DataFrame(readstat("data/shocks.dta", usecols=[:year,:sic87dd,:g]))
    shocks.year = convert(Vector{Int}, shocks.year)
    shocks.sic87dd = convert(Vector{Int}, shocks.sic87dd)
    open(GzipCompressorStream, "data/adh_shock.csv.gz", "w") do stream
        CSV.write(stream, shocks)
    end
end

function main()
    adh()
end

main()
