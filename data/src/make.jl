# Convert the original example datasets to compressed CSV files

# See data/README.md for the sources of the input data files
# To regenerate the output files:
# 1) Have all input files ready in the data folder
# 2) Instantiate the package environment for data/src
# 3) Run this script with the root folder of the repository being the working directory

using CSV, CodecZlib, DataFrames, ReadStatTables

function adh()
    cols = [:czone, :year, :y, :x, :z, :wei, :l_sh_routine33, :t2, :Lsh_manuf]
    lfile = DataFrame(readstat("data/location_level.dta", usecols=cols))
    lfile.czone = convert(Vector{Int}, lfile.czone)
    lfile.year = convert(Vector{Int}, lfile.year)
    open(GzipCompressorStream, "data/adh_location.csv.gz", "w") do stream
        CSV.write(stream, lfile)
    end
    sfile = DataFrame(readstat("data/Lshares.dta"))
    sfile = sfile[sfile.ind_share.!=0,:]
    sfile.czone = convert(Vector{Int}, sfile.czone)
    sfile.year = convert(Vector{Int}, sfile.year)
    sfile.sic87dd = convert(Vector{Int}, sfile.sic87dd)
    open(GzipCompressorStream, "data/adh_share.csv.gz", "w") do stream
        CSV.write(stream, sfile)
    end
end

function main()
    adh()
end

main()
