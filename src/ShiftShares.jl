module ShiftShares

using DataFrames: AbstractDataFrame, DataFrame, innerjoin, leftjoin!, groupby, combine, rename!
using FixedEffectModels

export bartik, ssagg

const TermOrTerms = Union{AbstractTerm, Tuple{AbstractTerm, Vararg{AbstractTerm}}}
const TupleTerm = Tuple{TermOrTerms, Vararg{TermOrTerms}}

"""
    bartik(ldf, shares, gdf, varnames, l_id, n_on, sharename::Symbol; prefix=:b)

Construct shift-share variables with shares being `sharename` from `shares`
and shifts being `varnames` from `gdf`.
Results are merged with `ldf`.

# Arguments
- `ldf`: a data frame for the sample.
- `shares`: a data frame for the exposure shares.
- `gdf`: a data frame for the shock-level sample.
- `varnames`: column names for variables from `gdf` that are used as shifts.
- `l_id`: column name(s) that define the level of observations in `ldf`.
- `n_on`: column name(s) used for joinning `shares` and `gdf`.
- `sharename`: column name of the exposure shares in `shares`.

# Keyword
- `prefix::Symbol=:b`: prefix for the column names of the constructed variables.
"""
function bartik(ldf, shares, gdf, varnames, l_id, n_on, sharename::Symbol; prefix::Symbol=:b)
    ldf = DataFrame(ldf, copycols=false)
    shares = DataFrame(shares, copycols=false)
    gdf = DataFrame(gdf, copycols=false)
    varnames isa Symbol && (varnames = (varnames,))
    sdf = innerjoin(shares, gdf, on=n_on)
    for v in varnames
        sdf[!,Symbol(prefix,v)] = sdf[!,v] .* sdf[!,sharename]
    end
    g = groupby(sdf, l_id)
    ss = combine(g, map(x->Symbol(prefix,x)=>sum, varnames)..., renamecols=false)
    leftjoin!(ldf, ss, on=l_id)
    return ldf
end

function _partial_out(df, varnames, @nospecialize(lhs), @nospecialize(rhs), weightname)
    f = lhs ~ rhs
    df_r, _, _, _ = partial_out(df, f, weights=weightname)
    df = copy(df, copycols=false)
    for v in varnames
        df[!,v] = df_r[!,v]
    end
    return df
end

"""
    ssagg(ldf, shares, varnames, l_on, n_id, sharename; kwargs...)

Transform the original sample to the shock level for shift-share IV estimation
based on the procedure from Borusyak et al. (2021).

# Arguments
- `ldf`: a data frame for the sample.
- `shares`: a data frame for the exposure shares.
- `varnames`: column names for variables that need to be transformed to the shock level.
- `l_on`: column name(s) used for joinning `ldf` and `shares`.
- `n_id`: column name(s) that define the shock level in `shares`.
- `sharename`: column name of the exposure shares in `shares`.

# Keywords
- `weightname::Union{Symbol,Nothing}=nothing`: column name of sample weights for the original sample.
- `snname::Symbol=:s_n`: column name of sample weights to be used for the shock-level sample.
- `controls::Union{TupleTerm, Vector{<:TupleTerm}, Dict{Symbol,<:TupleTerm}, Nothing}=nothing`: column names of regressors used for residualization; multiple groups of regressors for separate transformations may be specified with a `Vector` or `Dict`.

# Reference
Borusyak, Kirill, Peter Hull, and Xavier Jaravel. 2021.
"Quasi-Experimental Shift-Share Research Designs."
The Review of Economic Studies 89 (1): 181-213.
"""
function ssagg(ldf, shares, varnames, l_on, n_id, sharename::Symbol;
        weightname::Union{Symbol,Nothing}=nothing, snname::Symbol=:s_n,
        controls::Union{TupleTerm, Vector{<:TupleTerm}, Dict{Symbol,<:TupleTerm}, Nothing}=nothing)

    ldf = DataFrame(ldf; copycols = false)
    shares = DataFrame(shares; copycols = false)
    varnames isa Symbol && (varnames = (varnames,))
    lhs = ntuple(i->term(varnames[i]), length(varnames))

    # Always demean the variables with the sample weights
    controls === nothing && (controls = (term(1),))

    if controls isa TupleTerm
        ldf_r = _partial_out(ldf, varnames, lhs, controls, weightname)
        return _ssagg(ldf_r, shares, varnames, l_on, n_id, sharename, weightname, snname)
    elseif controls isa Vector
        N = length(controls)
        out = Vector{DataFrame}(undef, N)
        for i in 1:N
            ldf_r = _partial_out(ldf, varnames, lhs, controls[i], weightname)
            out[i] = _ssagg(ldf_r, shares, varnames, l_on, n_id, sharename, weightname, snname)
        end
        return out
    elseif controls isa Dict
        out = Dict{Symbol,DataFrame}()
        for (k, v) in controls
            ldf_r = _partial_out(ldf, varnames, lhs, v, weightname)
            out[k] = _ssagg(ldf_r, shares, varnames, l_on, n_id, sharename, weightname, snname)
        end
        return out
    end
end

function _ssagg(ldf::AbstractDataFrame, shares::AbstractDataFrame, varnames,
        l_on, n_id, sharename::Symbol, weightname::Union{Symbol,Nothing}, snname::Symbol)
    df = innerjoin(ldf, shares, on=l_on)
    if weightname !== nothing
        wt = df[!,weightname]
        df[!,sharename] .*= wt
    end
    s = df[!,sharename]
    for v in varnames
        df[!,v] .*= s
    end
    gdf = groupby(df, n_id)
    out = combine(gdf, map(x->x=>sum, (sharename, varnames...))..., renamecols=false)
    for v in varnames
        out[!,v] ./= out[!,sharename]
    end
    out[!,sharename] ./= sum(out[!,sharename])
    rename!(out, sharename=>snname)
    return out
end

"""
    datafile(name::Union{Symbol,String})

Return the file path of the example data file named `name`.csv.gz.
"""
datafile(name::Union{Symbol,String}) = (@__DIR__)*"/../data/$(name).csv.gz"

end # module
