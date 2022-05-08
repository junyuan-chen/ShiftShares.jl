module ShiftShares

using DataFrames: AbstractDataFrame, DataFrame, innerjoin, groupby, combine
using FixedEffectModels

export ssagg

const TermOrTerms = Union{AbstractTerm, Tuple{AbstractTerm, Vararg{AbstractTerm}}}
const TupleTerm = Tuple{TermOrTerms, Vararg{TermOrTerms}}

function _partial_out(ldf, varnames, @nospecialize(lhs), @nospecialize(rhs), weightname)
    f = lhs ~ rhs
    ldf_r, _, _, _ = partial_out(ldf, f, weights=weightname)
    ldf = copy(ldf, copycols=false)
    for v in varnames
        ldf[!,v] = ldf_r[!,v]
    end
    return ldf
end

"""
    ssagg(ldf, shares, varnames, l_on, n_on, sharename; kwargs...)

Transform the original sample to the shock level for shift-share IV estimation
based on the procedure from Borusyak et al. (2021).

# Reference
Borusyak, Kirill, Peter Hull, and Xavier Jaravel. 2021.
"Quasi-Experimental Shift-Share Research Designs."
The Review of Economic Studies 89 (1): 181-213.
"""
function ssagg(ldf, shares, varnames, l_on, n_on, sharename::Symbol;
        timename::Union{Symbol,Nothing}=nothing,
        weightname::Union{Symbol,Nothing}=nothing,
        controls::Union{TupleTerm, Vector{<:TupleTerm}, Dict{Symbol,<:TupleTerm}, Nothing}=nothing)

    ldf = DataFrame(ldf; copycols = false)
    shares = DataFrame(shares; copycols = false)
    varnames isa Symbol && (varnames = (varnames,))
    lhs = ntuple(i->term(varnames[i]), length(varnames))

    if timename !== nothing
        l_on = l_on isa Symbol ? [l_on, timename] : [l_on..., timename]
        n_on = n_on isa Symbol ? [n_on, timename] : [n_on..., timename]
    end

    # Always demean the variables with the sample weights
    controls === nothing && (controls = (term(1),))

    if controls isa TupleTerm
        ldf_r = _partial_out(ldf, varnames, lhs, controls, weightname)
        return _ssagg(ldf_r, shares, varnames, l_on, n_on, sharename, weightname)
    elseif controls isa Vector
        N = length(controls)
        out = Vector{DataFrame}(undef, N)
        for i in 1:N
            ldf_r = _partial_out(ldf, varnames, lhs, controls[i], weightname)
            out[i] = _ssagg(ldf_r, shares, varnames, l_on, n_on, sharename, weightname)
        end
        return out
    elseif controls isa Dict
        out = Dict{Symbol,DataFrame}()
        for (k, v) in controls
            ldf_r = _partial_out(ldf, varnames, lhs, v, weightname)
            out[k] = _ssagg(ldf_r, shares, varnames, l_on, n_on, sharename, weightname)
        end
        return out
    end
end

function _ssagg(ldf::AbstractDataFrame, shares::AbstractDataFrame, varnames,
        l_on, n_on, sharename::Symbol, weightname::Union{Symbol,Nothing})
    df = innerjoin(ldf, shares, on=l_on)
    if weightname !== nothing
        wt = df[!,weightname]
        df[!,sharename] .*= wt
    end
    s = df[!,sharename]
    for v in varnames
        df[!,v] .*= s
    end
    gdf = groupby(df, n_on)
    out = combine(gdf, map(x->x=>sum, (sharename, varnames...))..., renamecols=false)
    for v in varnames
        out[!,v] ./= out[!,sharename]
    end
    out[!,sharename] ./= sum(out[!,sharename])
    return out
end

datafile(name::Union{Symbol,String}) = (@__DIR__)*"/../data/$(name).csv.gz"

end # module
