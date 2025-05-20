module Crsp

using Statistics
using Dates
using DataFrames

using ..Pilates: WRDS

function add_permno_from_gvkey!(wrdsuser::WRDS.WrdsUser, data::DataFrame, linktypes=["LU", "LC", "LS"])
    # Check data integrity
    "gvkey" ∉ names(data) ? error("Column 'gvkey' not available in user data.") : nothing
    "datadate" ∉ names(data) ? error("Column 'datadate' not available in user data.") : nothing
    df = unique(data[!, [:gvkey, :datadate]])
    # Open the link data
    linktable = WRDS.WrdsTable(wrdsuser, "crsp_a_ccm", "ccmxpf_lnkhist", [])
    link = WRDS.get_fields(linktable, [:gvkey, :lpermno, :linktype, :linkprim, :linkdt, :linkenddt])
    dropmissing!(link, [:gvkey, :lpermno, :linktype, :linkprim])
    filter!(:linktype => x -> x ∈ linktypes, link)
    # rename!(link, :lpermno => :permno)

    # Merge the permno with the given 'linkprim' priority: P C J N
    df.permno .= missing
    for linkprim in ["P", "C", "J", "N"]
        dft = df[ismissing.(df.permno), [:gvkey, :datadate]]
        l = link[link.linkprim.==linkprim, :]
        dfl = innerjoin(dft, l, on=:gvkey)
        filter!([:datadate, :linkdt, :linkenddt] => (x, y, z) -> x .>= y .&& (ismissing.(z) .|| x .<= z), dfl)
        # Check for duplicates
        dfl_dup = findall(nonunique(dfl[!, [:gvkey, :datadate, :lpermno]]))
        if length(dfl_dup) > 0
            error("Duplicate gvkey-permno found.")
        end
        # Merge the data
        select!(dfl, [:gvkey, :datadate, :lpermno])
        leftjoin!(df, dfl, on=[:gvkey, :datadate])
        transform!(df, [:lpermno, :permno] => ByRow((x, y) -> ismissing(y) ? x : y) => :permno)
        select!(df, [:gvkey, :datadate, :permno])
    end

    # Add permno to user data
    leftjoin!(data, df, on=[:gvkey, :datadate])
end

function add_permno!(wrdsuser::WRDS.WrdsUser, data::DataFrame, from::Symbol; kwargs...)
    if "permno" ∉ names(data)
        if from == :gvkey
            add_permno_from_gvkey!(wrdsuser, data; kwargs...)
        else
            error("Adding permno from $from is not supported.")
        end
    end
end

function compounded_return(retv::Vector; logreturn=false)
    logret = sum(log.(1.0 .+ retv))
    logreturn ? ret = logret : ret = exp(logret) - 1.0
    ret
end

function compute_dsf(f::Function, var::Symbol, dfvarg::GroupedDataFrame, permno, date::Date, from::Dates.CompoundPeriod, to::Dates.CompoundPeriod; kwargs...)
    # Function to compute compounded return for one permno and date
    # dfvarg is expected to be the data for the variable var grouped by permno
    # Returns for the permno
    dfp = dfvarg[(permno,)]
    # Select the range to compute the compounded return
    startdate = date + from
    enddate = date + to
    # Only compute if enough available data
    stat = missing
    if minimum(dfp.date) <= startdate && maximum(dfp.date) >= enddate
        varv = dfp[startdate.<=dfp.date.&&dfp.date.<=enddate, var]
        stat = f(varv; kwargs...)
    end
    stat
end

function compute_dsf!(f::Function, var::Symbol, wrdsuser::WRDS.WrdsUser, data::DataFrame, datecol::Symbol, newcol::Symbol, from::Dates.AbstractTime, to::Dates.AbstractTime; kwargs...)
    # Compute statistic f using variable var from the dsf (daily stock file) data
    # Check data validity
    String(datecol) ∉ names(data) ? error("Date column $(String(datecol)) not available in the user data.") : nothing
    "permno" ∉ names(data) ? error("Column 'permno' required in the user data.") : nothing
    # Open the return data
    dsf = WRDS.WrdsTable(wrdsuser, "crsp_a_stock", "dsf", [:permno, :date])
    dfvar = WRDS.get_fields(dsf, [var])
    # Pregroup the return data by permno
    dropmissing!(dfvar)
    dfvarg = groupby(dfvar, :permno)
    # Use the non-missing user data
    data[!, newcol] .= missing
    df = @view data[.!ismissing.(data.permno), [:permno, datecol, newcol]]
    # Compute the funcion over the returns
    cret(permno, date) = compute_dsf(f, var, dfvarg, permno, date, canonicalize(from), canonicalize(to); kwargs...)
    transform!(df, [:permno, datecol] => ByRow((x, y) -> cret(x, y)) => newcol)
    nothing
end

function compounded_return!(wrdsuser::WRDS.WrdsUser, data::DataFrame, datecol::Symbol, newcol::Symbol, from::Dates.AbstractTime, to::Dates.AbstractTime; logreturn=false)
    compute_dsf!(compounded_return, :ret, wrdsuser, data, datecol, newcol, from, to; logreturn=logreturn)
end

function volatility_return!(wrdsuser::WRDS.WrdsUser, data::DataFrame, datecol::Symbol, newcol::Symbol, from::Dates.AbstractTime, to::Dates.AbstractTime)
    compute_dsf!(Statistics.std, :ret, wrdsuser, data, datecol, newcol, from, to)
end

end # module
