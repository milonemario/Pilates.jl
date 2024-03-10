module Ibes

using Statistics
using Dates
using DataFrames

using ..Pilates: WRDS

function add_ticker_from_permno!(wrdsuser::WRDS.WrdsUser, data::DataFrame)
    # Check data integrity
    "gvkey" ∉ names(data) ? error("Column 'gvkey' not available in user data.") : nothing
    "datadate" ∉ names(data) ? error("Column 'datadate' not available in user data.") : nothing
    df = unique(data[!, [:gvkey, :datadate]])
    # Open the link data
    linktable = WRDS.WrdsTable(wrdsuser, "ibes", "linktable")
    link = WRDS.get_fields(linktable, [:ticker, :permno, :ncusip, :sdate, :edate, :score])
    dropmissing!(link)
    # rename!(link, :lpermno => :permno)

    # Merge the permno with the given 'linkprim' priority: P C J N
    df.permno .= missing
    for linkprim in ["P", "C", "J", "N"]
        dft = df[ismissing.(df.permno), [:gvkey, :datadate]]
        l = link[link.linkprim .== linkprim, :]
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

function add_ticker!(wrdsuser::WRDS.WrdsUser, data::DataFrame, from::Symbol; kwargs...)
    if "ticker" ∉ names(data)
        if from == :permno
            add_ticker_from_permno!(wrdsuser, data; kwargs...)
        elseif from == :gvkey
            error("Adding IBES ticker directly from $from is not supported. Consider adding the CRSP permno firt.")
        else
            error("Adding IBES ticker from $from is not supported.")
        end
    end
end

end # module
