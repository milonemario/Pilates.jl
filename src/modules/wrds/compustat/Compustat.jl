module Compustat

using Dates
using DataFrames
using YAML

using ..Pilates: WRDS

function get_fields(wrdsuser::WRDS.WrdsUser, fields::Vector{Symbol}; frequency="Annual")
    tables_yml = YAML.load_file("$(@__DIR__)/files.yaml")
    tables_names = [t for t in keys(tables_yml) if t ∉ ["funda", "fundq"]]
    if frequency == "Annual"
        push!(tables_names, "funda")
    elseif frequency == "Quarterly"
        push!(tables_names, "fundq")
    else
        error("Frequency $frequency not supported. Should be 'Annual' or 'Quarterly'.")
    end

    tables_all = WRDS.WrdsTable.([wrdsuser], ["compustat"], tables_names)
    tables = WRDS.WrdsTable[]
    # Keep the tables that have data and avoid duplicated fields
    fields_found = Symbol[]
    fields_for_table = Dict{String, Vector{Symbol}}()
    for t in tables_all
        fields_t = [f for f in t.fields if f in fields && f ∉ fields_found]
        append!(fields_found, fields_t)
        if length(fields_t) > 0
            push!(tables, t)
            push!(fields_for_table, t.table => fields_t)
        end
    end

    # Get the data from the first table
    data = WRDS.get_fields(tables[1], fields_for_table[tables[1].table])
    # Add the data from the other tables
    for t in tables[2:end]
        df = WRDS.get_fields(t, fields_for_table[t.table])
        fields_on = [f for f ∈ t.index if f ∈ Symbol.(names(data))]
        if length(t.index) >= length(fields_on)
            data = leftjoin(df, data, on=fields_on)
        else
            data = leftjoin(data, df, on=fields_on)
        end
    end
    data
end

function get_lag(data::DataFrame, key::Vector{Symbol}, dlag::Dates.AbstractTime, lagdate::Symbol)
end

function get_fields(wrdsuser::WRDS.WrdsUser, fields::Vector{Pair{Symbol, Symbol}};
    frequency="Annual", lag=0)
    data = get_fields(wrdsuser, first.(fields); frequency=frequency) 
    if lag != 0
        if frequency == "Annual"
            table = WRDS.WrdsTable(wrdsuser, "compustat", "funda")
            dlag = Year(lag)
            add_fields!(data, wrdsuser, [:fyear])
            dropmissing!(data, :fyear)
            data._date_ = Date.(data.fyear)
            select!(data, Not(:fyear))
        elseif frequency == "Quarterly"
            error("Get lagged field for quarterly Compustat data yet to be implemented.")
            # table = WRDS.WrdsTable(wrdsuser, "compustat", "fundq")
            # dlag = Quarter(lag)
        end
        key = [:gvkey, table.format_index...] 
        # If duplicate filings for a given lagby date, keep the one with the latest datadate
        dfg = groupby(data, [key..., :_date_])
        transform!(dfg, :datadate => maximum => :datadate_latest)
        filter!([:datadate, :datadate_latest] => (x, y) -> x .== y, data)
        select!(data, Not(:datadate_latest))
        # Check that there are no duplicates
        if length(findall(nonunique(data[!, [key..., :_date_]]))) > 0
            error("Index for table $(table.table), schema $(table.schema) and vendor $(table.vendor) is non-unique.")
        end
        # Create lagged fields
        data._date_lag_ = data._date_ .+ dlag
        data_lag = rename(data[!, [key..., first.(fields)..., :_date_lag_]], fields)
        leftjoin!(data, data_lag, on=[key..., :_date_ => :_date_lag_] )
        select!(data, Not([:_date_, :_date_lag_, first.(fields)...]))
    else
        rename!(data, fields)
    end
    data
end

function add_fields!(data::DataFrame, wrdsuser::WRDS.WrdsUser, fields::Union{Vector{Symbol}, Vector{Pair{Symbol, Symbol}}}; kwargs...)
    # Check fields do not already exist
    fields_names = fields
    if typeof(fields) == Vector{Pair{Symbol, Symbol}}
        fields_names = last.(fields)
    end
    existing_fields = intersect(Symbol.(names(data)), fields_names)
    if length(existing_fields) > 0
        error("Fields $existing_fields already exist in the data.")
    end
    # Add the fields to the data
    df = get_fields(wrdsuser, fields; kwargs...)
    fields_on = Symbol.([f for f ∈ names(data) if f ∈ names(df)])
    leftjoin!(data, df, on=fields_on)
end

end # module
