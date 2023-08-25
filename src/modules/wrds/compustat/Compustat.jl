module Compustat

using DataFrames
using YAML

using ..Pilates: WRDS

#include("../WRDS.jl")
# using .WRDS: WrdsUser, WrdsTable

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

function add_fields!(data::DataFrame, wrdsuser::WRDS.WrdsUser, fields::Vector{Symbol}; frequency="Annual")
    missing_fields = [f for f in fields if f ∉ Symbol.(names(data))]
    if length(missing_fields) > 0
        df = get_fields(wrdsuser, missing_fields; frequency=frequency)
        fields_on = Symbol.([f for f ∈ names(data) if f ∈ names(df)])
        leftjoin!(data, df, on=fields_on)
    end
end

end # module
