module Compustat

using Dates
using DataFrames
using YAML

using ..Pilates: WRDS

struct Field
    fields::Vector  # Fields from the raw data
    fn::Function    # Function to transform the data
    name::Symbol    # Final name of field
end

Field(F::Field) = F
Field(field::Union{Symbol, Field}, fn, name) = Field([field], fn, name)
Field(s::Symbol) = Field(s, data -> data[!, s], s)
Field(p::Pair{Symbol, Symbol}) = Field(first(p), data -> data[!, first(p)], last(p))
Field(p::Pair{Field, Symbol}) = Field(first(p), data -> data[!, first(p).name], last(p))



Field(p::Pair{<:Any, <:Function}) = Field(first(p), last(p), nameof(last(p)))
Field(pf::Pair{<:Any, <:Pair{<:Function, Symbol}}) = Field(first(pf), first(last(pf)), last(last(pf)))

function raw_fields(field::Field)
    fields_raw = Symbol[]
    for f in field.fields
        if typeof(f) == Symbol
            push!(fields_raw, f)
        else
            push!(fields_raw, raw_fields(f)...)
        end
    end
    unique(fields_raw)
end

raw_fields(fields::Vector{Field}) = unique(reduce(vcat, raw_fields.(fields)))

function table_index(wrdsuser::WRDS.WrdsUser, frequency::String)
    if frequency == "Annual"
        table = WRDS.WrdsTable(wrdsuser, "compustat", "funda")
    elseif frequency == "Quarterly"
        table = WRDS.WrdsTable(wrdsuser, "compustat", "fundq")
    else
        error("Frequency $frequency not supported. Should be 'Annual' or 'Quarterly'.")
    end
    table.index
end

function compute_field(data::DataFrame, field::Field, index::Vector{Symbol})
    # Get all raw fields
    df = data[!, [index..., raw_fields(field)...]]
    # Compute any non-raw fields
    for F in field.fields[typeof.(field.fields) .!= Symbol]
        df[!, F.name] = compute_field(df, F, index)
    end
    # Compute the field
    field.fn(df)
end

function get_raw_fields(wrdsuser::WRDS.WrdsUser, fields::Vector{Symbol}; frequency="Annual")
    # Get the original fields
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
        fields_t = [f for f in t.fields if f in (fields) && f ∉ fields_found]
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

function get_fields(wrdsuser::WRDS.WrdsUser, fields::Vector{Field}; frequency="Annual", lag=0)
    # Get all raw fields
    fields_raw = raw_fields(fields)
    if lag != 0
        if frequency == "Annual"
            :fyear ∉ fields_raw ? push!(fields_raw, :fyear) : nothing
        elseif frequency == "Quarterly"
            error("Get lagged field for quarterly Compustat data yet to be implemented.")
        end
    end

    data = get_raw_fields(wrdsuser, fields_raw; frequency=frequency)

    # Transform and rename
    index = table_index(frequency)
    for field in fields
        # Create the dataframe required by the 
        data[!, field.name] .= compute_field(data, field, index)
    end

    fields_names = [f.name for f in fields]

    # Get the lags if requested
    if lag != 0
        lag!(data, wrdsuser, fields_names; frequency=frequency, lag=lag)
    end

    # Only keep the transformed fields
    select!(data, [index..., fields_names...])
    data
end

function get_fields(wrdsuser::WRDS.WrdsUser, fields::Vector; kwargs...)
    fields = Field.(fields)
    get_fields(wrdsuser, fields; kwargs...)
end

function lag!(data::DataFrame, wrdsuser::WRDS.WrdsUser, fields::Vector{Symbol}; frequency="Annual", lag=0)
    if frequency == "Annual"
        table = WRDS.WrdsTable(wrdsuser, "compustat", "funda")
        dlag = Year(lag)
        if "fyear" ∉ names(data)
            error("Field 'fyear' is required to compute lagged variables.")
        end
        df = dropmissing(data[!, [table.index..., :fyear, fields...]], :fyear)
        df._date_ = Date.(df.fyear)
        select!(df, Not(:fyear))
    elseif frequency == "Quarterly"
        error("Get lagged field for quarterly Compustat data yet to be implemented.")
        # table = WRDS.WrdsTable(wrdsuser, "compustat", "fundq")
        # dlag = Quarter(lag)
    end
    key = [:gvkey, table.format_index...] 
    # If duplicate filings for a given lagby date, keep the one with the latest datadate
    dfg = groupby(df, [key..., :_date_])
    transform!(dfg, :datadate => maximum => :datadate_latest)
    filter!([:datadate, :datadate_latest] => (x, y) -> x .== y, df)
    select!(df, Not(:datadate_latest))
    # Check that there are no duplicates
    if length(findall(nonunique(df[!, [key..., :_date_]]))) > 0
        error("Index for table $(table.table), schema $(table.schema) and vendor $(table.vendor) is non-unique.")
    end
    # Create lagged fields
    df._date_lag_ = df._date_ .+ dlag
    names_lag = [f => Symbol("__$(String(f))__") for f in fields]
    dfm = rename(df[!, [key..., fields..., :_date_lag_]], names_lag)
    leftjoin!(df, dfm, on=[key..., :_date_ => :_date_lag_] )
    select!(df, Not([:_date_, :_date_lag_, fields...]))
    leftjoin!(data, df, on=table.index)
    select!(data, Not([fields...]))
    rename!(data, [last(f) => first(f) for f in names_lag])
end

function add_fields!(data::DataFrame, wrdsuser::WRDS.WrdsUser, fields::Vector{Field}; kwargs...)
    # Check fields do not already exist
    existing_fields = intersect(Symbol.(names(data)), [f.name for f in fields])
    if length(existing_fields) > 0
        error("Fields $existing_fields already exist in the data.")
    end
    # Add the fields to the data
    df = get_fields(wrdsuser, fields; kwargs...)
    fields_on = Symbol.([f for f ∈ names(data) if f ∈ names(df)])
    leftjoin!(data, df, on=fields_on)
end

function add_fields!(data::DataFrame, wrdsuser::WRDS.WrdsUser, fields::Vector; kwargs...)
    fields = Field.(fields)
    add_fields!(data, wrdsuser, fields; kwargs...)
end

end # module
