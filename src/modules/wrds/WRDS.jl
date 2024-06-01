module WRDS

# Handles the connection to WRDS and data donwloads

using LibPQ
using DataFrames
using Parquet2: Dataset, writefile, select, append!
using CSV
using SASLib
using YAML
using Dates
using ProgressBars

import Base: convert

convert(t::Type{Int32}, s::String) = parse(t, s)

WRDS_POSTGRES_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_POSTGRES_PORT = 9737
WRDS_POSTGRES_DB = "wrds"

mutable struct WrdsUser
    username::String
    conn::Union{Nothing, LibPQ.Connection}
end

WrdsUser(username::String) = WrdsUser(username, nothing)

struct WrdsTable
    wrdsuser::WrdsUser
    schema::String
    table::String
    index::Vector{Symbol}
    fields::Vector{Symbol}
    types::Dict{Symbol, DataType}
    groups::Union{Nothing, Vector{Symbol}}
    where::String   # SQL WHERE clause always applied to the requests
end

function get_table_info(wrdsuser::WrdsUser, schema::String, tablename::String, tableindex::Vector{String})
    isnothing(wrdsuser.conn) || status(wrdsuser.conn) == "CONNECTION_BAD" ? connect(wrdsuser) : nothing
    query = "SELECT * FROM information_schema.columns WHERE table_schema = '$(schema)' AND table_name = '$(tablename)';"
    result = execute(wrdsuser.conn, query)
    data = DataFrame(result)
    # Make sure the provided index is possible
    isacolumn(x) = in(x, data.column_name)
    if !all(isacolumn.(tableindex))
        error("The given index is not compatible with the table columns (one or more missing columns)")
    end
    # Remap types
    types = Dict(
        "Int32" => data[in(["integer", "smallint", "bigint", "smallserial", "serial", "bigserial"]).(data.data_type), :column_name],
        "Float32" => data[in(["decimal", "numeric", "real", "double precision"]).(data.data_type), :column_name],
        "Date" => data[in(["date"]).(data.data_type), :column_name],
        "String" => data[in(["character", "char", "bpchar", "text", "character varying"]).(data.data_type), :column_name],
    )
    if sum([length(t) for t in values(types)]) != length(data.column_name)
        @warn "Not all columns types could be determined."
    end
    # Close the connection
    close(wrdsuser.conn)
    # Return the information
    Dict(
        "schema" => schema,
        "table" => tablename,
        "fields" => data.column_name,
        "index" => tableindex,
        "types" => types
    )
end

function update_index(wrdsuser::WrdsUser, schema::String, tablename::String, tableindex::Vector{String})
    tn_index = tablename_index(schema, tablename) 
    if isfile(index_file())
        index = YAML.load_file(index_file())
        if tn_index ∈ keys(index)
            if index[tn_index]["index"] != tableindex
                # Just change the tableindex in the index
                isacolumn(x) = in(x, index[tn_index]["fields"])
                if !all(isacolumn.(tableindex))
                    error("The given index is not compatible with the table columns (one or more missing columns)")
                end
                index[tn_index]["index"] = tableindex
                YAML.write_file(index_file(), index)
            end
        else
            table_info = get_table_info(wrdsuser, schema, tablename, tableindex)
            index[tn_index] = table_info
            YAML.write_file(index_file(), index)
        end
    else
        table_info = get_table_info(wrdsuser, schema, tablename, tableindex)
        index = Dict()
        index[tn_index] = table_info
        YAML.write_file(index_file(), index)
    end
end

function index_file()
    "pilates_index.yaml"
end

function tablename_index(schema::String, tablename::String)
    "$(schema).$(tablename)"
end

function WrdsTable(wrdsuser::WrdsUser, schema::String, tablename::String, tableindex::Vector;
    format_index::Vector{String}=Vector{String}([]), groups::Vector{String}=Vector{String}([]), where="")
    update_index(wrdsuser, schema, tablename, String.(tableindex))
    index = YAML.load_file(index_file())
    tn_index = tablename_index(schema, tablename)
    table_yml = index[tn_index]
    schema = table_yml["schema"]
    table = table_yml["table"]
    index = Symbol.(table_yml["index"])
    fields = Symbol.(table_yml["fields"])
    groups = length(groups) > 0 ? Symbol.(groups) : nothing
    types = Dict{Symbol, DataType}()
    for (type, f) in table_yml["types"]
        f = Symbol.(f)
        if type == "Int32"
            t = Int32
        elseif type == "Float32"
            t = Float32
        elseif type == "Date"
            t = Dates.Date
        elseif type == "String"
            t = String
        else
            error("Type $type not supported in file $types_file.")
        end
        new_types = Dict([field => t for field ∈ fields if field ∈ f])
        if length(new_types) > 0
            push!(types, new_types...)
        end
    end

    WrdsTable(wrdsuser, schema, table, index, fields, types, groups, where)
end

function pgpass(wrdsuser::WrdsUser)
    pgpass = ""
    if Base.Sys.iswindows()
        if haskey(ENV, "PASSDATA")
            pgpass = "$(ENV["PASSDATA"])/postgresql/pgpass.conf" 
        end
    else
        if haskey(ENV, "HOME")
            pgpass = "$(ENV["HOME"])/.pgpass" 
        end
    end

    function user(entry)
        split(entry, ":")[4]
    end

    if pgpass != ""
        # Creates the entry if it does not exists
        entries = []
        if isfile(pgpass)
            f = open(pgpass)
            entries = readlines(f)
            close(f)
        end
        if wrdsuser.username ∉ user.(entries)
            secret_pass = Base.getpass("WRDS password for user $(wrdsuser.username)")
            print("\n")
            pass = read(secret_pass, String)
            Base.shred!(secret_pass)
            entry = "$WRDS_POSTGRES_HOST:$WRDS_POSTGRES_PORT:$WRDS_POSTGRES_DB:$(wrdsuser.username):$pass"
            if isfile(pgpass)
                # Change permission to write
                Base.chmod(pgpass, 0o600)
            end
            f = open(pgpass, "a")
            write(f, entry)
            close(f)
            Base.chmod(pgpass, 0o400)
        end
    end
end

function connect(wrdsuser::WrdsUser)
    pgpass(wrdsuser)
    print("Connect user $(wrdsuser.username) to WRDS... ")
    wrdsuser.conn = LibPQ.Connection("host=$WRDS_POSTGRES_HOST dbname=$WRDS_POSTGRES_DB user=$(wrdsuser.username) port=$WRDS_POSTGRES_PORT")
    println("OK")
    nothing
end

function file(table::WrdsTable)
    # "$(table.schema)_$(table.table).parquet"
    "$(table.schema)/$(table.table).parquet"
end

function correct_types!(data::DataFrame, table::WrdsTable)
    # Index columns should not be missing
    for i in [c for c ∈ table.index if c ∈ Symbol.(names(data))]
        !haskey(table.types, i) ? error("Type for field $i is not defined for table $(table.table), schema $(table.schema).") : nothing
        data[!, i] .= convert.(table.types[i], data[!, i])
    end
    # Other columns can be missing
    fields = [f for f ∈ Symbol.(names(data)) if f ∉ table.index]
    for f in Symbol.(fields)
        !haskey(table.types, f) ? error("Type for field $f is not defined for table $(table.table), schema $(table.schema).") : nothing
        try
            data[!, f] .= convert.(Union{table.types[f], Missing}, data[!, f])
        catch e
            @warn "Column $(f) could not be properly converted."
        end
    end
end

function check_index(data::DataFrame, table::WrdsTable)
    if length(findall(nonunique(data[!, table.index]))) > 0
        error("Index for table $(table.table), schema $(table.schema) is not unique.")
    end
end

function download_fields(table::WrdsTable, fields::Vector{Symbol})
    isnothing(table.wrdsuser.conn) || status(table.wrdsuser.conn) == "CONNECTION_BAD" ? connect(table.wrdsuser) : nothing
    fields_todownload = [c for c in fields if c ∉ table.index]
    println("Download fields $(join(String.(fields_todownload), ", ")) from table $(table.schema).$(table.table)")
    if isnothing(table.groups)
        where_clause = ""
        if table.where != ""
            where_clause = "WHERE $(table.where)"
        end
        query = "SELECT $(join(String.([table.index..., fields_todownload...]), ", ")) FROM $(table.schema).$(table.table) $(where_clause)"
        result = execute(table.wrdsuser.conn, query)
        data = DataFrame(result)
        # Correct types and check index uniqueness
        correct_types!(data, table)
        check_index(data, table)
        # Add to existing table if any
        if isfile(file(table))
            ds = Dataset(file(table))
            df = DataFrame(ds)
            leftjoin!(df, data, on=table.index)
            writefile(file(table), df)
        else
            # Create folder for file
            mkpath(dirname(file(table)))
            writefile(file(table), data)
        end
    else
        # Get the groups
        query = "SELECT DISTINCT $(join(String.(table.groups), ", ")) FROM $(table.schema).$(table.table)"
        res_groups = execute(table.wrdsuser.conn, query)
        groups = DataFrame(res_groups)
        correct_types!(groups, table)
        # Add data by group
        for g in ProgressBar(eachrow(groups))
            filter = join(["$n = $(g[n])" for n in names(g)], " AND ")
            query = """SELECT $(join(String.([table.index..., fields...]), ", "))
                FROM $(table.schema).$(table.table)
                WHERE $filter
                """
            result = execute(table.wrdsuser.conn, query)
            dfg = DataFrame(result)
            correct_types!(dfg, table)
            check_index(dfg, table)
            # Add to exiting file if exists
            folder_group = "$(file(table))/$(join(["$n=$(g[n])" for n in names(g)], "/"))"
            if isdir(folder_group)
                ds = Dataset(file(table))
                append!(ds, permno="1000.0")
                df = DataFrame(ds)
                leftjoin!(df, dfg, on=table.index)
                writefile("$folder_group/part.0.parquet", df)
            else
                mkpath(folder_group)
                writefile("$folder_group/part.0.parquet", dfg)
            end
        end
    end
    # Close the connection
    close(wrdsuser.conn)
    nothing
end

function get_fields(table::WrdsTable, fields::Vector{Symbol}; kwargs...)
    if isfile(file(table)) || isdir(file(table))
        ds = Dataset(file(table))
        fields_missing = [f for f in fields if f ∉ Symbol.(ds.name_index.names)]
        if length(fields_missing) > 0
            if length(table.index) == 0
                @warn "The table $(table.schema).$(table.table) has no index. Redownload all fields."
                download_fields(table, fields)
            else
                download_fields(table, fields_missing)
            end
        end
    else
        download_fields(table, fields)
    end

    if isnothing(table.groups)
        ds = Dataset(file(table))
        cols = [table.index..., [f for f in fields if f ∉ table.index]...]
        ds |> select(cols...) |> DataFrame
    else
        error("Opening partitioned dataset is not yet supported.")
        # ds = Dataset(file(table))
        # append!(ds, kwargs...)
        # cols = [table.index..., fields...]
        # ds |> select(cols...) |> DataFrame
    end
end

function convert(table::WrdsTable, filepath::String)
    if isfile(filepath)
        if filesize(filepath) > 1e7 # File bigger than 100 MB
            @warn "Trying to convert a big file in memory. Make sure enough memory is available."
        end
        # Read table
        filetype = splitext(filepath)[2]
        if filetype == ".sas7bdat"
            rs = readsas(filepath)
            data = DataFrame(rs)
        elseif filetype == ".csv"
            data = DataFrame(CSV.File(filepath))
            # data = DataFrame(CSV.File(filepath, types=Dict(:fiscalYearEnd => Date)))
        else
            @error "Only conversion of SAS (.sas7bdat) and CSV (.csv) files is currently supported. You can select this file type when exporting from the WRDS web form."
        end
        # Lower case column names (WRDS default)
        rename!(data, Symbol.(lowercase.(names(data))))
        # Correct types and check index uniqueness
        correct_types!(data, table)
        check_index(data, table)
        # Create folder for file and save
        mkpath(dirname(file(table)))
        writefile(file(table), data)
    end
end

function convert(username::String, schema::String, tablename::String, tableindex::Vector{String}, filepath::String)
    user = WrdsUser(username)
    table = WrdsTable(user, schema, tablename, tableindex)
    convert(table, filepath)
end

end # module
