module WRDS

#include("compustat/Compustat.jl")

# Handles the connection to WRDS and data donwloads

using LibPQ
using DataFrames
using Parquet2: Dataset, writefile, select
using YAML
using Dates

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
    vendor::String
    schema::String
    table::String
    index::Vector{Symbol}
    fields::Vector{Symbol}
    types::Dict{Symbol, DataType}
end

function WrdsTable(wrdsuser::WrdsUser, vendor::String, tablename::String)
    tables_yml = YAML.load_file("$(@__DIR__)/$vendor/files.yaml")
    !haskey(tables_yml, tablename) ? error("Table $tablename not found for vendor $vendor from WRDS.") :
    table_yml = tables_yml[tablename]
    schema = table_yml["schema"]
    table = table_yml["table"]
    index = Symbol.(table_yml["index"])
    fields = Symbol.(table_yml["fields"])

    types_file = "$(@__DIR__)/$vendor/types.yaml"
    types_yml = YAML.load_file(types_file)
    types = Dict{Symbol, DataType}()
    for (type, f) in types_yml
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

    WrdsTable(wrdsuser, vendor, schema, table, index, fields, types)
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
            print("WRDS password for user $(wrdsuser.username):")
            pass = readline()
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
    "$(table.schema)_$(table.table).parquet"
end

function download_fields(table::WrdsTable, fields::Vector{Symbol})
    isnothing(table.wrdsuser.conn) ? connect(table.wrdsuser) :
    println("Download fields $(join(String.(fields), ", ")) from table $(table.schema).$(table.table)")
    # query = "SELECT $(join([table.index..., fields...], ", ")) FROM $(table.schema).$(table.table) ORDER BY $(join(table.index, ", "))"
    query = "SELECT $(join(String.([table.index..., fields...]), ", ")) FROM $(table.schema).$(table.table)"
    result = execute(table.wrdsuser.conn, query)
    data = DataFrame(result)
    # Correct types
    # Index columns should not be missing
    for i in table.index
        !haskey(table.types, i) ? error("Type for field $i is not defined for table $(table.table), schema $(table.schema) and vendor $(table.vendor)") :
        data[!, i] .= convert.(table.types[i], data[!, i])
    end
    # Other columns can be missing
    for f in Symbol.(fields)
        !haskey(table.types, f) ? error("Type for field $f is not defined for table $(table.table), schema $(table.schema) and vendor $(table.vendor)") :
        data[!, f] .= convert.(Union{table.types[f], Missing}, data[!, f])
    end
    # Check validity of index
    if length(findall(nonunique(data[!, table.index]))) > 0
        error("Index for table $(table.table), schema $(table.schema) and vendor $(table.vendor) is non-unique.")
    end
    data
end

function get_fields(table::WrdsTable, fields::Vector{Symbol})
    if isfile(file(table))
        ds = Dataset(file(table))
        fields_missing = [f for f in fields if f ∉ Symbol.(ds.name_index.names)]
        if length(fields_missing) > 0
            data = DataFrame(ds)
            df = download_fields(table, fields_missing)
            leftjoin!(data, df, on=table.index)
            writefile(file(table), data)
        else
            cols = [table.index..., fields...]
            data = ds |> select(cols...) |> DataFrame
        end
    else
        data = download_fields(table, fields)
        writefile(file(table), data)
    end
    data
end

end # module
