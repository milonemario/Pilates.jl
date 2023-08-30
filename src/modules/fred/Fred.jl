module Fred

using Dates
using DataFrames
using FredData

function get_series(series::String)
    # Pull one series from FRED.
    fkey = open("$(@__DIR__)/fred_api_key.txt")
    key = readline(fkey)
    close(fkey)
    f = FredData.Fred(key)
    get_data(f, series)
end

function add_series!(data::DataFrame, series::Pair{String, Symbol}, coldate::Symbol)
    s = get_series(series.first)
    df = s.data
    userdates = unique(data[!, coldate])
    # Extract year, quarter, month
    dfu = DataFrame(date=userdates,
        year=year.(userdates),
        quarter = quarterofyear.(userdates),
        month = month.(userdates))
    df.year = year.(df.date)
    df.quarter = quarterofyear.(df.date)
    df.month = month.(df.date)

    if s.freq_short == "A"
        key = [:year]
    elseif s.freq_short == "Q"
        key = [:year, :quarter]
    elseif s.freq_short == "M"
        key = [:year, :month]
    elseif s.freq_short == "D"
        key = [:date]
    else
        error("Frequency $(series.freq) not supported.")
    end

    newcol = series.second
    leftjoin!(dfu, df[!, [key..., :value]], on=key)
    rename!(dfu, :value => newcol, :date => coldate)
    leftjoin!(data, dfu[!, [coldate, newcol]], on=coldate)
end

function add_series!(data::DataFrame, series::Vector{Pair{String, Symbol}}, coldate::Symbol)
    for s in series
        add_series!(data, s, coldate)
    end
end

end # module
