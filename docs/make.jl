push!(LOAD_PATH, "../src/")

using Documenter
using Pilates

makedocs(
    sitename = "Pilates.jl",
    modules = [Pilates],
    pages = [
        "Home" => "index.md",
        "Modules" => [
            "WRDS" => [
                "Compustat" => "modules/wrds/compustat.md",
                "CRSP" => "modules/wrds/crsp.md"
            ],
            "FRED" => "modules/fred.md"
        ]
    ])

deploydocs(;
    repo = "github.com/milonemario/Pilates.jl"
)
