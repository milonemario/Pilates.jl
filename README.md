# Pilates.jl

[![Build Status](https://github.com/milonemario/Pilates.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/milonemario/Pilates.jl/actions/workflows/CI.yml?query=branch%3Amain)

**P**ilates.jl **I**s a **L**ibrary of **A**ccounting **T**ools for **E**conomist**S**.
Its purpose is to help data creation for Accounting, Finance, and Economic Research.

## Functionalities
- WRDS datasets
    - Compustat
        - [x] Fundamentals Annual (US)
        - [x] Fundamentals Quarterly (US)
    - CRSP
        - [x] Compounded returns from daily stock prices
        - [x] Volatility of returns from daily stock prices
- Fred
    - Any FRED series


## Quickstart

Starts with WRDS Compustat fundamental annual data.

```
julia> using Pilates

julia> wrdsuser = Pilates.WRDS.wrdsuser("username")

julia> data = Pilates.COmpustat.get_fields(wrdsuser, [:cik, :fyear, :at], frequency="Annual")
880344×9 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik     
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?  
────────┼────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing 
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing     

julia> Pilates.Compustat.add_fields!(data, wrdsuser, [:lt], frequency="Annual")
880344×10 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt          
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?    
────────┼─────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing     
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing     

```

Add compounded daily returns starting one year before until three months before the field datadate.

```
julia> Pilates.Crsp.add_permno!(wrdsuser, data, :gvkey)
880344×11 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt           permno  
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?     Int32?  
────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing      missing 
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮          ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing      missing 

julia> Pilates.Crsp.compounded_return!(wrdsuser, data, :datadate, :comp_ret, Year(-1), Month(-3))

julia> data
880344×12 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt           permno   comp_ret       
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?     Int32?   Float64?       
────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing      missing  missing        
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮          ⋮           ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing      missing  missing        

```

Add volatility daily returns starting one year before until three months before the field datadate.

```
julia> Pilates.Crsp.volatility_return!(wrdsuser, data, :datadate, :vol, Year(-1), Month(-3))

julia> data
880344×13 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt           permno   comp_ret  vol             
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?     Int32?   Missing   Float32?        
────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing      missing   missing  missing         
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮          ⋮        ⋮             ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing      missing   missing  missing         

```

Add Real Gross Domestic Product from FRED (series GDPC1)

```

julia> data
880344×14 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt           permno   comp_ret  vol              rgdp     
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?     Int32?   Missing   Float32?         Float64? 
────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing      missing   missing  missing           3440.92
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮          ⋮        ⋮             ⋮            ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing      missing   missing  missing          20182.5

```

Add Real Personal Consumption Expenditure and Real Government Consumption Expenditures and Gross Investment from FRED (series PCE and GCEC1)

```
julia> Pilates.Fred.add_series!(data, ["PCE" => :pce, "GCEC1" => :gce], :datadate)

julia> data
880344×16 DataFrame
    Row │ gvkey   datadate    indfmt  datafmt   consol  popsrc  fyear   at           cik      lt           permno   comp_ret  vol              rgdp      pce       gce      
        │ Int32   Date        String  String    String  String  Int32?  Float32?     Int32?   Float32?     Int32?   Missing   Float32?         Float64?  Float64?  Float64? 
────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
      1 │   1000  1961-12-31  INDL    STD       C       D         1961  missing      missing  missing      missing   missing  missing           3440.92     352.4   1176.74
   ⋮    │   ⋮         ⋮         ⋮        ⋮        ⋮       ⋮       ⋮          ⋮          ⋮          ⋮          ⋮        ⋮             ⋮            ⋮         ⋮         ⋮
 880344 │ 353945  2022-12-31  INDL    SUMM_STD  C       D         2022      981.551  1948862  missing      missing   missing  missing          20182.5    17736.5   3442.47

```
