module Pilates

include("./modules/wrds/WRDS.jl")
using .WRDS: WrdsUser, WrdsTable
export WRDS

include("./modules/wrds/compustat/Compustat.jl")
export Compustat

include("./modules/wrds/crsp/Crsp.jl")
export Crsp

include("./modules/fred/Fred.jl")
export Fred


end
