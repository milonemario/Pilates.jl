module Pilates

include("./modules/wrds/WRDS.jl")
using .WRDS: WrdsUser, WrdsTable
# export WrdsUser, WrdsTable

include("./modules/wrds/compustat/Compustat.jl")
include("./modules/wrds/crsp/Crsp.jl")


end
