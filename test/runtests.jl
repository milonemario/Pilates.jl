using Pilates
using Test

@testset "Pilates.jl" begin

    @testset "WRDS tests" begin
        include("wrds_tests.jl")

        @testset "Compustat tests" begin
            include("compustat_tests.jl")
        end

        @testset "Crsp tests" begin
            include("crsp_tests.jl")
        end
    end

    @testset "FRED tests" begin
        include("fred_tests.jl")
    end

end
