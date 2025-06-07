using Pilates

@testset "WrdsUser" begin
    @test typeof(WRDS.WrdsUser("testuser")) == WRDS.WrdsUser
end
