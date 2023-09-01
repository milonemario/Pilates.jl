
@testset "WrdsUser" begin
    @test typeof(WrdsUser("testuser")) == WrdsUser
end

@testset "WrdsTable" begin
    wrdsuser = WrdsUser("testuser")
    @test typeof(WrdsTable(wrdsuser, "compustat", "funda")) == WrdsTable
    @test_throws Error WrdsTable(wrdsuser, "compustat", "unknown_table")
    @test_throws Error WrdsTable(wrdsuser, "unknown_vendor", "unknown_table")
end
