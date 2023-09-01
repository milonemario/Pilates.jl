using DataFrames
using Dates
using Parquet2: writefile

function create_mock_tables()
    df_funda = DataFrame(
        gvkey = Int32[1, 1, 1, 2, 2, 2, 3, 3, 3],
        datadate = Date.([
            "2000-01-03", "2001-01-03", "2002-01-03",
            "2003-01-03", "2004-01-03", "2005-01-03",
            "2010-01-03", "2011-01-03", "2012-01-03"]),
        indfmt = String[
            "INDL", "INDL", "INDL",
            "INDL", "INDL", "INDL",
            "INDL", "INDL", "INDL"],
        datafmt = String[
            "STD", "STD", "STD",
            "SUMM_STD", "SUMM_STD", "SUMM_STD",
            "STD", "STD", "STD"],
        consol = String[
            "C", "C", "C",
            "C", "C", "C",
            "C", "C", "C"],
        popsrc = String[
            "D", "D", "D",
            "D", "D", "D",
            "D", "D", "D"],
        fyear = Int32[2000, 2001, 2002, 2003, 2004, 2005, 2010, 2011, 2012],
        at = Int32[1000, 1000, 1000, 2000, 2000, 2000, 3000, 3000, 3000]
    )

    writefile("comp_na_daily_all_funda.parquet", df_funda)

    df_fundq = DataFrame(
        gvkey = Int32[4, 4, 4, 5, 5, 5, 6, 6, 6],
        datadate = Date.([
            "2000-01-03", "2000-04-03", "2000-07-03",
            "2003-02-03", "2003-05-03", "2003-08-03",
            "2010-03-03", "2010-06-03", "2010-09-03"]),
        indfmt = String[
            "INDL", "INDL", "INDL",
            "INDL", "INDL", "INDL",
            "INDL", "INDL", "INDL"],
        datafmt = String[
            "STD", "STD", "STD",
            "SUMM_STD", "SUMM_STD", "SUMM_STD",
            "STD", "STD", "STD"],
        consol = String[
            "C", "C", "C",
            "C", "C", "C",
            "C", "C", "C"],
        popsrc = String[
            "D", "D", "D",
            "D", "D", "D",
            "D", "D", "D"],
        fyear = Int32[2000, 2000, 2000, 2003, 2003, 2003, 2010, 2010, 2010],
        at = Int32[4000, 4000, 4000, 5000, 5000, 5000, 6000, 6000, 6000]
    )

    writefile("comp_na_daily_all_fundq.parquet", df_fund1)

    df_names = DataFrame(
        gvkey = [1, 2, 3, 4, 5, 6],
        cik = [123, 456, 789, 234, 724, 364]
    )

    writefile("comp_na_daily_all_names.parquet", df_names)

    nothing
end

function clean_mock_tables()
    rm("comp_na_daily_all_funda.parquet")
    rm("comp_na_daily_all_fundq.parquet")
    rm("comp_na_daily_all_names.parquet")
end

@testset "Compustat Fields" begin
    create_mock_tables()

    wrdsuser = WrdsUser("testuser")

    @test_throws Error Compustat.get_fields(wrdsuser, [:fyear], "unknown_frequency")

    data = Compustat.get_fields(wrdsuser, [:fyear])
    @test typeof(data) == DataFrame
    nc = ncols(data)

    Compustat.add_fields!(wrdsuser, [:at])
    @test ncols(data) == nc + 1
    nc = ncols(data)

    Compustat.add_fields!(wrdsuser, [:cik])
    @test ncols(data) == nc + 1
    @test data.cik[1] == 123

    data = Compustat.get_fields(wrdsuser, [:fyear, :cik], frequency=="Quarterly")
    @test data.cik[1] == 234

    clean_mock_tables()
end
