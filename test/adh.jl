ldf = exampledata(:adh_location)
sdf = exampledata(:adh_share)
gdf = exampledata(:adh_shock)

@testset "bartik" begin
    b = bartik(ldf, sdf, gdf, :g, [:czone,:year], [:sic87dd,:year], :ind_share, prefix=:b_)
    @test b.b_g[1] ≈ b.z[1] atol=1e-7
    @test b.b_g[10] ≈ b.z[10] atol=1e-7
    @test ismissing(b[(b.czone.==27604).&(b.year.==1990),:b_g][1])
end

@testset "ADH" begin
    vars = (:y, :x, :z, :l_sh_routine33)
    df = ssagg(ldf, sdf, vars, [:czone,:year], [:sic87dd,:year], :ind_share,
        weightname=:wei, controls=term(:t2)+term(:Lsh_manuf))
    @test size(df) == (794, 7)
    # Compare results with Stata ssaggregate
    # Need to set type double in Stata
    @test df.s_n[1] ≈ 0.0059911775364 atol=1e-10
    @test df.s_n[end] ≈ 0.0266943675707 atol=1e-10
    @test df.y[1] ≈ 0.9038659881474 atol=1e-8
    @test df.x[1] ≈ -0.1129257186398 atol=1e-8
    @test df.z[end] ≈ -0.0903587219322 atol=1e-8
    @test df.l_sh_routine33[end] ≈ 0.8942995726825 atol=1e-7

    dfs1 = ssagg(ldf, sdf, vars, [:czone,:year], [:sic87dd,:year], :ind_share,
        weightname=:wei, controls=[term(:t2)+term(:Lsh_manuf), (fe(:czone),)])
    @test length(dfs1) == 2
    @test dfs1[1] == df
    df2 = dfs1[2]
    # Compare results with Stata ssaggregate
    @test df2.s_n[1] ≈ 0.0059911775364 atol=1e-10
    @test df2.y[1] ≈ 0.6102982773762 atol=1e-8
    @test df2.z[end] ≈ 0.8151737996387 atol=1e-8

    dfs2 = ssagg(ldf, sdf, vars, [:czone,:year], [:sic87dd,:year], :ind_share,
        weightname=:wei, controls=Dict(:c1=>term(:t2)+term(:Lsh_manuf), :c2=>(fe(:czone),)))
    @test dfs2[:c1] == df
    @test dfs2[:c2] == df2

    df0 = ssagg(ldf, sdf, vars, [:czone,:year], [:sic87dd,:year], :ind_share, weightname=:wei, snname=:sn)
    @test df0.sn[1] ≈ 0.0059911775364 atol=1e-10
    @test df0.y[1] ≈ 1.123561308606 atol=1e-7
    @test df0.z[end] ≈ 0.816621167919 atol=1e-8

    ndf = innerjoin(df, DataFrame(gdf), on=[:sic87dd,:year])
    r = reg(ndf, @formula(y~(x~g)+year), Vcov.robust(), weights=:s_n)
    @test r.coef[3] ≈ -0.29774702 atol=1e-6
    @test stderror(r)[3] ≈ 0.0931339 atol=1e-3
end
