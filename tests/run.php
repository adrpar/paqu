#!/usr/bin/php
<?php

require_once 'testing.php';

echo "Test 1\n";
echo runTest("select (x+y) from MDR1.FOF limit 10", "test1.ref");

echo "Test 2\n";
echo runTest("select rand() from MDR1.Particles62 limit 100", "test2.ref");

echo "Test 3\n";
echo runTest("select rand(3) from MDR1.Particles62 limit 100", "test3.ref");

echo "Test 4\n";
echo runTest("select rand(3) as `random` from MDR1.Particles62 limit 100", "test4.ref");

echo "Test 5\n";
echo runTest("select particleId,x,y,z,vx,vy,vz from MDR1.Particles62 where rand() <= 0.0006 limit 100", "test5.ref");

echo "Test 6\n";
echo runTest("select particleId,x,y,z,vx,vy,vz from MDR1.Particles62 where rand(3) <= 0.0006 limit 100", "test6.ref");

echo "Test 7\n";
echo runTest("SELECT prog.* FROM MDR1.FOFMtree prog, MDR1.FOFMtree descend WHERE descend.fofTreeId = 85000000000 AND prog.fofTreeId BETWEEN descend.fofTreeId AND descend.lastProgId ORDER BY prog.fofTreeId ASC", "test7.ref");

echo "Test 8\n";
echo runTest("SELECT `prog`.fofTreeId as `prog__fofTreeId`,`prog`.fofId as `prog__fofId`,`prog`.treeSnapnum as `prog__treeSnapnum`,`prog`.descendantId as `prog__descendantId`,`prog`.lastProgId as `prog__lastProgId`,`prog`.mainLeafId as `prog__mainLeafId`,`prog`.treeRootId as `prog__treeRootId`,`prog`.x as `prog__x`,`prog`.y as `prog__y`,`prog`.z as `prog__z`,`prog`.vx as `prog__vx`,`prog`.vy as `prog__vy`,`prog`.vz as `prog__vz`,`prog`.np as `prog__np`,`prog`.mass as `prog__mass`,`prog`.size as `prog__size`,`prog`.spin as `prog__spin`,`prog`.ix as `prog__ix`,`prog`.iy as `prog__iy`,`prog`.iz as `prog__iz`,`prog`.phkey as `prog__phkey` FROM MDR1.FOFMtree prog , MDR1.FOFMtree descend WHERE descend.fofTreeId = 85000000000 AND prog.fofTreeId BETWEEN descend.fofTreeId AND descend.lastProgId ORDER BY `prog__fofTreeId` ASC", "test8.ref");

echo "Test 9\n";
echo runTest("SELECT * FROM MDR1.FOF WHERE snapnum=85 ORDER BY mass desc LIMIT 10", "test9.ref");

echo "Test 10\n";
echo runTest("SELECT 0.25*(0.5+FLOOR(LOG10(Mvir)/0.25)) AS log_mass, COUNT(*) AS num FROM MDR1.BDMV WHERE snapnum=85 GROUP BY FLOOR(LOG10(Mvir)/0.25) ORDER BY FLOOR(LOG10(Mvir)/0.25)", "test10.ref");

echo "Test 11\n";
echo runTest("SELECT Bolshoi.BDMVProf.bdmProfileId as `Bolshoi__BDMVProf__bdmProfileId`,Bolshoi.BDMVProf.bdmId as `Bolshoi__BDMVProf__bdmId`,Bolshoi.BDMVProf.snapnum as `Bolshoi__BDMVProf__snapnum`,Bolshoi.BDMVProf.NinCat as `Bolshoi__BDMVProf__NinCat`,Bolshoi.BDMVProf.R_Rvir as `Bolshoi__BDMVProf__R_Rvir`,Bolshoi.BDMVProf.Rbin as `Bolshoi__BDMVProf__Rbin`,Bolshoi.BDMVProf.np as `Bolshoi__BDMVProf__np`,Bolshoi.BDMVProf.mass as `Bolshoi__BDMVProf__mass`,Bolshoi.BDMVProf.dens as `Bolshoi__BDMVProf__dens`,Bolshoi.BDMVProf.Vcirc as `Bolshoi__BDMVProf__Vcirc`,Bolshoi.BDMVProf.VpropRms as `Bolshoi__BDMVProf__VpropRms`,Bolshoi.BDMVProf.Vrad as `Bolshoi__BDMVProf__Vrad`,Bolshoi.BDMVProf.VradRms as `Bolshoi__BDMVProf__VradRms`,Bolshoi.BDMVProf.boundR_Rvir as `Bolshoi__BDMVProf__boundR_Rvir`,Bolshoi.BDMVProf.boundNp as `Bolshoi__BDMVProf__boundNp`,Bolshoi.BDMVProf.boundMass as `Bolshoi__BDMVProf__boundMass`,Bolshoi.BDMVProf.boundDens as `Bolshoi__BDMVProf__boundDens`,Bolshoi.BDMVProf.boundVcirc as `Bolshoi__BDMVProf__boundVcirc`,Bolshoi.BDMVProf.boundVcircRms as `Bolshoi__BDMVProf__boundVcircRms`,Bolshoi.BDMVProf.boundVrad as `Bolshoi__BDMVProf__boundVrad`,Bolshoi.BDMVProf.boundVradRms as `Bolshoi__BDMVProf__boundVradRms` FROM Bolshoi.BDMVProf WHERE bdmId = (SELECT bdmId FROM Bolshoi.BDMV WHERE snapnum=416 ORDER BY Mvir DESC LIMIT 1) ORDER BY `Bolshoi__BDMVProf__Rbin` ASC", "test11.ref");

echo "Test 12\n";
echo runTest("SELECT f.x,f.y,f.z, b.x,b.y,b.z FROM MDR1.FOF AS f, MDR1.BDMV AS b WHERE POWER(b.x-f.x,2) < 1000 limit 1", "test12.ref");

echo "Test 13\n";
echo runTest("SELECT p.particleId,p.x,p.y,p.z FROM (SELECT x AS haloX, y AS haloY, z AS haloZ, Rvir AS hR FROM MDR1.BDMV WHERE bdmId = 6200000001) AS h, MDR1.Particles62 AS p WHERE POWER(h.haloX-p.x,2) + POWER(h.haloY-p.y,2) + POWER(h.haloZ-p.z,2) <= h.hR*h.hR", "test13.ref");

echo "Test 14\n";
echo runTest("SELECT PROG.mass, PROG.treeSnapnum, DES.mass, DES.treeSnapnum, DES.fofId, PROG.fofId, DES.lastProgId FROM MDR1.FOFMtree AS PROG, MDR1.FOFMtree AS DES WHERE DES.treeSnapnum=39 AND PROG.fofTreeId BETWEEN DES.fofTreeId AND DES.lastProgId AND PROG.mass > 1.e13 ORDER BY DES.fofId", "test14.ref");

echo "Test 15\n";
echo runTest("SELECT i from barbar WHERE i=1;", "test15.ref");

echo "Test 16\n";
echo runTest("SELECT i from bar WHERE i=1;", "test16.ref");

echo "Test 17\n";
echo runTest("SELECT i from `TEST`.`bar` WHERE i>1;", "test17.ref");

echo "Test 18\n";
echo runTest("SELECT i from `bar`.`barbar` WHERE i>1;", "test18.ref");

echo "Test 19\n";
echo runTest("SELECT `D.treeRootId`, `D.x`,`D.y`,`D.z`,`D.mass`, `P.x`,`P.y`,`P.z`, `P.mass` FROM `bar`.`bar` WHERE `P.x` BETWEEN 515 and 516;", "test19.ref");

echo "Test 20\n";
echo runTest("SELECT x,y,z FROM MDR1.Particles85 WHERE RAND(154321) <= 2.91E-5;", "test20.ref");

echo "Test 21\n";
echo runTest("SELECT sprng_make_seed(), count(abs(*));", "test21.ref");

echo "Test 22\n";
echo runTest("SELECT t1.a, count(t1.a) from t1, t2, t3 where t1.id = t2.t1_id and t3.id = t2.date_id group by t1.a;", "test22.ref");

echo "Test 23\n";
echo runTest("SELECT h.niceCol FROM (SELECT b.niceCol FROM niceTbl as b) as h", "test23.ref");

echo "Test 24\n";
echo runTest("SELECT f3.fofId, p.x,p.y,p.z FROM MDR1.FOFParticles f, MDR1.FOFParticles3 f3, MDR1.particles85 p WHERE f.fofId = 85000000479 AND f.particleId = f3.particleId AND p.particleId = f.particleId ORDER BY f3.fofId ASC", "test24.ref");

echo "Test 25\n";
echo runTest("SELECT x FROM table WHERE ( x = 0.998373 ) or ( ( y = SIN (0.998373) ) and ( z = 0.998373 ) ) and ( z = 43 ) or ( ( ( z = 23 ) and ( z = 4 ) ) or ( x = 1 ) ) or ( y = 34 ) and ( x between 1 and 2 ) or ( z = 1 + 5 * 87.2134 )", "test25.ref");

echo "Test 26\n";
echo runTest("SELECT x FROM table WHERE x=0.998373 or (y=sin(0.998373) and z=0.998373) and z=43 or ((z=23 and z=4) or x=1) or y=34 and x between 1 and 2", "test26.ref");

echo "Test 27\n";
echo runTest("SELECT `h`.`ca`+`h`.`ab` as `total` FROM (SELECT count(a) as `ca`, avg(b) as `ab` FROM tblA) as `h`", "test27.ref");

echo "Test 28\n";
echo runTest("SELECT 2.0*`p`.`total` as `totalTwo` FROM (SELECT SUM(`h`.`ca`+`h`.`ab`) as `total` FROM (SELECT count(a) as `ca`, avg(b) as `ab` FROM tblA) as `h`) as `p`", "test28.ref");

echo "Test 29\n";
echo runTest("SELECT prog.fofTreeId FROM MDR1.FOFMtree prog, MDR1.FOFMtree descend WHERE descend.fofTreeId = 85000000000 AND prog.fofTreeId BETWEEN descend.fofTreeId AND descend.lastProgId ORDER BY 1 ASC", "test29.ref");

echo "Test 30\n";
echo runTest("SELECT * FROM MDR1.FOF WHERE snapnum=85 ORDER BY MDR1.FOF.mass desc LIMIT 10", "test30.ref");

echo "Test 31\n";
echo runTest("SELECT h.snapnum FROM (SELECT snapnum, expz FROM MDR1.FOF WHERE snapnum=85 LIMIT 10) as h ORDER BY h.expz ASC", "test31.ref");

echo "Test 32\n";
echo runTest("SELECT snapnum as `snappi` FROM MDR1.FOF WHERE snapnum=85 ORDER BY `snappi` desc LIMIT 10", "test32.ref");

//AT SOME POINT THIS WILL BE IMPLEMENTED:
/*echo "Test 33\n";
echo runTest("SELECT t1.name, t2.salary FROM employee t1 INNER JOIN info t2 ON t1.name = t2.name", "test33.ref");*/

//Test34
echo "Test 34\n";
echo runTest("SELECT b.x,b.y,b.z,b.phkey FROM MDPL.BDMW b WHERE b.snapnum=88 ORDER BY b.Mvir DESC LIMIT 10", "test34.ref");

//Test35
echo "Test 35\n";
echo runTest("SELECT 2.0 + b.bdmId, b.snapnum / r.zred, r.zred, b.x,b.y,b.z,b.phkey, hilbertKey(10,1000.0,3,b.x,b.y,b.z) as xyzphkey FROM MDPL.BDMW b, MDPL.Redshifts r WHERE b.snapnum=88 AND b.snapnum = r.snapnum AND b.Mvir > 1.e14 LIMIT 10", "test35.ref");

//Test36
echo "Test 36\n";
echo runTest("SELECT 2.0 + b.bdmId, b.snapnum / r.zred, r.zred, b.x,b.y,b.z,b.phkey, hilbertKey(10,1000.0,3,b.x,b.y,b.z) as xyzphkey FROM MDPL.BDMW b, MDPL.Redshifts r WHERE b.snapnum=88 AND b.snapnum = r.snapnum AND b.Mvir > 1.e14 ORDER BY 8 LIMIT 10", "test36.ref");

//Test37
echo "Test 37\n";
echo runTest("select host.*, sub.*, `host`.bdmId  AS `host__bdmId`, `host`.Mvir  AS `host__Mvir`, `sub`.snapnum  AS `sub__snapnum`, `sub`.hostFlag  AS `sub__hostFlag`, `sub`.Mvir  AS `sub__Mvir` from MDPL.BDMW host, MDPL.BDMW sub where host.bdmId = sub.hostFlag and host.Mvir < sub.Mvir and host.snapnum=88 and sub.snapnum=88", "test37.ref");

//Test38
echo "Test 38\n";
echo runTest("SELECT gas.ahfId, gas.Mvir, dm.ahfId, dm.Mvir, m.ahfId_Gas, m.ahfId_DM FROM Clues3_LGGas.AHFMatch as m, Clues3_LGGas.AHF as gas, Clues3_LGDM.AHF as dm WHERE m.ahfId_Gas = gas.ahfId AND m.ahfId_DM = dm.ahfId ORDER BY gas.Mvir DESC LIMIT 10", "test38.ref");

//Test39
echo "Test 39\n";
echo runTest("select `m`.`RAVE_OBS_ID` as `m__RAVE_OBS_ID`, `n`.`Fieldname` as `n__Fieldname` from `RAVEPUB_DR3`.`chemical_pipe_VDR3` as `m`, `RAVEPUB_DR3`.`MAP_RAVEID` as `n` limit 10", "test39.ref");

//Test40
echo "Test 40\n";
echo runTest("select a, count(*) from t1, t2, t3 where t1.id = t2.t1_id and t3.id = t2.date_id group by a", "test40.ref");

//Test41
echo "Test 41\n";
echo runTest("select a, count(*) from t1, t2, bar where t1.id = t2.t1_id and bar.id = t2.date_id group by a", "test41.ref");

//Test42
echo "Test 42\n";
echo runTest("select a, count(*) from t1, bar, t3 where t1.id=bar.t1_id and t3.id=bar.date_id group by a", "test42.ref");


?>
