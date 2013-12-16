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



?>



