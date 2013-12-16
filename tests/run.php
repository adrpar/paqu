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

?>



