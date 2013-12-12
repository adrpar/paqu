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

?>