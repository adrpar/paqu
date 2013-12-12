#!/usr/bin/php
<?php

require_once 'testing.php';

echo "Test 1\n";
echo runTest("select (x+y) from MDR1.FOF limit 10", "test1.ref");

?>