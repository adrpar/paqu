#!/usr/bin/php
<?php

require_once 'parallelQuery.php';
require_once 'mysqlii.php';

#command line arguments
	if($argc != 4) {
		echo "MySQL Parallel Query:\n";
		echo "paraQuery query resultTable logFile\n";
		exit(1);
	}

	$shard_query = $argv[1];
	$resTable = $argv[2];
	$logFile = $argv[3];

#open the log file for output:
	$fh = fopen($logFile, 'a') or die("Unable to open file.");

	$paraQuery = new ParallelQuery();
	try {
		$paraQuery->setCheckOnDB(false);
		$paraQuery->setHeadNodeTables(array("bar"));
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan($resTable);
		$paraQuery->printParallelQueryPlanOptimisationsToFile($fh);
		$paraQuery->printParallelQueryPlanToFile($fh);
		$paraQuery->translateQueryPlan();
		var_dump($paraQuery->getParallelQueryPlan()); 
		var_dump($paraQuery->getActualQueries()); die(0);
		$time = $paraQuery->executeQuery();
	} catch (Exception $err) {
		echo "An Error occured:\n\n";
		echo $err->getFile() . "(" . $err->getLine() . ") " . $err->getMessage() . "\n";
		exit(1);
	}
	echo "Query time: " . (string)$time . "\n";
	$paraQuery->printActualQueriesToFile($fh);
	fclose($fh);
?>
