#!/usr/bin/php
<?php

require_once 'parallelQuery.php';
require_once 'mysqlii.php';

##############################################
############ SERVER CONFIGURATION ############
##############################################

$cmdlineToHeadConnectionString	= "mysql://root:spider@120.0.0.1:3306";
$nodesToHeadConnectionString	= "mysql://root:spider@120.0.0.1:3306";
$database 						= "spider_tmp_shard";
$user 							= "spider";
$password 						= "spider";

$addRowNumbersToFinalTable 		= false;

$engine 						= "MyISAM";
$federatedEngine 				= "federated";

##############################################

#command line arguments
if($argc != 4) {
	echo "MySQL Parallel Query:\n";
	echo "paqu query resultTable logFile\n";
	exit(1);
}

$shard_query = $argv[1];
$resTable = $argv[2];
$logFile = $argv[3];

#open the log file for output:
$fh = fopen($logFile, 'a') or die("Unable to open file.");

$paraQuery = new ParallelQuery();

#set the options
$paraQuery->setHeadConnectionString($cmdlineToHeadConnectionString);
$paraQuery->setConnectOnServerSite($nodesToHeadConnectionString);
$paraQuery->setDB($database);
$paraQuery->setSpiderUsr($user);
$paraQuery->setSpiderPwd($password);
$paraQuery->setAddRowNumbersToFinalTable($addRowNumbersToFinalTable);
$paraQuery->setEngine($engine);
$paraQuery->setFedEngine($federatedEngine);

try {
	$paraQuery->setCheckOnDB(false);
	$paraQuery->setHeadNodeTables(array("bar"));
	$paraQuery->setSQL($shard_query);
	$paraQuery->generateParallelQueryPlan($resTable);
	$paraQuery->printParallelQueryPlanOptimisationsToFile($fh);
	$paraQuery->printParallelQueryPlanToFile($fh);
	$paraQuery->translateQueryPlan();
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
