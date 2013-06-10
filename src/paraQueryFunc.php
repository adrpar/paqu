<?php

if(!defined('HAVE_PHP_PARAQUERY')) {
    require_once 'parallelQuery.php';
    require_once 'mysqlii.php';
    
    function paraQuery($shard_query, $resTable, $logFile) {
	#open the log file for output:
	$fh = fopen($logFile, 'a') or die("Unable to open file.");

	$paraQuery = new ParallelQuery();
	try {
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

	return 0;
    }
}
?>