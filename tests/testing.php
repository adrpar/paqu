<?php

require_once '../src/parallelQuery.php';
require_once '../src/mysqlii.php';

function runTest($query, $refFile) {
	$paraQuery = runParaQuery($query);

	$plan = $paraQuery->getParallelQueryPlan();
	stripAggregate($plan);
	removeSpace($plan);

	$refPlan = loadReference($refFile);
	stripAggregate($refPlan);
	removeSpace($refPlan);

	if(comparePlans($plan, $refPlan) === true) {
		return "PASSED\n";
	} else {
		return "FAILED\n";
	}
}

function runParaQuery($query) {
	$resTable = "TEST.TEST";

	$paraQuery = new ParallelQuery();

	try {
		$paraQuery->setCheckOnDB(false);
		$paraQuery->setHeadNodeTables(array("bar"));
		$paraQuery->setSQL($query);
		$paraQuery->generateParallelQueryPlan($resTable);
		$paraQuery->translateQueryPlan();
	} catch (Exception $err) {
		echo "An Error occured:\n\n";
		echo $err->getFile() . "(" . $err->getLine() . ") " . $err->getMessage() . "\n";
		exit(1);
	}

	return $paraQuery;
}

function stripAggregate(&$plan) {
	foreach($plan as &$line) {
		$line = preg_replace("/aggregation_tmp_[0-9]+/", "", $line);
	}
}

function removeSpace(&$plan) {
	foreach($plan as &$line) {
		$line = trim($line);
		$line = str_replace(" ", "", $line);
		$line = str_replace("\n", "", $line);
		$line = str_replace("\t", "", $line);
		$line = str_replace("\r", "", $line);
	}
}

function loadReference($file) {
	return file($file);
}

function comparePlans($plan, $refPlan) {
	if(count($plan) != count($refPlan)) {
		return false;
	}

	foreach($plan as $key => $line) {
		if($line !== $refPlan[$key]) {
			return false;
		}
	}

	return true;
}