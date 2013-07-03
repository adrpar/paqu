#!/usr/bin/php
<?php

require_once 'parallelQuery.php';
require_once 'mysqlii.php';

/*$shard_query = "select Mvir FROM MDR1.BDMV as b WHERE b.snapnum = 85 and (b.x between -200 and 1200) and (b.y between -200 and 1200) and (b.z between 450 and 550)";
$shard_query = "select Mvir FROM MDR1.BDMV WHERE snapnum = 85 and (x between -200 and 1200) and (y between -200 and 1200) and (z between 450 and 550)";
	$paraQuery = new ParallelQuery();
	$paraQuery->setSQL($shard_query);
	$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result101');
	$paraQuery->printParallelQueryPlanOptimisations();
	$paraQuery->printParallelQueryPlan();
	$paraQuery->translateQueryPlan();
	$time = $paraQuery->executeQuery();
	echo "Query time: " . (string)$time . "\n";*/

#$shard_query = "select halos.bdmid, halos.Mvir from P2.BDMV halos where halos.Mvir between pow(10, 15) and pow(10, 15.1);";
/*$shard_query = "select h.fofId, b.fofTreeId, b.fofId, b.x, b.y, b.z from MDR1.FOF as h, MDR1.FOFMtree as b where (h.x - b.x) between -0.1 and 0.1 and (h.y - b.y) between -0.1 and 0.1 and (h.z - b.z) between -0.1 and 0.1 and b.treeSnapnum = 39 and h.snapnum=85;";
	$paraQuery = new ParallelQuery();
	$paraQuery->setSQL($shard_query);
	$paraQuery->generateParallelQueryPlan('spider_tmp_shard.correlate2');
	$paraQuery->printParallelQueryPlanOptimisations();
	$paraQuery->printParallelQueryPlan();
	$paraQuery->translateQueryPlan();
	$paraQuery->printActualQueries();
	$time = $paraQuery->executeQuery();
	echo "Query time: " . (string)$time . "\n";*/

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


    //tests();


	function tests() {
		$shard_query = "select 1.0*(0.5+floor(log10(h.Mvir)/1.0)), avg(h.Rvir), stddev(h.Rvir) from MDR1.BDMV as `h` where h.snapnum=85 group by floor(log10(h.Mvir)/1.0);";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";
		
		$shard_query = "select power(10.000000,(0.05*(0.5+floor(log10(p.R_Rvir)/0.05)))), avg(p.Vrad /( sqrt(6.67428e-8 * halos.Mvir * 1.988e33 /(halos.Rvir * 3.0856e24)) / 100000)) from (select Mvir, Rvir, bdmId from MDR1.BDMV where snapnum=85 and Mvir between 1e12 and 1.1e12) as halos, MDR1.BDMVprof p where p.bdmId = halos.bdmId group by floor(log10(p.R_Rvir)/0.05) order by 1;";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result2');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$paraQuery->printActualQueries();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";
		
		$shard_query = "select p.particleId, p.snapnum, p.x, p.y, p.z, p.vx, p.vy, p.vz, p.phkey from (select fP.particleId from MDR1.FOFParticles as fP, (select fofId from MDR1.FOF where snapnum=85 order by mass desc limit 1) as mC where fP.fofId = mC.fofId and fP.snapnum=85) as hP, MDR1.particles85 p where p.particleId = hP.particleId";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result3');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select 0.1 * (0.5 + floor(log10(f.mass) / 0.1)) as log_mass, count(*) / 0.1 / (250*250*250) as num from MDR1.FOF f, (select snapnum from MDR1.redshifts where zred = 0.0) as redZ where f.snapnum = redZ.snapnum group by floor(log10(f.mass) / 0.1) order by log_mass";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result4');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select distinct f.snapnum, r.zred from MDR1.FOF f, MDR1.redshifts r where f.snapnum = r.snapnum group by f.snapnum";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result5');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select distinct f.snapnum, r.zred from MDR1.FOF f, (select * from MDR1.redshifts) r where f.snapnum = r.snapnum group by f.snapnum";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result5');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select halos.bdmid, halos.Mvir from MDR1.BDMV halos where halos.snapnum=85 order by halos.Mvir desc limit 100";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result6');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select bdmProfileId, Rbin, mass from MDR1.BDMVprof where bdmId = (select h.bdmId from MDR1.BDMV h where h.snapnum=85 order by Mvir desc limit 1)";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result7');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select 0.02*(0.5+floor(p.R_Rvir/0.02)), avg(p.dens/1e9) from (select bdmId, snapnum, Mvir from MDR1.BDMV where snapnum=85 and Mvir > 1e15) as halos, MDR1.BDMVprof as p where p.bdmId=halos.bdmId group by floor(p.R_Rvir/0.02)";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result8');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select mt.fofTreeId, mt.x, mt.y, mt.z from MDR1.FOFMtree mt, (select tree.fofId, tree.fofTreeId, tree.lastProgId from MDR1.FOFMtree tree, (select fofId from MDR1.FOF where snapnum=85 order by mass desc limit 2) as h where tree.fofId = h.fofId) as fL where mt.fofTreeId between fL.fofTreeId and fL.lastProgId";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result9');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select mt.* from MDR1.FOFMtree mt, (select tree.* from MDR1.FOFMtree tree, (select fofId from MDR1.FOF where snapnum=85 order by mass desc limit 2) as h where tree.fofId = h.fofId) as fL where mt.fofTreeId between fL.fofTreeId and fL.lastProgId";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result10');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";

		$shard_query = "select * from Bolshoi.particles416 where x=0.998373 or y=0.998373 or z=0.998373";
		$paraQuery = new ParallelQuery();
		$paraQuery->setSQL($shard_query);
		$paraQuery->generateParallelQueryPlan('spider_tmp_shard.result10');
		$paraQuery->printParallelQueryPlanOptimisations();
		$paraQuery->printParallelQueryPlan();
		$paraQuery->translateQueryPlan();
		$time = $paraQuery->executeQuery();
		echo "Query time: " . (string)$time . "\n";
	}
	
	?>
