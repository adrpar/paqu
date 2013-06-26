<?php

/*
  Copyright (c) 2012, Adrian Partl @ Leibniz Inst. for Astrophysics Potsdam
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright
  notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
 * Neither the name of the <organization> nor the
  names of its contributors may be used to endorse or promote products
  derived from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

error_reporting(E_ALL);

include_once 'shard-query-parallel.php';

/**
 * @file queryPlanWriter.php
 * @brief Functions writing the final query plan and driver for shard-query-parallel
 *
 * The functions in this file take the optimised query plan produced by shard-query-parallel
 * and rewrite it into a query plan using the MySQL spider shard UDF and our custom defined
 * parallelisation commands.
 * 
 * These functions also wrap the shard-query-parallel tools and drive the whole parallel
 * optimisation process.
 */


/**
 * @brief Generates the parallel query plan from a given SQL query
 * @param query SQL query that will be parallelised
 * @return the raw sharded queries to be run on the coordination and shard nodes
 * 
 * These function will create a parallel query plan from an input SQL statement
 * using shard-query-parallel. It will identify any implicit joins, properly separate
 * them into distinct queries and assign them to temporary tables. Further any aggregate
 * function is properly transfered into sharded versions. This function will eventually
 * produce a query plan that can be further processed by the query plan to SQL parser.
 */
function PHPSQLprepareQuery($query) {
    $shard_query = new ShardQuery();
    $shard_query->verbose = false;
    
    $shard_query->process_sql($query);

    if(!empty($shard_query->errors)) {#
    	echo "An Error occured:\n\n";
    	foreach($shard_query->errors as $key => $error) {
    	    echo $key . ": " . $error . "\n";
  	  }
	
      throw new Exception('SQL Parse Error');
    }

    #determine if this is a serial or parallel query:
    #parallel only if there are other tables than temporary (i.e. dependent) ones involved
    $parallel = false;
    foreach($shard_query->parsedCopy['FROM'] as $fromNode) {
    	if($fromNode['table'] != 'DEPENDENT-SUBQUERY') {
    	    $parallel = true;
    	}
    }

    $shard_query->subqueries[$shard_query->table_name] = $shard_query->shard_sql;
    $shard_query->subqueries[$shard_query->table_name]['parallel'] = $parallel;

    return $shard_query;
}

/**
 * @brief Generates a set of SQL statements of the parallelised query
 * @param shard_query the sharded queries produced by the parallel query optimiser
 * @param resultTable the name of the result table to write the results into
 * @param addRowNumber add row numbers to the final result table
 * @param aggregateDist flag that defines whether aggregate results should be treated as distinct
 * @return array of SQL statements to be run on the head node
 * 
 * Deprecated, kept for further reference. This function takes a query plan and produces
 * the SQL needed to run the plan in parallel. This uses the spider engine UDFs and a combination
 * of federated tables to achieve the parallelism. However the new function using the parallel
 * query functions should be used!
 */
function PHPSQLqueryPlanWriterFullSQL($shard_query, $resultTable, $addRowNumber = false, $aggregateDist = true) {
    $commandArray = array();
    $dropTables = array();
    
    foreach($shard_query->subqueries as $key => $query) {
    	if($query['parallel'] === true) {
  	    #remove any LIMIT clause, that might interfere
  	    #if this limit is part of a subquery, there must be a closing parenthesis at pos > limit_pos
  	    $tmpPos = strrpos($query[0], "LIMIT");
  	    $tmpParPos = strrpos($query[0], ")");
  	    if($tmpPos === false || $tmpPos < $tmpParPos) {
  		  $tmpPos = strlen($query[0]);
  	    }
  	    $limitFreeQuery = substr($query[0], 0, $tmpPos);

  	    $hostTableCreateQuery = "CREATE DATABASE IF NOT EXISTS spider_tmp_shard; USE spider_tmp_shard; CREATE TABLE spider_tmp_shard." . $key . " ENGINE=MyISAM " . $limitFreeQuery . " LIMIT 0";
  	    $shardCreateFedTable = "SELECT spider_bg_direct_sql('CREATE DATABASE IF NOT EXISTS spider_tmp_shard; CREATE TABLE spider_tmp_shard." . $key . " ENGINE=federated CONNECTION=\"mysql://root:spider@alma015.lin.ib:3306/spider_tmp_shard/" . $key . "\" " . $limitFreeQuery . " LIMIT 0;', '', concat('host \"', host ,'\", port \"', port ,'\", user \"msandbox\", password \"msandbox\"')) from mysql.spider_tables where table_name like 'BDMV#%'";
  	    $shardActualQuery = "SELECT spider_bg_direct_sql('USE spider_tmp_shard; INSERT INTO spider_tmp_shard." . $key . " ". $query[0] . "', '', concat('host \"', host ,'\", port \"', port ,'\", user \"msandbox\", password \"msandbox\"')) from mysql.spider_tables where table_name like 'BDMV#%'";

  	    $dropTableShard = "SELECT spider_bg_direct_sql('DROP TABLE spider_tmp_shard." . $key . "', '', concat('host \"', host ,'\", port \"', port ,'\", user \"msandbox\", password \"msandbox\"')) from mysql.spider_tables where table_name like 'BDMV#%'";
  	    $dropTableHead = "DROP TABLE spider_tmp_shard." . $key ;

  	    array_push($commandArray, $hostTableCreateQuery);
  	    array_push($commandArray, $shardCreateFedTable);
  	    array_push($commandArray, $shardActualQuery);
  	    array_push($dropTables, $dropTableShard);
  	    array_push($dropTables, $dropTableHead);
    	} else {
  	    $hostTableCreateQuery = "CREATE DATABASE IF NOT EXISTS spider_tmp_shard; USE spider_tmp_shard; CREATE TABLE spider_tmp_shard." . $key . " ENGINE=MyISAM " . $query[0] . " LIMIT 0";
  	    $shardActualQuery = "USE spider_tmp_shard; INSERT INTO spider_tmp_shard." . $key . " ". $query[0] ;
  	    $dropTableHead = "DROP TABLE spider_tmp_shard." . $key ;

      	array_push($commandArray, $hostTableCreateQuery);
  	    array_push($commandArray, $shardActualQuery);
  	    array_push($dropTables, $dropTableHead);
    	}
    }

    #remove the last drop table since we want to keep the results for further use
    $tmpQuery = array_pop($dropTables);
    
    #gather the result into table
    array_push($dropTables, PHPSQLaggregateResult($shard_query, $resultTable, $addRowNumber, $aggregateDist));
    array_push($dropTables, $tmpQuery);
    
    return array_merge($commandArray, $dropTables);
}

/**
 * @brief Generates a set of statements using the parallel query function to 
 * @param shard_query the sharded queries produced by the parallel query optimiser
 * @param resultTable the name of the result table to write the results into
 * @param addRowNumber add row numbers to the final result table
 * @param aggregateDist flag that defines whether aggregate results should be treated as distinct
 * @return parallel query plan using the parallel query functions
 * 
 * This function takes a query plan and produces a query plan using our parallel query functions. These
 * need to be unfortunately processed by the parallelQuery class, since MySQL stored procedures are not
 * doing a great job with dynamic SQL. However this gives the opportunity to actually intersect the query
 * plan and tweak/correct it if needed.
 */
function PHPSQLqueryPlanWriter($shard_query, $resultTable, $addRowNumber = false, $aggregateDist = true) {
    $commandArray = array();
    $dropTables = array();
    
    foreach($shard_query->subqueries as $key => $query) {
    	if($query['parallel'] === true) {
  	    $paraQuery = "CALL paquExec(\"" . str_replace("\n", " ", $query[0]) . "\", \"" . $key . "\")";
  	    
  	    $dropTableHead = "CALL paquDropTmp(\"" . $key . "\")";

  	    array_push($commandArray, $paraQuery);
  	    array_push($dropTables, $dropTableHead);
    	} else {
  	    $hostTableCreateQuery = "CREATE DATABASE IF NOT EXISTS spider_tmp_shard; USE spider_tmp_shard; CREATE TABLE spider_tmp_shard." . $key . " ENGINE=MyISAM " . $query[0] . " LIMIT 0";
  	    $shardActualQuery = "USE spider_tmp_shard; INSERT INTO spider_tmp_shard." . $key . " ". $query[0] ;
  	    $dropTableHead = "DROP TABLE spider_tmp_shard." . $key ;

      	array_push($commandArray, $hostTableCreateQuery);
  	    array_push($commandArray, $shardActualQuery);
  	    array_push($dropTables, $dropTableHead);
    	}
    }

    #remove the last drop table since we want to keep the results for further use
    $tmpQuery = array_pop($dropTables);
    
    #gather the result into table
    array_push($dropTables, PHPSQLaggregateResult($shard_query, $resultTable, $addRowNumber, $aggregateDist));
    array_push($dropTables, $tmpQuery);
    
    return array_merge($commandArray, $dropTables);
}

/**
 * @brief add distinct clause after SELECT in corrdination queries if the user asks for it
 * @param shard_query the sharded queries produced by the parallel query optimiser
 * @param resultTable the name of the result table to write the results into
 * @param addRowNumber add row numbers to the final result table
 * @param aggregateDist flag that defines whether aggregate results should be treated as distinct
 * @return Coordination/Aggregation SELECT statement with DISTINCT clause added
 * 
 * 
 */
function PHPSQLaggregateResult($shard_query, $resultTable, $addRowNumber = false, $aggregateDist = true) {
    #add distinct clause after SELECT in corrdination query if user asks for it
    if($addRowNumber === true) {
      $shard_query->coord_sql = str_replace("SELECT", "SELECT @i:=@i+1 AS `row_id`, ", $shard_query->coord_sql);
    }

    if($aggregateDist == true) {
    	if(strpos($shard_query->coord_sql, "DISTINCT") === false) {
  	    $shard_query->coord_sql = str_replace("SELECT", "SELECT DISTINCT", $shard_query->coord_sql);
    	}
    }
    
    if($addRowNumber === false) {
      $hostResultCreate = "USE spider_tmp_shard; CREATE TABLE " . $resultTable . " ENGINE=MyISAM " . $shard_query->coord_sql;
    } else {
      $hostResultCreate = "USE spider_tmp_shard; SET @i=0; CREATE TABLE " . $resultTable . " ENGINE=MyISAM " . $shard_query->coord_sql;
    }

    return $hostResultCreate;
}

?>
