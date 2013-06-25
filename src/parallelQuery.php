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

require_once 'queryPlanWriter.php';
require_once 'parseSqlAll.php';
require_once 'mysqlii.php';


/**
 * @class ParallelQuery
 * @brief Parallel Queries Using Spider MySQL Engine.
 *
 * This class wraps all the complicated procedures around the
 * parallel query optimiser and rewriter. This class needs a
 * connection to the database to validate things and resolve
 * any SQL * statements by including all the columns in the query.
 * 
 * In order to configure this class, edit the private properties
 * accordingly. These cannot be set dynamically at the moment, since
 * they usually won't change for a specific deployment.
 */

class ParallelQuery {
    private $defConnect = "mysql://root:spider@127.0.0.1:3306";	    //!< Connection string to the server USING spider
    private $evalQueryTimeout = 1000;				    //!< Timeout for evaluating SQL syntax of the query

    private $defDB = "spider_tmp_shard";			    //!< Temporary database where federated tables are written to
    private $defEngine = "MyISAM";				    //!< Default engine to use for federated temporary tables
    private $defConnectOnServerSite = "mysql://root:spider@120.0.0.1:3306";    //!< Connection string of the spider head node
    private $defSpiderUsr = "msandbox";				    //!< User on the spider nodes side
    private $defSpiderPwd = "msandbox";				    //!< Password to access spider nodes
    private $fedEngine = "federated";				    //!< Federated tables engine to use
    
    private $queryInput = "";					    //!< Holds the input query
    private $shardedQueries = array();				    //!< Holds the sharding query plan
    private $actualQueries = array();				    //!< Holds the translated query plan (translated into elementary functions)
    private $actualQueriesTime = array();			    //!< Array holding query times
    private $queryPlannerOutput = "";				    //!< Any output that the query planner produces during optimisation
    
    private $connection = false;

    private $checkOnDB = true;                          //!< Run checks on DB or has query already been validated elsewhere 
    private $addRowNumbersToFinalTable = false;         //!< Adds row numbers to final result table

    function __construct() {
	$this->queryInput = "";
    }
    
    function __destruct() {
	if($this->connection !== false) {
	    $this->closeDBConnect();
	}
    }

    function setDB($value) {
        $this->defDB = $value;
    }

    function setEngine($value) {
        $this->defEngine = $value;
    }

    function setConnectOnServerSite($value) {
        $this->defConnectOnServerSite = $value;
    }

    function setSpiderUsr($value) {
        $this->defSpiderUsr = $value;
    }

    function setSpiderPwd($value) {
        $this->defSpiderPwd = $value;
    }

    function setFedEngine($value) {
        $this->fedEngine = $value;
    }

    function setCheckOnDB($value) {
        $this->checkOnDB = $value;
    }

    function setAddRowNumbersToFinalTable($value) {
        $this->addRowNumbersToFinalTable = $value;
    }

    /**
     * @brief Set user SQL Query
     * @param validate SQL
     * @param a ZEND DB adapter, if this is called through ZEND
     * 
     * Set a user defined SQL query. This will eventually connect to the DB and check
     * if the query has a valid syntax through "explain". This requires connection to the
     * DB. Further a call to parseSqlAll will be made, which will rewrite any * in the
     * query into a proper list of all columns in the specified tables. Again, this needs
     * connection to the DB to resolve the columns.
     */
    function setSQL($sql, $zendAdapter = false) {
    	$this->queryInput = $sql;
    	
    	if($this->checkOnDB === true && $zendAdapter === false) {
            $this->connectToDB();
            
            $this->connection->queryTimeout = $this->evalQueryTimeout;

            try {
                $res = $this->connection->query("explain " . $sql);
            } catch (MySQLIIException $err) {
                if($err->mysql_errno != 666) {
                    throw $err;
                }
            }
            
            $this->queryInput = parseSqlAll($sql, $this->connection, false);

            $this->closeDBConnect();
        }

        if($zendAdapter !== false) {
            $this->queryInput = parseSqlAll($sql, false, $zendAdapter);
        }
    }
    
    /**
     * @brief Set a query plan using sharded functions
     * @param sql input SQL query
     * 
     * Set a custom defined query plan which uses the sharding query functions:
     *	-   paquExec(sqlQuery, temporaryTable): runs the sqlQuery on all shard nodes
     *	    and returns output to temporaryTable
     *	-   paquDropTmp(temporaryTable): drops the temporary table and closes all
     *	    federated links on the shard nodes
     * 
     * With this function it is possible to implement and define custom sharding strategies
     * if the parallel query planner produces wrong plans. It can also be used to tweak a
     * given query plan manually, by adjusting it and setting it through this function.
     */
    function setShardedQueries($sql) {
        $this->shardedQueries = array();
        
        #this function will merge multiline queries into lines
        $tmpSql = str_replace(array('\\\'','\\"',"\r\n","\n","()"),array("''",'""'," "," "," "), $sql);
        $tmpSql = preg_replace("/\s+/", " ", $tmpSql);
        
        $tokens = explode(";", $tmpSql);
        
        $tmpLine = "";
        foreach($tokens as $token) {
            if(trim($token, " ") === "") {
                continue;
            }
            
            $tmpLine .= $token . ";";
            
            #count ( and ) and compare. if not equal, this string is still open and more lines need to be added
            $count1 = substr_count($tmpLine, "(");
            $count2 = substr_count($tmpLine, ")");
            
            if($count1 !== $count2) {
                continue;
            }
            
            $count1 = substr_count($tmpLine, "\"");
            if($count1 % 2 != 0) {
                continue;
            }
            
            $count1 = substr_count($tmpLine, "`");
            if($count1 % 2 != 0) {
                continue;
            }

            $count1 = substr_count($tmpLine, "'");
            if($count1 % 2 != 0) {
                continue;
            }

            $tmpLine = preg_replace("/;{2,}/", ";", $tmpLine);
            array_push($this->shardedQueries, trim($tmpLine, " ;"));
            $tmpLine = "";            
        }
    }
    
    /**
     * @brief Takes the specified SQL query and runs the parallel optimiser
     * @param resultTable name of the result table in which to write final result
     * 
     * This function will take the specified SQL query and run the approperiate
     * steps to parallelise the query. This will produce a query plan that can then
     * be edited or further processed. Any output of the parallel query planner is
     * written into the queryPlannerOutput variable.
     * 
     * The final result of the query plan is written into $resultTable
     */
    function generateParallelQueryPlan($resultTable) {
    	if($this->queryInput === "") {
    	    throw new Exception('Empty query');
    	}
    	
    	$tmp = explode(".", $resultTable);
    	if(count($tmp) < 2) {
    	    throw new Exception('Specify database');
    	}
    	
    	if($this->checkTableExists($resultTable)) {
    	    throw new Exception("ParallelQuery: Result table already exists");
    	}
    	
    	$shard_query = PHPSQLprepareQuery($this->queryInput);

    	try {
    	    $this->shardedQueries = PHPSQLqueryPlanWriter($shard_query, $resultTable, $this->addRowNumbersToFinalTable);
    	} catch (Exception $error) {
    	    throw new Exception("ParallelQuery: Error\n\n" . $error->getMessage() . "\n");
    	}
    	
    	$this->queryPlannerOutput = $shard_query->outputString;
    }
    
    /**
     * @brief Turns a query plan into actuall SQL that can be run on the head node
     * 
     * This function will parse the query plan and substitute all calls to paquExec
     * and paquDropTmp with proper SQL using spider engine UDFs. This function is needed
     * due to the inabilites of MySQL Stored Procedures to properly work with dynamic SQL queries.
     * 
     * The general idea of the rewriting strategy is: Create temporary table on the head node using
     * the default engine specified in this class. Create federated tables on the shard nodes
     * that link to the actual table on the head node. We are using federated to communicate here.
     * This is necessary, because further queries can then access results of other parallel queries
     * through these federated tables.
     * 
     * In the end, paquDropTmp will drop the federated tables on the shard nodes and eventually 
     * drop the table on the head.
     * 
     * Temporary tables are always written into the temporary database specified in this class.
     * 
     */
    function translateQueryPlan() {
        if($this->shardedQueries === "") {
            throw new Exception('Empty query');
        }
       
        $this->actualQueries = array();
	
        $query_id = "/* PaQu: QID " . mt_rand(1, 100000000) . " */ ";
        
        foreach($this->shardedQueries as $query) {
            if(preg_match('/\s*call\s*paquExec\s*\(\s*\"(.{1,}?)\"\s*,\s*\"(.{1,}?)\"\s*\)\s*/i', $query, $matches)) {
                #remove any LIMIT clause, that might interfere
                #if this limit is part of a subquery, there must be a closing parenthesis at pos > limit_pos
                $tmpPos = strrpos($matches[1], "LIMIT");
                $tmpParPos = strrpos($matches[1], ")");
                if($tmpPos === false || $tmpPos < $tmpParPos) {
                    $tmpPos = strlen($matches[1]);
                }
                $limitFreeQuery = substr($matches[1], 0, $tmpPos);

                $hostTableCreateQuery = $query_id . " CREATE DATABASE IF NOT EXISTS ". $this->defDB .
                                        "; USE ". $this->defDB .
                                        "; " . $query_id . " CREATE TABLE ". $this->defDB ."." . $matches[2] .
                                        " ENGINE=". $this->defEngine . " " . $limitFreeQuery . " LIMIT 0";
                        
                $shardCreateFedTable = $query_id . " SELECT spider_bg_direct_sql('CREATE DATABASE IF NOT EXISTS ". $this->defDB .
                                        "; " . $query_id . " CREATE TABLE ". $this->defDB ."." . $matches[2] . 
                                        " ENGINE=" . $this->fedEngine . " CONNECTION=\"" . $this->defConnectOnServerSite . "/". $this->defDB . "/" . $matches[2] . "\" " . 
                                        $limitFreeQuery . " LIMIT 0;', '', concat('host \"', `__sp__`.host ,'\", port \"', `__sp__`.port ,'\", user \"". $this->defSpiderUsr ."\"";
                
                if(!empty($this->defSpiderPwd)) {
                    $shardCreateFedTable .= ", password \"". $this->defSpiderPwd ."\"";
                }

                $shardCreateFedTable .= "')) from (select * from mysql.spider_tables group by host, port) as `__sp__`"; #where table_name like '" . $this->defTable ."#%'";
                
                $shardActualQuery = $query_id . " SELECT spider_bg_direct_sql('" . $query_id . " USE ". $this->defDB .
                                    "; " . $query_id . " INSERT INTO ". $this->defDB . "." . $matches[2] . " ". 
                                    $matches[1] . "', '', concat('host \"', `__sp__`.host ,'\", port \"', `__sp__`.port ,'\", user \"". $this->defSpiderUsr ."\"";

                if(!empty($this->defSpiderPwd)) {
                    $shardActualQuery .= ", password \"". $this->defSpiderPwd ."\"";
                }

                $shardActualQuery .= "')) from (select * from mysql.spider_tables group by host, port) as `__sp__`"; #where table_name like '" . $this->defTable ."#%'";

                array_push($this->actualQueries, $hostTableCreateQuery);
                array_push($this->actualQueries, $shardCreateFedTable);
                array_push($this->actualQueries, $shardActualQuery);
            } else if(preg_match('/\s*call\s*paquDropTmp\s*\(\s*\"(.{1,}?)\"\s*\)\s*/i', $query, $matches)) {
                $dropTableShard = $query_id . " SELECT spider_bg_direct_sql('" . $query_id . " DROP TABLE ". $this->defDB . "." . $matches[1] . 
                                  "', '', concat('host \"', `__sp__`.host ,'\", port \"', `__sp__`.port ,'\", user \"". $this->defSpiderUsr ."\"";

                if(!empty($this->defSpiderPwd)) {
                    $dropTableShard .= ", password \"". $this->defSpiderPwd ."\"";
                }

                $dropTableShard .= "')) from (select * from mysql.spider_tables group by host, port) as `__sp__`"; # where table_name like '" . $this->defTable ."#%'";
                
                $dropTableHead = $query_id . " DROP TABLE ". $this->defDB . "." . $matches[1];
                
                array_push($this->actualQueries, $dropTableShard);
                array_push($this->actualQueries, $dropTableHead);
            } else {
                array_push($this->actualQueries, $query);
            }
        }
    }
    
    /**
     * @brief Print the parallel query plan to STDOUT
     * 
     * This function will output the parallel query plan to stdout, i.e. print the
     * content of shardedQueries.
     */
    function printParallelQueryPlan() {
	if($this->queryInput === "") {
	    echo "No query...\n";
	    return;
	}
	
	echo "This is the query plan for the query:\n" . $this->queryInput . "\n\n";
	foreach($this->shardedQueries as $query) {
	    echo $query . ";\n";
	}
    }

    /**
     * @brief Get the parallel query plan as array
     * 
     * This function will output the parallel query plan as array
     */
    function getParallelQueryPlan() {
        return $this->shardedQueries;
    }

    /**
     * @brief Set the parallel query plan
     * 
     * This function will set the parallel query plan
     */
    function setParallelQueryPlan($plan) {
        $this->shardedQueries = $plan;
    }

    /**
     * @brief Get the actual queries as array
     * 
     * This function will output the actual queries plan as array
     */
    function getActualQueries() {
        return $this->actualQueries;
    }

     /**
     * @brief Print the parallel query plan to a file
     * @param fh file handler
     * 
     * This function will output the parallel query plan to a file, i.e. print the
     * content of shardedQueries.
     */
    function printParallelQueryPlanToFile($fh) {
	if($this->queryInput === "") {
	    fwrite($fh, "No query...\n");
	    return;
	}
	
	fwrite($fh, "This is the query plan for the query:\n" . $this->queryInput . "\n\n");
	foreach($this->shardedQueries as $query) {
	    fwrite($fh, $query . ";\n");
	}
    }

    /**
     * @brief Print the output of the parallel query planner to STDOUT
     * 
     * This function will output any output produced by the parallel query planner
     * to stdout, i.e. print the content of queryPlannerOutput.
     */
    function printParallelQueryPlanOptimisations() {
	if($this->queryInput === "") {
	    echo "No query...\n";
	    return;
	}
	
	echo "This is the query plan optimisation output for the query:\n" . $this->queryInput . "\n\n";
	echo $this->queryPlannerOutput;
    }

     /**
     * @brief Print the output of the parallel query planner to a file
     * @param fh file ahdnler
     * 
     * This function will output any output produced by the parallel query planner
     * to a file, i.e. print the content of queryPlannerOutput.
     */
    function printParallelQueryPlanOptimisationsToFile($fh) {
	if($this->queryInput === "") {
	    fwrite($fh, "No query...\n");
	    return;
	}
	
	fwrite($fh, "This is the query plan optimisation output for the query:\n" . $this->queryInput . "\n\n");
	fwrite($fh, $this->queryPlannerOutput);
    }
    
    /**
     * @brief Print the actual queries run on the server to STDOUT
     * 
     * This function will output the actual queries that are run on the server
     * to stdout, i.e. the content of actualQueries. If the queries already have
     * been run on the server, the query times are also written.
     */
    function printActualQueries() {
	if($this->queryInput === "") {
	    echo "No query...\n";
	    return;
	}
	
	$this->connectToDB();
	
	echo "These are the actual queries that are run:\n" . $this->queryInput . "\n\n";
	if(empty($this->actualQueriesTime)) {
	    foreach($this->actualQueries as $query) {
		echo $query . ";\n";
	    }
	} else {
	    foreach($this->actualQueries as $key => $query) {
		echo $query . ";\nExecution time: (" . (string)$this->actualQueriesTime[$key] . " sec)\n";
	    }
	}
    }

    /**
     * @brief Print the actual queries run on the server to STDOUT
     * 
     * This function will output the actual queries that are run on the server
     * to a file, i.e. the content of actualQueries. If the queries already have
     * been run on the server, the query times are also written.
     */
    function printActualQueriesToFile($fh) {
	if($this->queryInput === "") {
	    fwrite($fh, "No query...\n");
	    return;
	}
	
	$this->connectToDB();
	
	fwrite($fh, "These are the actual queries that are run:\n" . $this->queryInput . "\n\n");
	if(empty($this->actualQueriesTime)) {
	    foreach($this->actualQueries as $query) {
		fwrite($fh, $query . ";\n");
	    }
	} else {
	    foreach($this->actualQueries as $key => $query) {
		fwrite($fh, $query . ";\nExecution time: (" . (string)$this->actualQueriesTime[$key] . " sec)\n");
	    }
	}
    }
    
    /**
     * @brief Private function to connect to a MySQL DB
     */
    private function connectToDB() {
        if($this->connection === false) {
            preg_match('~mysql://([^:@/]*):?([^@/]*)@?([^/:]*):?([^/]*)/?([^/]*)~', $this->defConnect, $tmp);
            
            $usr = $tmp[1];
            $pwd = $tmp[2];
            $con = $tmp[3];
	    $port = $tmp[4];
            
	    $this->connection = new mysqlii($con, $usr, $pwd, $this->defDB, $port);
            
            if($this->connection->errno) {
                throw new Exception('ParallelQuery Error:\n\nCould not connect to DB: ' . mysql_error() . "\n");
            }
        }
    }
    
    /**
     * @brief Private function executing the queries of the query plan
     */
    public function executeQuery() {
	if($this->queryInput === "" || count($this->actualQueries) == 0) {
	    throw new Exception("Can not execute unspecified/empty query.");
	}
	
	$this->actualQueriesTime = array();
	$queryStartTime = microtime(true);
	
	$this->connectToDB();
	
	foreach($this->actualQueries as $query) {
	    $subQueryStartTime = microtime(true);
	    
	    try {
		$res = $this->unmanagedMultiQuery($query);
	    } catch (MySQLIIException $err) {
		print $err->showMysqlError();
		throw $err;
	    }
	    
	    $subQueryEndTime = microtime(true);
	    array_push($this->actualQueriesTime, ($subQueryEndTime - $subQueryStartTime));
	}
	
	$queryEndTime = microtime(true);
	
	return ($queryEndTime - $queryStartTime);
    }
    
    /**
     * @brief Private function to close the DB connection
     */
    private function closeDBConnect() {
        if($this->connection !== false) {
            $this->connection->close();
            $this->connection = false;
        }
    }
    
    /**
     * @brief Private function to check on the server if a table exists.
     */
    private function checkTableExists($table) {
        if($this->checkOnDB === false) {
            return false;
        }

    	$this->connectToDB();
    	
    	$escTable = $this->connection->real_escape_string($table);
    	
    	try {
    	    $res = $this->connection->query("describe " . $escTable);
    	} catch (MySQLIIException $err) {
    	    if($err->mysql_errno != 1146) {
    		throw $err;
    	    } else {
    		return 0;
    	    }
    	}
    	
    	return 1;
    }
    
    /**
     * @brief Private function to execute queries on the DB server
     * 
     * This function just executes queries and does NOT retrieve any results these
     * queries might produce. Results need to be written to temporary tables on the
     * server!
     */
    private function unmanagedMultiQuery($query) {
	$loop = true;
	if($this->connection->multi_query($query)) {
	    $i = 0;
	    do {
		$i++;
		if($res = $this->connection->use_result()) {
		    if($this->connection->errno) {
			throw new MySQLIIException($this->connection->error, $this->connection->errno);
		    }
		    
		    $res->close();
		}
		
		if($this->connection->more_results()) {
		    $this->connection->next_result();
		} else {
		    $loop = false;
		}
	    } while ($loop);
	    
	    if($this->connection->errno) {
		throw new MySQLIIException($this->connection->error, $this->connection->errno);
	    }
	} else {
	    throw new MySQLIIException($this->connection->error, $this->connection->errno);
	}
    }
}

?>
