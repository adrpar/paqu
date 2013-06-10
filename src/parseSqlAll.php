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

if(!class_exists("PHPSQLParser"))
	require_once 'php-sql-parser2.php';
if(!class_exists("PHPSQLCreator"))
	require_once 'php-sql-creator2.php';

/**
 * @file parseSqlAll.php
 * @brief Parser that transforms SQL * attributes into full list of 
 *	  columns of the given DB table
 *
 * These set of functions will resolve all the SQL * attributes in a
 * given SQL query into the full list of columns. For this, a DB connection
 * is needed to query the columns in a given DB table. 
 * 
 * NOTE: One of the design decision in this set of functions was to use the
 * most recent version of the PHP SQL Parser library 
 * (http://code.google.com/p/php-sql-parser/), especially due to the
 * ability to generate SQL from the parse tree. This new version unfortunately
 * is incompatible with the one used in shard-query-parallel.php. Therefore
 * two version of the same library are present in the code base.
 */


/**
 * @brief Exchange all SQL * attributes in a query with complete column list
 * @param sql SQL statement to apply this filter to
 * @param mysqlConn a properly initialised MySQLI/MySQLII connection to the DB
 * @param zendAdapter a valid ZEND DB adapter
 * @return a new SQL query with the substituted SQL * attribute
 * 
 * This function performs all the substitution, lookup in the DB and other tasks
 * needed to exchange all SQL * attributes with a complete list of columns.
 */
function parseSqlAll($sql, $mysqlConn = false, $zendAdapter = false) {
    $sqlTree = processQueryWildcard($sql, $mysqlConn, $zendAdapter);

    try {
		$newSql = new PHPSQLCreator($sqlTree->parsed);
    } catch (UnableToCreateSQLException $err) {
		return $sql;
    }
    
    return $newSql->created;
}

/**
 * @brief Recursive function that acts on a node of the SQL tree to process the
 *	  SQL query.
 * @param sql SQL statement to apply this filter to
 * @param mysqlConn a properly initialised MySQLI/MySQLII connection to the DB
 * @param zendAdapter a valid ZEND DB adapter
 * @return a new SQL parser tree with the resolved columns
 * 
 * This recursive function parses a given part of the SQL tree and substitutes
 * all the SQL * attributes in subqueries in FROM, WHERE and eventually in the
 * SELECT statement.
 */
function processQueryWildcard($sql, $mysqlConn = false, $zendAdapter = false) {
    $sqlTree = new PHPSQLParser($sql);
    
    _parseSqlAll_FROM($sqlTree->parsed, $mysqlConn, $zendAdapter);
    _parseSqlAll_WHERE($sqlTree->parsed, $mysqlConn, $zendAdapter);
    _parseSqlAll_SELECT($sqlTree->parsed, $mysqlConn, $zendAdapter);

    return $sqlTree;
}

/**
 * @brief Identifies subqueries that need processing in the FROM clause
 * @param sqlTree SQL parser tree node of complete query/subquery
 * @param mysqlConn a properly initialised MySQLI/MySQLII connection to the DB
 * @param zendAdapter a valid ZEND DB adapter
 * 
 * This function parser the current level of the sqlTree to find any subqueries
 * in the FROM statement. If subqueries are found, process them recursively using
 * processQueryWildcard.
 */
function _parseSqlAll_FROM(&$sqlTree, $mysqlConn = false, $zendAdapter = false) {
    if(!array_key_exists('FROM', $sqlTree))
	    return;
    
    foreach($sqlTree['FROM'] as &$node) {
		if($node['expr_type'] == "subquery") {
		    $tree = processQueryWildcard($node['base_expr'], $mysqlConn, $zendAdapter);
		    $node['sub_tree'] = $tree->parsed;
		}
    }
}

/**
 * @brief Identifies subqueries that need processing in the WHERE clause
 * @param sqlTree SQL parser tree node of complete query/subquery
 * @param mysqlConn a properly initialised MySQLI/MySQLII connection to the DB
 * @param zendAdapter a valid ZEND DB adapter
 * 
 * This function parser the current level of the sqlTree to find any subqueries
 * in the WHERE statement. If subqueries are found, process them recursively using
 * processQueryWildcard.
 */
function _parseSqlAll_WHERE(&$sqlTree, $mysqlConn = false, $zendAdapter = false) {
    if(!array_key_exists('WHERE', $sqlTree))
	    return;

    foreach($sqlTree['WHERE'] as &$node) {
		if($node['expr_type'] == "subquery") {
		    $tree = processQueryWildcard(trim($node['base_expr'], '()'), $mysqlConn, $zendAdapter);
	    	    $node['sub_tree'] = $tree->parsed;
		}
    }
}

/**
 * @brief Add all columns to the SELECT tree
 * @param sqlTree SQL parser tree node of complete query/subquery
 * @param mysqlConn a properly initialised MySQLI/MySQLII connection to the DB
 * @param zendAdapter a valid ZEND DB adapter
 * 
 * This function will evaluate the all the tables that need SQL * attribute substitution.
 * The database is queried to retrieve a complete list of columns of each table and the
 * approperiate SELECT colref nodes are added to the SQL parser tree. The SQL * attribute
 * is removed from the sqlTree SELECT node.
 */
function _parseSqlAll_SELECT(&$sqlTree, $mysqlConn = false, $zendAdapter = false) {
	if(!array_key_exists('SELECT', $sqlTree))
		return;

	$table = false;

	$selectCpy = $sqlTree['SELECT'];
	$sqlTree['SELECT'] = array();

	foreach($selectCpy as &$node) {
		if(strpos($node['base_expr'], "*") !== false && $node['sub_tree'] === false) {
			//we have found an all operator and need to find the corresponding
			//table to look things up

			$tmp = _parseSqlAll_parseResourceName($node['base_expr']);

			//make things nicer
			$dbName = false;
			$tableName = false;
			$tableFullName = false;
			if(count($tmp) === 4) {
				$dbName = $tmp[1];
				$tableName = $tmp[2];
				$tableFullName = "`" . $tmp[1] . "`.`" . $tmp[2] . "`";
				$colName = $tmp[3];
			} else if (count($tmp) === 3) {
				$tableName = $tmp[1];
				$tableFullName = "`" . $tmp[1] . "`";
				$colName = $tmp[2];
			} else if (count($tmp) === 2) {
				$colName = $tmp[1];
			}

			$table = array();
			$alias = array();
			if($tableFullName === false) {
				//if this is no alias, we assume that the first table in FROM
				//which is not a subquery resolves to this *

				$aliasNames = true;
				if(count($sqlTree['FROM']) === 1)
					$aliasNames = false;

				foreach($sqlTree['FROM'] as $fromNode) {
					if($fromNode['expr_type'] == "table") {
						$table[] = $fromNode['table'];
						if($aliasNames === false && $fromNode['alias'] === false) {
							$alias[] = false;
						} else {
							$alias[] = trim($fromNode['table'], "`");
						}
					} else if ($fromNode['expr_type'] == "subquery") {
						//handle subqueries...
						_parseSqlAll_linkSubquerySELECT($fromNode['sub_tree'], $sqlTree, $fromNode['alias']['name']);
					}
				}
		    } else {
				foreach($sqlTree['FROM'] as $fromNode) {
				    //it could be, that the table here is actually another aliased table (which should
				    //have been processed here already, since SELECT is called last) -> link to tree
				    if($fromNode['expr_type'] == "table") {
				    	if($fromNode['alias'] !== false) {
							if(trim($fromNode['alias']['name'], "`") === $tableName) {
							    $table[] = $fromNode['table'];
							    break;
							}
						} else {
							if($fromNode['table'] === $tableFullName) {
							    $table[] = $fromNode['table'];
							    break;
							}
						}
				    } else if ($fromNode['expr_type'] == "subquery") {
						if(trim($fromNode['alias']['name'], "`") === $tableName) {
						    _parseSqlAll_linkSubquerySELECT($fromNode['sub_tree'], $sqlTree, $tableName);
				    		continue 2;
						}			
				    }
				}
				$alias[] = trim($tableFullName, "`");
		    }
		    
		    if(empty($table))
				continue;

		    //now that we know the table, we need to look up what is in there
		    foreach(array_keys($table) as $key) {
			    if($mysqlConn !== false) {
				    _parseSqlAll_getColsMysqlii($sqlTree, $node, $mysqlConn, $table[$key], str_replace(".", "`.`", $alias[$key]));
			    }

			    if($zendAdapter !== false) {
				    _parseSqlAll_getColsZend($sqlTree, $node, $zendAdapter, $table[$key], str_replace(".", "`.`", $alias[$key]));
			    }
		    }
		} else {
			array_push($sqlTree['SELECT'], $node);
		}
	}
}

function _parseSqlAll_linkSubquerySELECT(&$subtreeNode, &$resultTree, $alias) {
    //link the rows to the tree
    $count = 0;
    foreach($subtreeNode['SELECT'] as $selNode) {
		$tmp = _parseSqlAll_parseResourceName($selNode['base_expr']);

		unset($tmp[0]);
		$count = 0;
		$selNode['alias'] = array("as" => true,
								  "name" => "",
								  "base_expr" => "as ");
		foreach($tmp as $element) {
			if($count === 0) {
				$selNode['base_expr'] = "`" . $alias . "`.`" . $element;
				$selNode['alias']['name'] = "`" . $alias . "__" . $element;
			} else {
				$selNode['base_expr'] .= "__" . $element;
				$selNode['alias']['name'] .= "__" . $element;
			}

			$count += 1;
		}

		$selNode['base_expr'] .= "`";
		$selNode['alias']['name'] .= "`";
		$selNode['alias']['base_expr'] .= $selNode['alias']['name'];

		if($count === 0) {
		    $node = $selNode;
		} else {
		    array_push($resultTree['SELECT'], $selNode);
		}

		$count++;
    }
}

function _parseSqlAll_parseResourceName($resource) {
	$tmp = array();
	$tmp[0] = $resource;

	$split = explode(".", $tmp[0]);
	$currFullName = "";
	foreach($split as $token) {
		$numQuote = substr_count($token, "`");
		if($numQuote === 2 || ($currFullName === "" && $numQuote === 0)) {
			//either `foo` or foo. token
			$tmp[] = trim($token, "`");
		} else if ($currFullName !== "" && $numQuote === 1) {
			$currFullName .= "." . trim($token, "`");
			$tmp[] = $currFullName;
			$currFullName = "";
		} else {
			if($currFullName === "") {
				$currFullName .= trim($token, "`");
			} else {
				$currFullName .= "." . trim($token, "`");
			}
		}
	}

	return $tmp;
}

function _parseSqlAll_getColsMysqlii(&$sqlTree, &$node, $mysqlConn, $table, $alias) {
    $mysqlConn->queryTimeout = 2000;
    try {
		$res = $mysqlConn->noTimeOutQuery("describe " . $table);
    } catch (MySQLIIException $err) {
		throw $err;
    }

    $count = 0;
    while($row = $res->fetch_row()) {
		if($count == 0) {
		    //this is the item we change
		    if($alias === false) {
				$node['base_expr'] = $row[0];
		    } else {
				$node['base_expr'] = "`" . $alias . "`." . $row[0];
				$node['alias'] = array("as" => true,
								   "name" => "`" . str_replace(".", "__", str_replace("`", "", $node['base_expr'])) . "`",
								   "base_expr" => "as `" . str_replace(".", "__", str_replace("`", "", $node['base_expr'])) . "`");
		    }
		    $nodeTemplate = $node;

		    array_push($sqlTree['SELECT'], $node);
		} else {
		    $newNode = $nodeTemplate;			//this is set on the first passing when count is 0
		    if($alias === false) {
				$newNode['base_expr'] = $row[0];
		    } else {
				$newNode['base_expr'] = "`" . $alias . "`." . $row[0];
				$newNode['alias'] = array("as" => true,
								   "name" => "`" . str_replace(".", "__", str_replace("`", "", $newNode['base_expr'])) . "`",
								   "base_expr" => "as `" . str_replace(".", "__", str_replace("`", "", $newNode['base_expr'])) . "`");
		    }
		    
		    array_push($sqlTree['SELECT'], $newNode);
		}
		
		$count++;
    }
}

function _parseSqlAll_getColsZend(&$sqlTree, &$node, $zendAdapter, $table, $alias) {
	$res = $zendAdapter->query("describe " . $table)->fetchAll();

    $count = 0;

    foreach($res as $count => $row) {
		if($count == 0) {
		    //this is the item we change
		    if($alias === false || empty($alias)) {
				$node['base_expr'] = $row['Field'];
		    } else {
				$node['base_expr'] = "`" . $alias . "`." . $row['Field'];
				$node['alias'] = array("as" => true,
								   "name" => "`" . str_replace(".", "__", str_replace("`", "", $node['base_expr'])) . "`",
								   "base_expr" => "as `" . str_replace(".", "__", str_replace("`", "", $node['base_expr'])) . "`");
		    }
		    $nodeTemplate = $node;

		    array_push($sqlTree['SELECT'], $node);
		} else {
		    $newNode = $nodeTemplate;			//this is set on the first passing when count is 0
		    if($alias === false || empty($alias)) {
				$newNode['base_expr'] = $row['Field'];
		    } else {
				$newNode['base_expr'] = "`" . $alias . "`." . $row['Field'];
				$newNode['alias'] = array("as" => true,
								   "name" => "`" . str_replace(".", "__", str_replace("`", "", $newNode['base_expr'])) . "`",
								   "base_expr" => "as `" . str_replace(".", "__", str_replace("`", "", $newNode['base_expr'])) . "`");
		    }
		    
		    array_push($sqlTree['SELECT'], $newNode);
		}
    }
}

