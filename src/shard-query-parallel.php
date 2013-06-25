<?php

/*
  Copyright (c) 2010, Justin Swanhart
  Parts (c) 2012, Adrian Partl @ Leibniz Inst. for Astrophysics Potsdam
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

/* This script requires PEAR Net_Gearman */
/* It also requires Console_Getopt, but this should be installed by default with pear */
require_once 'php-sql-parser.php';
require_once 'parOptimImplicitJoin.php';
#$params = get_commandline();
#FIXME: This should not extend MySQLDAl, but should create a new DL object
#DL needs additions such as creating table, check existence of table, etc.
#This will be useful for FlexCDC and Shard-Query.

class ShardQuery {

    var $messages = array();
    var $push_where = "";
    var $inlist_merge_threshold = 128;
    var $inlist_merge_size = 128;
    var $engine = 'MEMORY';
    var $cols;
    var $debug;
    var $fetch_method;
    var $gearman_servers;
    var $inlist_opt;
    var $row_count;
    var $shown_temp_table_create;
    var $table_name;
    var $tmp_shard;
    var $verbose;
    var $conn = null;
    var $workers; # the count of workers
    var $force_shard = array();
    var $error = false;
    var $partition_callback = false;
    var $partition_column = false;
    var $coord_odku = array();
    var $subqueries = array();
    var $outputString = "";

    #FIXME: Make this support SUM(X) + SUM(Y).
    #To do this create process_select_expr and then iterate (possively recursively) 
    #over all expressions in the select calling that function

    function process_select($select, $recLevel, $straight_join = false, $distinct = false, $whereSubquery = false) {
		$error = array();
		$shard_query = "";   #Query to send to each shard
		$coord_query = "";   #Query to send to the coordination node

		$avg_count = 0;

		$group = array();  #list of positions which contain non-aggregate functions
		$push_group = array();   #this is necessary for non-distributable aggregate functions
		$group_aliases = array(); #list of group aliases which will be indexed on the aggregation temp table

		$is_aggregate = false;
		$coord_odku = array();

		if($distinct === true) {
		    $shard_query .= "DISTINCT ";
		    $coord_query .= "DISTINCT ";
		}

		$used_agg_func = 0;

		foreach ($select as $pos => $clause) {
		    if ($shard_query && $pos != 0)
				$shard_query .= ",";
		    if ($coord_query && $pos != 0)
				$coord_query .= ",";

		    /*if (!empty($clause['base_expr']) && $clause['base_expr'] == "*") {
				$error[] = array('error_clause' => '*', 'error_reason' => '"SELECT *" is not supported');
				continue;
		    }*/
		    $alias = $clause['alias'];

		    if (strpos($alias, '.')) {
				$alias = trim($alias, '``');
				$tmp = explode('.', $alias);

				if (count($tmp) > 2) {
				    #this is a more complicated expression, use the alias as it is (might be a formula or something)
				    $alias = "`" . substr(trim($clause['alias'], "`"), 0, 50) . "`";
				} else {
				    $alias = $tmp[0] . "." . $tmp[1];
				    $alias = "`$alias`";
				}
		    }

		    //further escape any function name that might end up in the alias
            if ($clause['expr_type'] === "aggregate_function" ||
                    $clause['expr_type'] === "function") {

                //build escaped string
                $alias = "_" . $this->buildEscapedString(array($clause));
        	}

    /**
     * Function that will recursively go through the branch at the function to
     * construct the escaped column name
     * @param array $inNode SQL parse tree node
     * @return string parts of the escaped function name
     */
    function buildEscapedString($inNode) {
        $str = "";

        foreach ($inNode as $currNode) {
            $partStr = "";

            if (array_key_exists("sub_tree", $currNode) && $currNode["sub_tree"] !== false) {
                $partStr = $this->buildEscapedString($currNode["sub_tree"]);
            }

            $partStr = str_replace(".", "__", $partStr);

            if ($currNode["expr_type"] === "aggregate_function" ||
                    $currNode['expr_type'] === "function") {
                $str .= $currNode["base_expr"] . "_" . $partStr;        #last "_" already added below
            } else {
                $str .= $currNode["base_expr"] . "_";
            }
        }

        return $str;
    }

		    $base_expr = $clause['base_expr'];

		    switch ($clause['expr_type']) {
				case 'expression':
				    if ($clause['sub_tree'][0]['expr_type'] == 'aggregate_function') {
						$is_aggregate = true;
						$skip_next = true;
						$base_expr = $clause['sub_tree'][1]['base_expr'];
						$alias = "`" . substr(trim($clause['alias'], "`"), 0, 50) . "`";
						$function = $clause['sub_tree'][0]['base_expr'];

						switch ($function) {
						    #these are aggregates that dont need special treatment on the coordination side
						    case 'MIN':
						    case 'MAX':
						    case 'SUM':
								$used_agg_func = 1;
								$base_expr = trim($base_expr, ' ()');
								$base_expr = fix_trunc_parenth($base_expr);
								$expr_info = explode(" ", $base_expr);
								if (!empty($expr_info[0]) && strtolower($expr_info[0]) == 'distinct') {
								    if ($this->verbose) {
										echo "Detected a {$function} [DISTINCT] expression!\n";
								    }

								    unset($expr_info[0]);
								    $new_expr = join(" ", $expr_info);
								    $shard_query .= "$new_expr AS $alias";
								    $coord_query .= "{$function}(distinct $alias) as $alias";
								    $push_group[] = $pos + 1;
								} else {
								    switch ($function) {
										case 'SUM':
										    $coord_odku[] = "$alias=$alias +  VALUES($alias)";
										    break;
										case 'MIN':
										    $coord_odku[] = "$alias=IF($alias < VALUES($alias), VALUES($alias),$alias)";
										    break;
										case 'MAX':
										    $coord_odku[] = "$alias=IF($alias > VALUES($alias), VALUES($alias), $alias)";
										    break;
								    }
								    $shard_query .= "{$function}({$base_expr}) AS $alias";
								    $coord_query .= "{$function}({$alias}) AS $alias";
								}

								break;

						    #special treatment needed
						    case 'AVG':
						    case 'STDDEV':
						    case 'STD':
						    case 'STDDEV_POP':
						    case 'STDDEV_SAMP':
						    case 'VARIANCE':
						    case 'VAR_POP':
						    case 'VAR_SAMP':
						    case 'GROUP_CONCAT':
								$used_agg_func = 1;
								$base_expr = trim($base_expr, ' ()');
								$base_expr = fix_trunc_parenth($base_expr);
								$expr_info = explode(" ", $base_expr);
								if (!empty($expr_info[0]) && strtolower($expr_info[0]) == 'distinct') {
								    if ($this->verbose) {
										echo "Detected a {$function} [DISTINCT] expression!\n";
								    }

								    unset($expr_info[0]);
								    $new_expr = join(" ", $expr_info);
								    $shard_query .= "$new_expr AS $alias";
								    $coord_query .= "{$function}(distinct $alias) as $alias";
								} else {
								    switch ($function) {
										case 'AVG':
										    $alias = trim($alias, '`');
										    $shard_query .= " COUNT({$base_expr}) AS `cnt_{$alias}`, SUM({$base_expr}) AS `sum_{$alias}`";
										    $coord_query .= " (SUM(`sum_{$alias}`) / SUM(`cnt_{$alias}`)) AS `$alias`";
										    break;
										case 'STDDEV':
										case 'STD':
										case 'STDDEV_POP':
										    $alias = trim($alias, '`');
										    $avgAlias = '`agr_' . $alias . '`';
										    $shard_query .= " sum_of_squares({$base_expr}) AS `ssqr_{$alias}`, AVG({$base_expr}) as `avg_{$alias}`, COUNT({$base_expr}) AS `cnt_{$alias}`";
										    $coord_query .= " SQRT(partitAdd_sum_of_squares(`ssqr_{$alias}`, `avg_{$alias}`, `cnt_{$alias}`) / (SUM(`cnt_{$alias}`) - 1)) AS `$alias`";
										    break;

										case 'STDDEV_SAMP':
										    $alias = trim($alias, '`');
										    $avgAlias = '`agr_' . $alias . '`';
										    $shard_query .= " sum_of_squares({$base_expr}) AS `ssqr_{$alias}`, AVG({$base_expr}) as `avg_{$alias}`, COUNT({$base_expr}) AS `cnt_{$alias}`";
										    $coord_query .= " SQRT(partitAdd_sum_of_squares(`ssqr_{$alias}`, `avg_{$alias}`, `cnt_{$alias}`) / SUM(`cnt_{$alias}`)) AS `$alias`";
										    break;

										case 'VARIANCE':
										case 'VAR_POP':
										    $alias = trim($alias, '`');
										    $avgAlias = '`agr_' . $alias . '`';
										    $shard_query .= " sum_of_squares({$base_expr}) AS `ssqr_{$alias}`, AVG({$base_expr}) as `avg_{$alias}`, COUNT({$base_expr}) AS `cnt_{$alias}`";
										    $coord_query .= " partitAdd_sum_of_squares(`ssqr_{$alias}`, `avg_{$alias}`, `cnt_{$alias}`) / (SUM(`cnt_{$alias}`) - 1) AS `$alias`";
										    break;

										case 'VAR_SAMP':
										    $alias = trim($alias, '`');
										    $avgAlias = '`agr_' . $alias . '`';
										    $shard_query .= " sum_of_squares({$base_expr}) AS `ssqr_{$alias}`, AVG({$base_expr}) as `avg_{$alias}`, COUNT({$base_expr}) AS `cnt_{$alias}`";
										    $coord_query .= " partitAdd_sum_of_squares(`ssqr_{$alias}`, `avg_{$alias}`, `cnt_{$alias}`) / (SUM(`cnt_{$alias}`) - 1) AS `$alias`";
										    break;

										default:
										    $shard_query .= "{$function}({$base_expr}) AS $alias";
										    $coord_query .= "{$function}({$alias}) AS $alias";
										    break;
								    }
								}
								$push_group[] = $pos + 1;
								$group_aliases[] = $alias;

								break;

						    case 'COUNT':
								$used_agg_func = 1;
								$base_expr = trim($base_expr, ' ()');
								$base_expr = fix_trunc_parenth($base_expr);

								$expr_info = explode(" ", $base_expr);
								if (!empty($expr_info[0]) && strtolower($expr_info[0]) == 'distinct') {
								    if ($this->verbose) {
										echo "Detected a COUNT [DISTINCT] expression!\n";
								    }
								    unset($expr_info[0]);
								    $new_expr = join(" ", $expr_info);
								    $shard_query .= "$new_expr AS $alias";
								    $coord_query .= "COUNT(distinct $alias) as $alias";
								    $push_group[] = $pos + 1;
								} else {
								    $shard_query .= "COUNT({$base_expr}) AS $alias";
								    $coord_query .= "SUM($alias) AS $alias";
								    $coord_odku[] = "$alias=$alias +  VALUES($alias)";
								}

								break;

						    default:
								$error[] = array('error_clause' => $clause['base_expr'],
								    'error_reason' => 'Unsupported aggregate function');

								break;
						}
			    	} else {
						$group[] = $pos + 1;
						$group_aliases[] = $alias;

						$shard_query .= $base_expr . ' AS ' . $alias;

						#if this is a temporary column used for grouping, don't select it in the coordination query
						if (!array_key_exists('group_clause', $clause) && $whereSubquery === false) {
						    $coord_query .= $alias;
						    $coord_odku[] = "$alias=VALUES($alias)";
						} else {
						    $coord_query = substr($coord_query, 0, -1);
						}
			    	}

				    break;

				case 'operator':
				case 'const':
				case 'colref':
				case 'reserved':
				case 'function':
				    $group[] = $pos + 1;
				    $group_aliases[] = $alias;

				    $shard_query .= $base_expr . ' AS ' . $alias;
				    
				    #exclude certain aggregation expressions
				    if (strpos($alias, 'agr_stddev') === false) {
						#if this is a temporary column from WHERE, don't select it in the coordination
						if(!array_key_exists('where_col', $clause) || $recLevel >= 0) {
						    #don't select order by columns, that are implicit
						    //if(!array_key_exists('order_clause', $clause)) {
						    if($whereSubquery === false) {
								$coord_query .= $alias;
								$coord_odku[] = "$alias=VALUES($alias)";
						    } else {
								if(!array_key_exists('where_col', $clause) && !array_key_exists('order_clause', $clause)) {
								    $coord_query .= $alias;
								    $coord_odku[] = "$alias=VALUES($alias)";
								} else {
								    $coord_query = substr($coord_query, 0, -1);
								}
						    }
						} else {
				    		$coord_query = substr($coord_query, 0, -1);
						}
				    } else {
						#remove the ',' that is too much in the query
						$coord_query = substr($coord_query, 0, -1);
				    }

			    	break;

				default:
				    $error[] = array('error_clause' => $clause['base_expr'],
					'error_reason' => 'Unsupported expression type (did you forget an alias on an aggregate expression?)');
				    break;
	    	}
		}

		$sql = "SELECT ";
		if ($straight_join)
		    $sql .= "STRAIGHT_JOIN ";

		$sql .= $shard_query;

		$shard_group = array();
		#merge pushed and provided group-by
		if ($used_agg_func) {
		    $shard_group = $group;
		    foreach ($push_group as $push) {
				$shard_group[] = $push;
		    }
		    #they shouldn't conflict, but this ensures so
		    $shard_group = array_unique($shard_group);
		} else {
		    $group = array();
		    $shard_group = array();
		}

		#we can't send pushed group by to the coord shard, so send the expression based 
		return array('error' => $error, 'shard_sql' => $sql, 'coord_odku' => $coord_odku, 'coord_sql' => 'SELECT ' . $coord_query, 'shard_group' => join(',', $shard_group), 'coord_group' => join(',', $group), 'group_aliases' => join(',', $group_aliases));
    }

    function process_from($tables, $recLevel) {

	/* DEPENDENT-SUBQUERY handling
	 */

	foreach ($tables as $key => $table) {
	    if ($table['table'] == 'DEPENDENT-SUBQUERY') {
		$tmpTree = $this->parsed;
		
		#determine if this is a serial or parallel query:
		#parallel only if there are other tables than temporary (i.e. dependent) ones involved
		$parallel = false;
		foreach($table['sub_tree']['FROM'] as $fromNode) {
		    if($fromNode['table'] != 'DEPENDENT-SUBQUERY') {
			$parallel = true;
		    }
		}
		
		$this->process_sql($table['sub_tree'], $recLevel++);
		$this->parsed = $tmpTree;
		$this->subqueries[$this->table_name] = $this->shard_sql;
		$this->subqueries[$this->table_name]['parallel'] = $parallel;

		$sql = '( ' . $this->coord_sql . ')';
		$tables[$key]['table'] = $sql;
	    }
	}

	#escape the table name if it is unescaped
	if ($tables[0]['alias'] != "" && $tables[0]['alias'][0] != '`' && $tables[0]['alias'][0] != '(')
	    $tables[0]['alias'] = '`' . $tables[0]['alias'] . '`';

	#the first table is always prefixed by FROM
	$sql = "FROM " . $tables[0]['table'];
	if($tables[0]['alias'] != "") {
	    $sql .= ' AS ' . $tables[0]['alias'];
	}
	
	$cnt = count($tables);

	#now create the rest of the FROM clause
	for ($i = 1; $i < $cnt; ++$i) {

	    if ($tables[$i]['ref_type'] == 'USING') {
		$tables[$i]['ref_clause'] = "(" . trim($tables[$i]['ref_clause']) . ")";
	    } elseif ($tables[$i]['ref_type'] == 'ON') {
		$tables[$i]['ref_clause'] = ' (' . $tables[$i]['ref_clause'] . ") ";
	    }

	    if ($sql)
		$sql .= " ";
	    if ($tables[$i]['alias'] != "" && $tables[$i]['alias'][0] != '`' && $tables[$i]['alias'][0] != '(') {
		$pos = strpos($tables[$i]['alias'], '.');
		if ($pos !== false) {
		    $info = explode('.', $tables[$i]['alias']);
		    $table = $info[1];
		    $tables[$i]['alias'] = '`' . $table . '`';
		} else {
		    $tables[$i]['alias'] = '`' . $tables[$i]['alias'] . '`';
		}
	    }
	    $sql .= $tables[$i]['join_type'] . ' ' . $tables[$i]['table'];
	    if($tables[$i]['alias'] != "")
		$sql .= ' AS ' . $tables[$i]['alias'];
		$sql .= ' ' . $tables[$i]['ref_type'] . $tables[$i]['ref_clause'];
	}

	return $sql;
    }

    function set_partition_info($column, $callback) {
	$this->partition_column = $column;
	if (!is_callable($callback)) {
	    throw new Exception('Invalid callback (is_callable failed)');
	}

	$this->callback = $callback;
    }

    function get_partition_info($column, $key) {
	$result = call_user_func($this->callback, $column, $key);
	#	if($this->verbose) {
	#		echo "PARTITION LOOKUP: $column, $key => $result\n";
	#	}

	if (is_array($result)) {
	    $keys = array_keys($result);
	    $result = $keys[0];
	}
	return $result;
    }

    function append_all(&$queries, $append) {
	for ($i = 0; $i < count($queries); ++$i) {
	    $queries[$i] .= $append;
	}
    }

    function process_where($where) {
	$this->in_lists = array();
	$prev = "";
	$next_is_part_key = false;
	$this->force_shard = false;
	$shard_id = false;
	$this->force_broadcast = false;
	$total_count = 0;

	$sql = "WHERE";
	$queries = array($sql);

	$start_count = count($where);
	foreach ($where as $pos => $clause) {
	    $tmpSql = "";
	    $isSubquery = false;
	    
	    if($clause['sub_tree'] !== false) {
		foreach($clause['sub_tree'] as $key => $subNode) {
		    if($subNode['expr_type'] == 'subquery') {
			$tmpTree = $this->parsed;
			$tmpTableName = $this->table_name;

			$this->table_name = "aggregation_tmp_" . mt_rand(1, 100000000);
			$this->process_sql($subNode['sub_tree'], 0, true);
			$this->parsed = $tmpTree;
			$this->subqueries[$this->table_name] = $this->shard_sql;
			$this->subqueries[$this->table_name]['parallel'] = true;

			$this->table_name = $tmpTableName;
			$tmpSql .= '( ' . $this->coord_sql . ') ';
			$clause[$key]['table'] = $sql;

			$isSubquery = true;
		    } else {
			$tmpSql .= $subNode['base_expr'] . " ";
		    }
		}

		if($isSubquery === true) {
		    $clause['base_expr'] = $tmpSql;
		}
	    }
	    	    
	    if (empty($where[$pos]))
		continue;
	    $sql .= " ";
	    $this->append_all($queries, " ");
	    if ($next_is_part_key) {
		if (!trim($clause['base_expr']))
		    continue;
		if ($clause['expr_type'] == 'const' && $shard_id = $this->get_partition_info($prev, $clause['base_expr'])) {
		    if ($this->verbose)
			echo "PARTITION SELECTION SELECTED SHARD_ID: $shard_id\n";
		    $this->force_shard = $shard_id;
		}
		$next_is_part_key = false;
	    }

	    if ($clause['expr_type'] == 'operator') {
		if (strtolower($clause['base_expr']) == 'between' &&
			$this->between_opt && ($this->between_opt == '*' || $this->between_opt == $prev)) {
		    $offset = 0;
		    $operands = array();
		    #find the operands to the between expression	
		    $and_count = 0;

		    for ($n = $pos + 1; $n < $start_count; ++$n) {
			if ($where[$n]['expr_type'] == 'operator' && strtoupper($where[$n]['base_expr']) == 'AND') {
			    if ($and_count) {
				break;
			    } else {
				$and_count+=1;
			    }
			}
			$operands[$offset] = array('pos' => $n, 'base_expr' => $where[$n]['base_expr']);
			++$offset;
		    }

		    #determine what kinds of operands are in use
		    $matches = $vals = array();
		    $is_date = false;

		    if (is_numeric(trim($operands[0]['base_expr'])) ||
			    preg_match("/('[0-9]+-[0-9]+-[0-9]+')/", $operands[0]['base_expr'], $matches)) {
			if ($matches) {
			    $vals[0] = $matches[0];
			    $matches = array();
			    preg_match("/('[0-9]+-[0-9]+-[0-9]+')/", $operands[2]['base_expr'], $matches);
			    $vals[1] = $matches[0];

			    $is_date = true;
			} else {
			    $vals[0] = $operands[0]['base_expr'];
			    $vals[1] = $operands[2]['base_expr'];
			}
			if (!$is_date) {
			    $sub_tree = array();
			    for ($n = $vals[0]; $n <= $vals[1]; ++$n) {
				$sub_tree[] = $n;
			    }
			} else {

			    #conversion of date between requires connecting
			    #to the database to make sure that the date_diff calculation
			    #is accurate for the timezone in which the database servers are

			    $date_sql = "SELECT datediff(" . $vals[1] . ',' . $vals[0] . ") as `d`";
			    if ($this->verbose) {
				echo "Sending SQL to do date calculation:\n$date_sql\n\n";
			    }

			    $stmt = $this->my_query($date_sql);
			    if (!$stmt) {
				throw new Exception("While doing date diff: " . $this->my_error($this->conn));
			    }

			    $row = mysql_fetch_assoc($stmt);
			    $days = $row['d'];
			    for ($n = 0; $n <= $days; ++$n) {
				$sub_tree[] = $vals[0] . " + interval $n day";
			    }
			}

			for ($n = $pos + 1; $n <= $operands[2]['pos']; ++$n) {
			    unset($where[$n]);
			}

			if ($this->verbose) {
			    $this->messages[] = "A BETWEEN has been converted to an IN list with " . count($sub_tree) . " items\n";
			}
			$this->in_lists[] = $sub_tree;
			$old = $queries;

			$queries = array("");
			$ilist = "";
			$sub_tree = array_values($sub_tree);

			if (count($sub_tree) >= $this->inlist_merge_threshold) {
			    for ($z = 0; $z < count($sub_tree); ++$z) {
				if ($ilist)
				    $ilist .= ",";
				$ilist .= $sub_tree[$z];
				if ((($z + 1) % $this->inlist_merge_size) == 0) {
				    foreach ($old as $sql) {
					$queries[] = $sql . " IN (" . $ilist . ")";
				    }
				    $ilist = "";
				}
			    }
			    foreach ($old as $sql) {
				if ($ilist)
				    $queries[] = $sql . " IN (" . $ilist . ")";
			    }
			    $ilist = "";
			} else {
			    foreach ($sub_tree as $val) {
				foreach ($old as $sql) {
				    $queries[] = $sql .= " = $val";
				}
			    }
			}

			unset($sub_tree);

			continue;
		    } else {
			if ($this->verbose) {
			    echo "BETWEEN could not be optimized - invalid operands\n";
			}
		    }
		} elseif ($clause['base_expr'] == '=' &&
			($this->partition_column && strtolower($this->partition_column) == strtolower($prev) && !$this->force_broadcast )) {
		    if (!$this->force_shard) {
			$next_is_part_key = true;
		    } else {
			if ($this->verbose) {
			    echo "More than one partition key found.  Query broadcast forced\n";
			}
			$this->force_shard = false;
			$this->force_broadcast = true;
		    }
		}
		$this->append_all($queries, $clause['base_expr']);
	    } elseif ($clause['expr_type'] != 'in-list') {
		$this->append_all($queries, $clause['base_expr']);
		$prev = $clause['base_expr'];
	    } elseif ($this->inlist_opt && ($this->inlist_opt == '*' || $this->inlist_opt == $prev)) {
		$old = $queries;
		$queries = array();

		foreach ($clause['sub_tree'] as $vals) {

		    foreach ($old as $sql) {
			$queries[] = "$sql ($vals) ";
		    }
		}
	    } else {
		$prev = $clause['base_expr'];
		$this->append_all($queries, $prev);
	    }
	}

	foreach ($queries as $pos => $q) {
	    if (!trim($q))
		unset($queries[$pos]);
	}

	return array_values($queries);
    }

    /* if $sql is an Array(), then it is assumed it is already parsed */

function process_sql($sql, $recLevel = 0, $whereSubquery = false) {
	#only useful for the fetch worker for debugging	
	$this->shown_temp_table_create = true;
	$this->sql = $sql;
	$parser = null;
	$straight_join = false;

	$conn = false;

	$this->shard_sql = ""; #identical SQL which will be broadcast to all shards
	$this->coord_sql = ""; #the summary table sql
	$this->in_lists = array();
	$error = array();

	$select = null;

	if (!is_array($sql)) {
	    #TODO: support parser re-use	
	    #$this->parsed = $this->client->do('sql_parse',$sql);
	    $parser = new PHPSQLParserOld($sql);

	    $this->parsed = PHPSQLbuildShardQuery($parser->parsed);
	    $this->parsedCopy = $this->parsed;
	} else {
	    $this->parsed = $sql;
	}

	if (!empty($this->parsed['UNION ALL'])) {
	    $queries = array();
	    foreach ($this->parsed['UNION ALL'] as $sub_tree) {
			$this->process_sql($sub_tree, $recLevel++);
			$queries = array_merge($queries, $this->shard_sql);
	    }
	    $this->table_name = "aggregation_tmp_" . mt_rand(1, 100000000);
	    $coord_sql = "SELECT * from " . $this->table_name;
	} elseif (!empty($this->parsed['UNION'])) {
	    $queries = array();
	    foreach ($this->parsed['UNION'] as $sub_tree) {
			$this->process_sql($sub_tree, $recLevel++);
			$queries = array_merge($queries, $this->shard_sql);
	    }
	    $this->table_name = "aggregation_tmp_" . mt_rand(1, 100000000);

	    #UNION operation requires deduplication of the temporary table
	    $coord_sql = "SELECT DISTINCT * from " . $this->table_name;
	} elseif (!empty($this->parsed['SELECT'])) {
	    #reset the important variables	
	    $select = $from = $where = $group = $order_by = $order_by_coord = "";
	    $this->errors = array();

	    #we only support SQL_MODE=ONLY_FULL_GROUP_BY, and we build the GROUP BY from the SELECT expression
	    //unset($this->parsed['GROUP']);
	    #The SELECT clause is processed first.
	    $distinct = false;
	    if (!empty($this->parsed['OPTIONS'])) {
			if(in_array('STRAIGHT_JOIN', $this->parsed['OPTIONS'])) {
			    $straight_join = true;
			} 
		
			if (in_array('DISTINCT', $this->parsed['OPTIONS'])) {
			    $distinct = true;
			}
		
			unset($this->parsed['OPTIONS']);
	    }

	    $select = $this->process_select($this->parsed['SELECT'], $recLevel, $straight_join, $distinct, $whereSubquery);

	    if (!empty($select['error'])) {
			$this->errors = $select['error'];
			return false;
	    }

	    unset($this->parsed['SELECT']);

	    if (empty($this->parsed['FROM'])) {
			$this->errors = array('Unsupported query', 'Missing FROM clause');
			return false;
	    } else {
			$select['shard_sql'] .= "\n" . $this->process_from($this->parsed['FROM'], $recLevel);
			$this->table_name = "aggregation_tmp_" . mt_rand(1, 100000000);

			#we only select from a single table here	
			$select['coord_sql'] .= "\nFROM `$this->table_name`";
			
			if(array_key_exists("USE INDEX", $this->parsed) || array_key_exists("IGNORE INDEX", $this->parsed)) {
			    $index = "";
			    if(array_key_exists("USE INDEX", $this->parsed)) {
					$index .= " USE INDEX ";
					$tree = $this->parsed['USE INDEX'];
					unset($this->parsed['USE INDEX']);
			    }

			    if(array_key_exists("IGNORE INDEX", $this->parsed)) {
					$index .= " IGNORE INDEX ";
					$tree = $this->parsed['IGNORE INDEX'];
					unset($this->parsed['IGNORE INDEX']);
			    }
			    
			    if($tree !== false) {
					foreach($tree as $node) {
					    $index .= $node['base_expr'] . " ";
					}
			    }
		    
			    $select['shard_sql'] .= $index;
			    $select['coord_sql'] .= $index;
			}

			unset($this->parsed['FROM']);
	    }

	    if ($this->push_where !== false && $this->push_where) {
			if (!empty($this->parsed['WHERE'])) {
			    $this->parsed['WHERE'][] = array('expr_type' => 'operator', 'base_expr' => 'and', 'sub_tree' => "");
			}
			if (!$parser)
			    $parser = new PHPSQLParserOld();
			$this->messages[] = "Where clause push detected.  Pushing additional WHERE condition:'" . $this->push_where . "' to each storage node.\n";
			if ($this->push_where)
			    foreach ($parser->process_expr_list($parser->split_sql($this->push_where)) as $item)
					$this->parsed['WHERE'][] = $item;
		    }

		    #note that this will extract inlists and store them in $this->in_lists (if inlist optimization is on) 	
		    if (!empty($this->parsed['WHERE'])) {
				$where_clauses = $this->process_where($this->parsed['WHERE']);
				unset($this->parsed['WHERE']);
		    }

		    if (!empty($this->parsed['ORDER'])) {
				$order_by = "";
				$order_by_coord = "";
				foreach ($this->parsed['ORDER'] as $o) {
				    if ($order_by)
						$order_by .= ",";
				    if ($order_by_coord)
						$order_by_coord .= ",";
				    
				    #check if order by arguemnt is just a number. if yes, we dont need to quote this
				    if(is_numeric(trim($o['alias'], "`"))) {
						$o['alias'] = trim($o['alias'], "`");
				    }
				    
				    $order_by .= $o['alias'] . ' ' . $o['direction'];
				    $order_by_coord .= $o['alias'] . ' ' . $o['direction'];
				}

				//only do order by on shards if LIMIT statement is present - otherwise sorting has
				//to be done on coordination node

				if(!empty($this->parsed['LIMIT'])) {
					$order_by = "ORDER BY {$order_by}";
				} else {
					$order_by = "";
				}
				$order_by_coord = "ORDER BY {$order_by_coord}";

				unset($this->parsed['ORDER']);
		    }

		    #ADDED GROUP BY SUPPORT HERE
		    $group_by = "";
		    $group_by_coord = "";
		    if (!empty($this->parsed['GROUP'])) {
				foreach ($this->parsed['GROUP'] as $g) {
				    if ($group_by)
						$group_by .= ",";
				    $group_by .= $g['base_expr'];

				    if ($group_by_coord)
						$group_by_coord .= ",";

				    $group_by_coord .= $g['alias'];
				}

				$group_by = " GROUP BY {$group_by}";
				$group_by_coord = " GROUP BY {$group_by_coord}";
				unset($this->parsed['GROUP']);
		    }

		    $limit = "";
		    $limit_coord = "";
		    if (!empty($this->parsed['LIMIT'])) {
				$limit .= " LIMIT {$this->parsed['LIMIT']['start']},{$this->parsed['LIMIT']['end']}";
				$limit_coord .= " LIMIT {$this->parsed['LIMIT']['start']},{$this->parsed['LIMIT']['end']}";
				unset($this->parsed['LIMIT']);
		    }

		    foreach ($this->parsed as $key => $clauses) {
				$this->errors[] = array('Unsupported query', $key . ' clause is not supported');
		    }

		    if ($this->errors) {
				return false;
		    }

		    $queries = array();
		    if (!empty($where_clauses)) {
				foreach ($where_clauses as $where) {
				    $queries[] = $select['shard_sql'] . ' ' . $where . ' ' . $group_by . ' ' . $order_by. ' ' . $limit;
				}
		    } else {
				if($order_by === "")
				    $queries[] = $select['shard_sql'] . $group_by . ' ORDER BY NULL' . ' ' . $limit;
				else 
				    $queries[] = $select['shard_sql'] . $group_by . ' ' . $order_by . ' ' . $limit;
		    }
		} else {
		    $this->errors = array('Unsupported query', 'Missing expected clause:SELECT');
		    return false;
		}

		$this->coord_sql = $select['coord_sql'] . ' ' . $group_by_coord . ' ' . $order_by_coord . ' ' . $limit_coord;
		$this->coord_odku = $select['coord_odku'];
		$this->shard_sql = $queries;
		$this->agg_key_cols = $select['group_aliases'];

		if ($this->verbose) {
		    echo "-- INPUT SQL:\n$sql\n";
		    echo "\n--PARALLEL OPTIMIZATIONS:\n";


		    if ($this->agg_key_cols) {
				echo "\n* The following projections were selected for a UNIQUE CHECK on the storage node operation:\n{$this->agg_key_cols}\n";
				if ($this->coord_odku)
				    echo "\n* storage node result set merge optimization enabled:\nON DUPLICATE KEY UPDATE\n" . join(",\n", $this->coord_odku) . "\n";
		    }
		    echo "\n";

		    foreach ($this->messages as $msg) {
				echo "-- $msg\n";
		    }

		    echo "\n-- SQL TO SEND TO SHARDS:\n";
		    print_r($this->shard_sql);

		    echo "\n-- AGGREGATION SQL:\n{$this->coord_sql}" . ( $this->agg_key_cols && $this->coord_odku ? "\nON DUPLICATE KEY UPDATE\n" . join(",\n", $this->coord_odku) . "\n" : "\n");
		} else {
		    if(is_array($sql)) {
				$this->outputString .= "-- INPUT SQL:\nArray\n";
		    } else {
				$this->outputString .= "-- INPUT SQL:\n$sql\n";
		    }
    		$this->outputString .= "\n--PARALLEL OPTIMIZATIONS:\n";

		    if ($this->agg_key_cols) {
				$this->outputString .= "\n* The following projections were selected for a UNIQUE CHECK on the storage node operation:\n{$this->agg_key_cols}\n";
				if ($this->coord_odku)
				    $this->outputString .= "\n* storage node result set merge optimization enabled:\nON DUPLICATE KEY UPDATE\n" . join(",\n", $this->coord_odku) . "\n";
			    }
			    $this->outputString .= "\n";

			    foreach ($this->messages as $msg) {
					$this->outputString .= "-- $msg\n";
			    }

			    $this->outputString .= "\n-- SQL TO SEND TO SHARDS:\n";
			    $tmpReturn = print_r($this->shard_sql, true);
			    //foreach($tmpReturn as $line)
			    $this->outputString .= $tmpReturn . "\n";

			    $this->outputString .= "\n-- AGGREGATION SQL:\n{$this->coord_sql}" . ( $this->agg_key_cols && $this->coord_odku ? "\nON DUPLICATE KEY UPDATE\n" . join(",\n", $this->coord_odku) . "\n" : "\n");
		}
		return true;
    }
}

function fix_trunc_parenth($string) {
    $open = substr_count($string, "(");
    $close = substr_count($string, ")");
    
    if($open > $close) {
	for($i=0; $i<($open-$close); $i++) {
	    $string .= ")";
	}
    } else if ($open < $close) {
	for($i=0; $i<($close-$open); $i++) {
	    $string = "(" . $string;
	}
    }
    
    return $string;    
}

?>
