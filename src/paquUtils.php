<?php

/*
  Copyright (c) 2012-2014, Adrian Partl @ Leibniz Inst. for Astrophysics Potsdam
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

function collectNodes($node, $expr_type) {
	$return = array();

	//is this a leave, wrap it in an array, so that the code below
	//can be kept simple
	if(array_key_exists("expr_type", $node)) {
		$node = array($node);
	} 
 
	foreach($node as $subNode) {
		if(!is_array($subNode)) {
			continue;
		}

		if(!isset($subNode['expr_type'])) {
			$return = array_merge($return, collectNodes($subNode, $expr_type));
			continue;
		}

		if(!empty($subNode['sub_tree'])) {
			$return = array_merge($return, collectNodes($subNode['sub_tree'], $expr_type));
		}

		if($subNode['expr_type'] === $expr_type) {
			$return[] = $subNode;
		}
	}
	

	return $return;
}

//this function checks if the column name and table name match, checking also
//the aliases. IF the column has no table name specified, this function will return
//TRUE!
function isColumnInTable($column, $table) {
	//check if this is a lonely '*'
	if(!isset($column['no_quotes']) && $column['base_expr'] === "*") {
		return true;
	}

	//check for alias
	if($table['alias'] !== false) {
		$columnNamePartCount = count($column['no_quotes']['parts']);

		//case: Database.Table.Column
		switch ($columnNamePartCount) {
			case 3:
				//if the column name is made up of 3 parts, it cannot refere to 
				//an alias
				break;
			case 2:
				if($column['no_quotes']['parts'][0] === $table['alias']['no_quotes']['parts'][0]) {
					return true;
				}
				break;
			default:
				break;
		}
	} 

	//if no alias is present
	$tableNamePartCount = count($table['no_quotes']['parts']);
	$columnNamePartCount = count($column['no_quotes']['parts']);

	//case: Database.Table.Column
	switch ($columnNamePartCount) {
		case 3:
			//this only has a chance, if the table name is made up of 2 entries
			if($tableNamePartCount == 2) {
				if($column['no_quotes']['parts'][0] === $table['no_quotes']['parts'][0] &&
					$column['no_quotes']['parts'][1] === $table['no_quotes']['parts'][1]) {
					return true;
				}
			}
			break;
		case 2:
			//table could be just the table or Database.Table
			if($tableNamePartCount == 2) {
				if($column['no_quotes']['parts'][0] === $table['no_quotes']['parts'][1]) {
					return true;
				}
			} else if ($tableNamePartCount == 1) {
				if($column['no_quotes']['parts'][0] === $table['no_quotes']['parts'][0]) {
					return true;
				}
			}
			break;
		case 1:
			//no table name is given, thus return true
			return true;
		default:
			break;
	}

	return false;
}

/**
 * Function that will recursively go through the branch at the function to
 * construct the escaped column name
 * @param array $inNode SQL parse tree node
 * @return string parts of the escaped function name
 */
function buildEscapedString($inNode, $addInitialPrefix = true) {
	$str = "";

    if($addInitialPrefix === true) {
	    $str = "_";
    }

    foreach ($inNode as $currNode) {
        $partStr = "";

        if (array_key_exists("sub_tree", $currNode) && $currNode["sub_tree"] !== false) {
            $partStr = buildEscapedString($currNode["sub_tree"], false);
        }

        $partStr = str_replace(".", "__", $partStr);
        $partStr = str_replace("`", "", $partStr);

        if ($currNode["expr_type"] === "aggregate_function" ||
                $currNode['expr_type'] === "function") {
            $str .= $currNode["base_expr"] . "_" . $partStr;        #last "_" already added below
        } else if ($partStr === "") {
            $str .= $currNode["base_expr"] . "_";
        } else { 
        	$str .= $partStr;
        }
    }

    return $str;
}

function getBaseExpr($node) {
	$return = "";

	if($node['expr_type'] === "function" || $node['expr_type'] === "aggregate_function") {
		//check if the function base_expression is already fully resolved or is a vanilla
		//base_expr from the parser - parser does not include the brackets
		//if this is already resolved, we need to remove everything until the first bracket
		//to retrieve the function name
		if(strpos($node['base_expr'], "(") === false) {
			$return = $node['base_expr'];
		} else {
			$tmp = explode("(", $node['base_expr']);
			$return = $tmp[0];
		}

		//aggregate base_expr just consist of the function name in upper case
		if($node['expr_type'] === "aggregate_function") {
			$return = strtoupper($return);
			return $return;
		}

		$return .= "( ";
	} else if ($node['expr_type'] === "bracket_expression") {
		$return .= "( ";
	}

	if(isset($node['sub_tree']) && $node['sub_tree'] !== false && $node['expr_type'] !== 'subquery') {
		$tmp = "";
		
		$count = 0;
		$max = count($node['sub_tree']);
		foreach($node['sub_tree'] as $subNode) {
			$tmp .= getBaseExpr($subNode);
			$count++;

			if($count < $max &&
					($node['expr_type'] === "function" || $node['expr_type'] === "aggregate_function")) {
				$tmp .= ", ";
			} else if($count < $max) {
				$tmp .= " ";
			}
		}

		//could be, that we gathered the stuff here and it was already there... then don't append
		if($tmp !== $return) {
			$return .= $tmp;
		}
	}

	if($node['expr_type'] === "function" || $node['expr_type'] === "aggregate_function" || 
				$node['expr_type'] === "bracket_expression") {
		$return .= " )";
	} else if (isColref($node) && $node['base_expr'] !== "*") {
		$start = true;
		
		foreach($node['no_quotes']['parts'] as $part) {
			if($start === false && array_key_exists("delim", $node['no_quotes'])) {
				$return .= $node['no_quotes']['delim'];
			}

			if($part === "*") {
				$return .= $part;
			} else {
				$return .= "`" . $part . "`";
			}

			$start = false;
		}
	} else if ($node['expr_type'] !== "expression" && $node['expr_type'] !== "bracket_expression") {
		$return = $node['base_expr'];
	}

	return $return;
}

//takes an array and creates an alias out of the given parts
function createAliasNode($partArray, $delim = ".") {
	$alias = array();

	$alias['as'] = true;
	$alias['no_quotes'] = array();
	$alias['no_quotes']['parts'] = array();

	if(count($partArray) > 1) {
		$alias['no_quotes']['delim'] = $delim;
	} else {
		$alias['no_quotes']['delim'] = false;
	}

	$name = "";

	foreach($partArray as $part) {
		$alias['no_quotes']['parts'][] = $part;
		if($name === "") {
			$name .= "`" . $part . "`";
		} else {
			$name .= $delim . "`" . $part . "`";
		}
	}

	$alias['name'] = $name;
	$alias['base_expr'] = "as " . $name;

	return $alias;
}

function aliasIsEqual($aliasA, $aliasB, $fuzzyMatch = false) {
	if(count($aliasA['no_quotes']) !== count($aliasB['no_quotes'])) {
		return false;
	}

	foreach($aliasA['no_quotes'] as $key => $part) {
		if($part !== $aliasB['no_quotes'][$key]) {
			if($fuzzyMatch === true) {
				$nameA = implode(".", $aliasA['no_quotes']['parts']);
				$nameB = implode(".", $aliasB['no_quotes']['parts']);

				if(strpos($nameA, $nameB) === false && strpos($nameB, $nameA) === false) {
					return false;
				}
			} else {
				return false;
			}
		}
	}

	return true;
}

//fuzzyMatch implodes the unquoted column parts and checks if A is contained in B
//or vice versa
function columnIsEqual($colA, $colB, $fuzzyMatch = false) {
	if($colA['expr_type'] !== $colB['expr_type']) {
		return false;
	}

	if(!isColref($colA) && !isColref($colB)) {
		if($colA === $colB) {
			return true;
		} else {
			return false;
		}
	}

	//if this is a "*" node, as in SELECT * FROM, then the no_quotes part is not present
	//and it does not make sense to extract anything anyways
	if(!isset($colA['no_quotes']) || !isset($colA['no_quotes'])) {
		if($colA === $colB) {
			return true;
		} else {
			return false;
		}
	}

	if(count($colA['no_quotes']) !== count($colB['no_quotes'])) {
		return false;
	}

	foreach($colA['no_quotes']['parts'] as $key => $part) {
		if($part !== $colB['no_quotes']['parts'][$key]) {
			if($fuzzyMatch === true) {
				$nameA = implode(".", $colA['no_quotes']['parts']);
				$nameB = implode(".", $colB['no_quotes']['parts']);

				if(strpos($nameA, $nameB) === false && strpos($nameB, $nameA) === false) {
					return false;
				}
			} else {
				return false;
			}
		}
	}

	return true;
}

function extractDbName($node) {
	//is this a table type or something else
	if(isTable($node)) {
		$partCounts = count($node['no_quotes']['parts']);

		//a table node
		if($partCounts > 1) {
			return $node['no_quotes']['parts'][ $partCounts - 2 ];
		} else {
			return false;
		}
	} else if(isColref($node)) {
		//if this is a "*" node, as in SELECT * FROM, then the no_quotes part is not present
		//and it does not make sense to extract anything anyways
		if(!isset($node['no_quotes'])) {
			return false;
		}

		$partCounts = count($node['no_quotes']['parts']);

		if($partCounts > 2) {
			return $node['no_quotes']['parts'][ 0 ];
		} else {
			return false;
		}
	} else {
		//don't know what to do
		return false;
	}
}

function extractTableAlias($node) {
	if((isTable($node) || isSubquery($node))
		&& isset($node['alias']['as'])) {
		$partCounts = count($node['alias']['no_quotes']['parts']);

		//a table node
		return $node['alias']['no_quotes']['parts'][ $partCounts - 1 ];
	} else if ( isColref($node) &&
				 isset($node['alias']['as']) ) {

		$partCounts = count($node['alias']['no_quotes']['parts']);

		if($partCounts > 1) {
			return $node['alias']['no_quotes']['parts'][ $partCounts - 2 ];
		} else {
			return false;
		}

	} else {
		//don't know what to do
		return false;
	}
}

function extractTableName($node) {
	//is this a table type or colref/alias?
	if(isTable($node)) {
		$partCounts = count($node['no_quotes']['parts']);
	
		//a table node
		return $node['no_quotes']['parts'][ $partCounts - 1 ];
	} else if ( isColref($node) || isset($node['as']) ) {

		//if this is a "*" node, as in SELECT * FROM, then the no_quotes part is not present
		//and it does not make sense to extract anything anyways
		if(!isset($node['no_quotes'])) {
			return false;
		}

		$partCounts = count($node['no_quotes']['parts']);

		if($partCounts > 1) {
			return $node['no_quotes']['parts'][ $partCounts - 2 ];
		} else {
			return false;
		}

	} else {
		//don't know what to do
		return false;
	}
}

function extractColumnAlias($node) {
	//is this a table type or colref/alias?
	if ( (isColref($node) || isFunction($node) || isExpression($node))  &&
				 isset($node['alias']['as']) ) {

		$partCounts = count($node['alias']['no_quotes']['parts']);

		return $node['alias']['no_quotes']['parts'][ $partCounts - 1 ];
	} else {
		//don't know what to do
		return false;
	}
}

function extractColumnName($node) {
	//is this a table type or colref/alias?
	if ( isColref($node) || isset($node['as']) ) {

		//if this is a "*" node, as in SELECT * FROM, then the no_quotes part is not present
		//and it does not make sense to extract anything anyways
		if(!isset($node['no_quotes'])) {
			return false;
		}

		$partCounts = count($node['no_quotes']['parts']);

		return $node['no_quotes']['parts'][ $partCounts - 1 ];
	} else {
		//don't know what to do
		return false;
	}
}

function hasAlias($node) {
	if(isset($node['alias']) && $node['alias'] !== false) {
		return true;
	} else {
		return false;
	}
}

function hasSubtree($node) {
	if(isset($node['sub_tree']) && $node['sub_tree'] !== false) {
		return true;
	} else {
		return false;
	}
}

function isSubquery($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'subquery') {
		return true;
	} else {
		return false;
	}
}

function isOperator($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'operator') {
		return true;
	} else {
		return false;
	}
}

function isColref($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'colref') {
		return true;
	} else {
		return false;
	}
}

function isReserved($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'reserved') {
		return true;
	} else {
		return false;
	}
}

function isTable($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'table') {
		return true;
	} else {
		return false;
	}
}

function isFunction($node) {
	if(isset($node['expr_type']) && ($node['expr_type'] === 'function' || $node['expr_type'] === 'aggregate_function')) {
		return true;
	} else {
		return false;
	}
}

function isExpression($node) {
	if(isset($node['expr_type']) && $node['expr_type'] === 'expression') {
		return true;
	} else {
		return false;
	}
}

function implodeNoQuotes($node) {
	if(array_key_exists("delim", $node)) {
		if($node['delim'] !== false) {
			return implode($node['delim'], $node['parts']);
		} else {
			return implode("", $node['parts']);
		}
	} else {
		return implode(".", $node['parts']);
	}

}

function rewriteTableNameInSubqueries(&$subTree, $toThisTableName, $exceptThisTableName) {
	foreach($subTree as &$node) {
		if(hasSubtree($node)) {
			rewriteTableNameInSubqueries($node['sub_tree'], $toThisTableName, $exceptThisTableName);
		}

		if(isColref($node)) {
			$currTable = extractTableName($node);

			if($currTable !== $exceptThisTableName) {
				//if there is a * in the subquery somewhere
				if(!array_key_exists("no_quotes", $node)) {
					$node['no_quotes'] = array("delim" => ".", "parts" => array());
				}

				$node['no_quotes']['parts'] = array($toThisTableName, implodeNoQuotes($node['no_quotes']));
				$node['base_expr'] = getBaseExpr($node);
			}
		}
	}
}

function setNoQuotes(&$node, array $parts, $delim = false) {
	if($delim !== false) {
		$node['no_quotes']['delim'] = $delim;
	} else if (count($parts) > 1) {
		//apply a default delimiter
		$node['no_quotes']['delim'] = ".";
	}

	$node['no_quotes']['parts'] = $parts;

	$node['base_expr'] = getBaseExpr($node);
}

?>
