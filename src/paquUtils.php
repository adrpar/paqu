<?php

error_reporting(E_ALL);

function collectNodes($node, $expr_type) {
	$return = array();

	//is this a leave, wrap it in an array, so that the code below
	//can be kept simple
	if(array_key_exists("expr_type", $node)) {
		$node = array($node);
	} 

	foreach($node as $subNode) {
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
function buildEscapedString($inNode) {
    $str = "";

    foreach ($inNode as $currNode) {
        $partStr = "";

        if (array_key_exists("sub_tree", $currNode) && $currNode["sub_tree"] !== false) {
            $partStr = buildEscapedString($currNode["sub_tree"]);
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
	$return = $node['base_expr'];

	if($node['expr_type'] === "function" || $node['expr_type'] === "aggregate_function") {
		$return .= "(";
	}

	if(isset($node['sub_tree']) && $node['sub_tree'] !== false) {
		$tmp = "";
		
		foreach($node['sub_tree'] as $subNode) {
			$tmp .= getBaseExpr($subNode);
		}

		//could be, that we gathered the stuff here and it was already there... then don't append
		if($tmp !== $return) {
			$return .= $tmp;
		}
	}

	if($node['expr_type'] === "function" || $node['expr_type'] === "aggregate_function") {
		$return .= ")";
	}

	return $return;
}

?>
