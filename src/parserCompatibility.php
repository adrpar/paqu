<?php

	error_reporting(E_ALL);

	function addAliasToAll(&$parsedTree) {
		//any subqueries?
		if(isset($parsedTree['FROM'])) {
			foreach($parsedTree['FROM'] as &$node) {
				if($node['expr_type'] === "subquery") {
					$node['sub_tree'] = addAliasToAll($node['sub_tree']);
				} else {
					if($node['alias'] == false) {
						$alias = array();
						$alias['as'] = true;
						$alias['name'] = $node['base_expr'];
						$alias['base_expr'] = $node['base_expr'];

						$node['alias'] = $alias;
					}
				}
			}
		}

		foreach($parsedTree['SELECT'] as &$node) {
			if($node['alias'] == false) {
				$alias = array();
				$alias['as'] = true;
				$alias['name'] = $node['base_expr'];

				if($node['expr_type'] == 'function') {
					//get a string representation of this
					$node['base_expr'] = getBaseExpr($node);

					$alias['name'] = str_replace(".", "__", $node['base_expr']);
		            $alias['name'] = str_replace("(", "_", $alias['name']);
		            $alias['name'] = str_replace(")", "_", $alias['name']);
		            $alias['name'] = str_replace(" ", "_", $alias['name']);
		            $alias['name'] = "_" . $alias['name'] . "_";
				} else {
					$alias['name'] = $node['base_expr'];
				}

				$alias['base_expr'] = "as `" . $alias['name'] . "`";

				$node['alias'] = $alias;
			}
		}
	}

?>
