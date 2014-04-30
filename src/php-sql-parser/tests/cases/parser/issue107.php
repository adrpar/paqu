<?php
/**
 * issue107.php
 *
 * Test case for PHPSQLParser.
 *
 * PHP version 5
 *
 * LICENSE:
 * Copyright (c) 2010-2014 Justin Swanhart and André Rothe
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * @author    André Rothe <andre.rothe@phosco.info>
 * @copyright 2010-2014 Justin Swanhart and André Rothe
 * @license   http://www.debian.org/misc/bsd.license  BSD License (3 Clause)
 * @version   SVN: $Id: issue107.php 1346 2014-04-15 13:46:14Z phosco@gmx.de $
 * 
 */
namespace PHPSQLParser;
use PHPSQLParser\utils\ExpressionType;

require_once dirname(__FILE__) . '/../../../src/PHPSQLParser/PHPSQLParser.php';
require_once dirname(__FILE__) . '/../../../src/PHPSQLParser/utils/ExpressionType.php';
require_once dirname(__FILE__) . '/../../test-more.php';

try {
    $sql = "CREATE TABLE IF NOT EXISTS `engine4_urdemo_causebug` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `extra` int(11)  NOT NULL DEFAULT 56,
  PRIMARY KEY (`id`),
  INDEX client_idx (id)
) ENGINE=InnoDB;";
    $parser = new PHPSQLParser($sql);
    $p = $parser->parsed;
} catch (Exception $e) {
    $p = array();
}
ok($p['TABLE']['create-def']['sub_tree'][1]['sub_tree'][1]['sub_tree'][5]['expr_type'] === ExpressionType::DEF_VALUE,
        'column definition with DEFAULT value');

?>
