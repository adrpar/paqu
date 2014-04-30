<?php
/**
 * issue56.php
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
 * @version   SVN: $Id: issue56.php 1346 2014-04-15 13:46:14Z phosco@gmx.de $
 * 
 */
namespace PHPSQLParser;
require_once dirname(__FILE__) . '/../../../src/PHPSQLParser/PHPSQLParser.php';
require_once dirname(__FILE__) . '/../../test-more.php';

// optimizer/index hints
// TODO: not solved
$parser = new PHPSQLParser();
$sql = "insert /* +APPEND */ into TableName (Col1,col2) values(1,'pol')";
$parser->parse($sql, true);
$p = $parser->parsed;
$expected = getExpectedValue(dirname(__FILE__), 'issue56a.serialized');
eq_array($p, $expected, 'optimizer hint within INSERT');

// optimizer/index hints
// TODO: not solved
$parser = new PHPSQLParser();
$sql = "insert /* a comment -- haha */ into TableName (Col1,col2) values(1,'pol')";
$parser->parse($sql, true);
$p = $parser->parsed;
$expected = getExpectedValue(dirname(__FILE__), 'issue56a.serialized');
eq_array($p, $expected, 'multiline comment with inline comment inside');

// inline comment
// TODO: not solved
$sql = "SELECT acol -- an inline comment
FROM --another comment
table
WHERE x = 1";
$parser->parse($sql, true);
$p = $parser->parsed;
$expected = getExpectedValue(dirname(__FILE__), 'issue56b.serialized');
eq_array($p, $expected, 'inline comment should not fail, issue 56');

// inline comment
// TODO: not solved
$sql = "SELECT acol -- an /*inline comment
FROM --another */comment
table
WHERE x = 1";
$parser->parse($sql, true);
$p = $parser->parsed;
$expected = getExpectedValue(dirname(__FILE__), 'issue56b.serialized');
eq_array($p, $expected, 'inline comment with multiline comment inside');

?>
