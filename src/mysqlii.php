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

/**
 * @class MySQLIIException
 * @brief MySQLIIExeption handling class
 *
 * This exception is thrown by errors resulting in the MySQLII class
 */
class MySQLIIException extends Exception {
    public $mysql_error = "";
    public $mysql_errno = 0;
    
    public function __construct($mysql_error, $mysql_errno, $code=0) {
    	parent::__construct($mysql_error, $code);
    	
    	$this->mysql_error = $mysql_error;
    	$this->mysql_errno = $mysql_errno;
    }
    
    /**
     * @brief Returns a nicely formated string with the according mysql error message.
     */
    public function showMysqlError() {
    	return 'MySQL Error [' . (string)$this->mysql_errno . ']: ' . $this->mysql_error . "\n";
    }
}

/**
 * @class mysqlii
 * @brief An extension to the mysqli class that enables query timeouts
 *
 * This extension to the php mysqli default class enables the setting of a query timeout.
 * Two connections to the database are openend by the class, one for the actual query and
 * another one for monitoring the execution time of the other. If the query time exceed a
 * given value, a kill command is transmitted to the server through the second connection.
 * 
 * NOTE: THIS CLASS NEEDS php5.4 OR AN MYSQLI VERSION COMPILED WITH ASYNC QUERIES!
 */
class mysqlii extends mysqli {
    public $queryTimeout = 60000;		    //!< Query timeout in milliseconds!
    
    public $killConnection = null; 
    
    public function __construct($host = false, $user = false, $pass = false, $dbname = "", $port = false, $socket = false) {
    	if($host === false && $socket === false)
    	    $host = ini_get("mysqli.default_host");
    	if($user === false)
    	    $user = ini_get("mysqli.default_user");
    	if($pass === false)
    	    $pass = ini_get("mysqli.default_pw");
    	if($port === false)
    	    $port = ini_get("mysqli.default_port");
    	if($socket === false && $host === false)
    	    $socket = ini_get("mysqli.default_socket");
    	
            parent::__construct($host, $user, $pass, $dbname, $port, $socket);
    	
    	if($this->connect_error) {
                throw new Expection('mysqlii: Connect Error (' . $this->connect_errno . ') '
                        . $this->connect_error);
	   }

    	#initialise the connection for killing queries
    	$this->killConnection = new mysqli($host, $user, $pass, $dbname, $port, $socket);

      if ($this->killConnection->connect_error) {
          throw new Expection('mysqlii: Connect Error in Kill Connection (' . $this->killConnection->connect_errno . ') '
                  . $this->$killConnection->connect_error);
      }
    }
    
    #inspired by stackoverflow question 7582485
    /**
     * @brief Query a database with a query time managed connection
     * @param query the actual query to run
     * @param resultmode as in mysqli
     * 
     * This function will perform the given query if the connection to the DB is
     * successfully established. By running the query in asynchronuous mode it can
     * be monitored to check if a query exceeded the query time or not. If yes, the
     * kill connection will issue a kill command to the server with the approperiate
     * PID.
     */
    public function query($query, $resultmode = MYSQLI_STORE_RESULT) {
    	parent::query($query, MYSQLI_ASYNC);
    	
    	$thread_id = $this->thread_id;
    	
    	$startTime = microtime(true);
    	
    	do {
    	    $links = $errors = $reject = array($this);
    	    
    	    $poll = $this->poll($links, $errors, $reject, 0, 500000);
    	    
    	    $currTime = microtime(true);
    	    
    	    if(($currTime - $startTime) * 1000 >= $this->queryTimeout) {
    		$this->killConnection->kill($thread_id);
    		throw new MySQLIIException("Query was killed due to query timeout.", 666);
    	    }
    	} while (!$poll);

            $res = $this->reap_async_query();
    	
    	if($links[0]->errno) {
    	    throw new MySQLIIException($links[0]->error, $links[0]->errno);
    	}
    	
    	if(is_object($res)) {
    	    return $res;
    	} else {
    	    return true;
    	}
    }
    
    public function noTimeOutQuery($query, $resultmode = MYSQLI_STORE_RESULT) {
    	return $this->killConnection->query($query, $resultmode);
    }
    
    /**
     * @brief close connection to mysql db
     * 
     */
    public function close() {
    	$this->killConnection->close();
    	
    	parent::close();
    }
}


?>
