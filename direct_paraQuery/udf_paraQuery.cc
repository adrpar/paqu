/*****************************************************************
 *********                UDF_PARAQUERY                 **********
 *****************************************************************
 * (C) 2012 A. Partl, eScience Group AIP - Distributed under GPL
 * 
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; version 2 of the License.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *****************************************************************
 * this function links the PHP parallel query optimiser as a UDF
 * to mysql. This uses the libPhpEmbed library to couple php to c
 * 
 *****************************************************************
 */

#include <stdio.h>
#include <string.h>
#include <mysql.h>
#include "php_stl.h"

#define PHPCODEPATH "/home/adrpar/Documents/eScience/daiquiri/incubator/spiderQueryPlanner/paraQueryFunc.php"

extern "C" {
    
    my_bool paraQuery_init( UDF_INIT* initid, UDF_ARGS* args, char* message );
    void paraQuery_deinit( UDF_INIT* initid );
    char *paraQuery( UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error );
    
}

struct paraQuery_data {
    php_stl phpEmbed;
};

char phpMessage[766] = "";
char phpError[766] = "";
char phpOutput[766] = "";

void handle_message(const char *str) {
    if(strlen(str) <= 765) {
	strcpy(phpMessage, str);
    } else {
	strncpy(phpMessage, str, 765);
	phpMessage[765] = '\0';
    }
}

void handle_error(const char *str) {
    if(strlen(str) <= 765) {
	strcpy(phpError, str);
    } else {
	strncpy(phpError, str, 765);
	phpError[765] = '\0';
    }
}
void handle_output(const char *str) {
    if(strlen(str) <= 765) {
	strcpy(phpOutput, str);
    } else {
	strncpy(phpOutput, str, 765);
	phpOutput[765] = '\0';
    }
}

my_bool paraQuery_init( UDF_INIT* initid, UDF_ARGS* args, char* message ) {
    //checking stuff to be correct
    if(args->arg_count != 3) {
	strcpy(message, "wrong number of arguments: paraQuery() requires three parameter");
	return 1;
    }
    
    if(args->arg_type[0] != STRING_RESULT) {
	strcpy(message, "paraQuery() requires the first parameter to be a string");
	return 1;
    }

    if(args->arg_type[1] != STRING_RESULT) {
	strcpy(message, "paraQuery() requires the second parameter to be a string");
	return 1;
    }

    if(args->arg_type[2] != STRING_RESULT) {
	strcpy(message, "paraQuery() requires the third parameter to be a string");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    
    //allocate memory and initialize
    paraQuery_data *memBuf = new paraQuery_data;
    
    memBuf->phpEmbed.set_error_function(handle_error);
    memBuf->phpEmbed.set_message_function(handle_message);
    memBuf->phpEmbed.set_output_function(handle_output);
    
    //load php code
    if(SUCCESS != memBuf->phpEmbed.load(PHPCODEPATH)) {
	strcpy(message, phpOutput);
	delete memBuf;
	initid->ptr = NULL;
	return 1;
    }
    
    initid->maybe_null = 0;
    initid->max_length = 17 + 31;
    initid->ptr = (char*)memBuf;
    
    return 0;
}

void paraQuery_deinit( UDF_INIT* initid ) {
    paraQuery_data *memBuf = (paraQuery_data*)initid->ptr;
    
    if(initid->ptr != NULL) {
	delete memBuf;
    }
}

char *paraQuery( UDF_INIT* initid, UDF_ARGS* args, char* result, unsigned long* length, char* is_null, char* error ) {
    char* query;
    char* resTable;
    char* logFile;

    phpError[0] = '\0';
    phpOutput[0] = '\0';
    phpMessage[0] = '\0';
    
    paraQuery_data *memBuf = (paraQuery_data*)initid->ptr;
    
    query = (char*) malloc((args->lengths[0] + 1) * sizeof (char));
    if (query == NULL) {
	strcpy(result, "Error: no memory");
	*length = strlen("Error: no memory");
	return result;
    }
    
    resTable = (char*) malloc((args->lengths[1] + 1) * sizeof (char));
    if (resTable == NULL) {
	strcpy(result, "Error: no memory");
	*length = strlen("Error: no memory");
	return result;
    }

    logFile = (char*) malloc((args->lengths[2] + 1) * sizeof (char));
    if (logFile == NULL) {
	strcpy(result, "Error: no memory");
	*length = strlen("Error: no memory");
	return result;
    }

    strcpy(query, (char*) args->args[0]);
    strcpy(resTable, (char*) args->args[1]);    
    strcpy(logFile, (char*) args->args[2]);
    
    memBuf->phpEmbed.call_long("paraQuery", "sss", query, resTable, logFile);
    
    //need to get rid of things here...
    //delete memBuf;
    //initid->ptr = NULL;
    
    if(strlen(phpMessage) > 0 && strstr(phpMessage, "preparing function paraQuery") == NULL) {
	strcpy(result, phpMessage);
	*length = strlen(phpMessage);
	return result;
    }
    
    if(strlen(phpError) > 0 && strstr(phpError, "preparing function paraQuery") == NULL) {
	strcpy(result, phpError);
	*length = strlen(phpError);
	return result;
    }

    if(strlen(phpOutput) > 0 && strstr(phpOutput, "preparing function paraQuery") == NULL) {
	strcpy(result, phpOutput);
	*length = strlen(phpOutput);
	return result;
    }

    strcpy(result, "DONE");
    *length = strlen("DONE");
    return result;
}
