/*****************************************************************
 * *******                 UDF_SUM_OF_SQUARES              *******
 *****************************************************************
 * (C) 2012 A. Partl, eScience Group AIP - Distributed under GPL
 * 
 * this function will calculate the sum of squares using the 
 * Welford one-pass algorithm
 * 
 * See Welford 1962 or wikipedia for further information * 
 * 
 *****************************************************************
 */

#include <stdio.h>
#include <string.h>
#include <mysql.h>

extern "C" {
    
    my_bool sum_of_squares_init( UDF_INIT* initid, UDF_ARGS* args, char* message );
    void sum_of_squares_deinit( UDF_INIT* initid );
    void sum_of_squares_clear( UDF_INIT* initid, char* is_null, char* error );
    void sum_of_squares_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error );
    double sum_of_squares( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error );
    
}

struct squared_data {
    unsigned long long count;
    double mean;
    double M2;
};

my_bool sum_of_squares_init( UDF_INIT* initid, UDF_ARGS* args, char* message ) {
    //checking stuff to be correct
    if(args->arg_count != 1) {
	strcpy(message, "wrong number of arguments: sum_of_squares() requires one parameter");
	return 1;
    }
    
    if(args->arg_type[0] != REAL_RESULT) {
	strcpy(message, "sum_of_squares() requires a real/float/double parameter");
	return 1;
    }
    
    //no limits on number of decimals
    initid->decimals = 31;
    
    //allocate memory and initialize
    squared_data *memBuf = new squared_data;
    memBuf->count = 0;
    memBuf->mean = 0.0;
    memBuf->M2 = 0.0;
    
    initid->maybe_null = 1;
    initid->max_length = 17 + 31;
    initid->ptr = (char*)memBuf;
    
    return 0;
}

void sum_of_squares_deinit( UDF_INIT* initid ) {
    squared_data *memBuf = (squared_data*)initid->ptr;
    
    if(memBuf != NULL) {
	delete memBuf;
    }
}

void sum_of_squares_clear( UDF_INIT* initid, char* is_null, char* error ) {
     squared_data *memBuf = (squared_data*)initid->ptr;
    
    //reset buffer
    memBuf->count = 0;
    memBuf->mean = 0.0;
    memBuf->M2 = 0.0;
   
    *is_null = 0;
}

void sum_of_squares_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error ) {
    //this is the crucial part of Welfords alg (see wikipedia en.wikipedia.org/wiki/Algorithms_for_calculating_variance)
    
    if(args->args[0] != NULL) {
	squared_data *memBuf = (squared_data*)initid->ptr;
	double delta;
	
	memBuf->count++;
	delta = *(double*)args->args[0] - memBuf->mean;
	memBuf->mean += delta / (double)memBuf->count;
	memBuf->M2 += delta * (*(double*)args->args[0] - memBuf->mean);
    }
}

double sum_of_squares( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error ) {
    squared_data *memBuf = (squared_data*)initid->ptr;
    
    //check if we have valid data...
    if(memBuf->count == 0 || *is_error != 0) {
	*is_null = 1;
	return 0.0;
    }
    
    return memBuf->M2;
}