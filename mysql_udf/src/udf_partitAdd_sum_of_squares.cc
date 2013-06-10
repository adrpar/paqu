/*****************************************************************
 * *******          UDF_PARTITADD_SUM_OF_SQUARES           *******
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
 * this function will calculate the sum of squares from partitioned
 * results of the Welford algorithm. this uses the Chan et al. 
 * algorithm
 * 
 * partitadd_sum_of_sqares(PART_M2, PART_MEAN, PART_COUNT)
 * - par1: PART_M2:	the sum of squares for a given partition
 * - par2: PART_MEAN:	the mean for a given partition
 * - par3: PART_COUNT:	the number of elements in the partition
 * 
 * See Welford 1962, Chan et al. 1979 or wikipedia for further 
 * information
 * 
 *****************************************************************
 */

#include <stdio.h>
#include <string.h>
#include <vector>
#include <cmath>
#include <mysql.h>

//this allows to exactly define the buffer size according to the number of available
//shards
#ifndef NSHARDS
#define NSHARDS 10
#endif

extern "C" {
    
    my_bool partitAdd_sum_of_squares_init( UDF_INIT* initid, UDF_ARGS* args, char* message );
    void partitAdd_sum_of_squares_deinit( UDF_INIT* initid );
    void partitAdd_sum_of_squares_clear( UDF_INIT* initid, char* is_null, char* error );
    void partitAdd_sum_of_squares_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error );
    double partitAdd_sum_of_squares( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error );
    
}

struct partitSquaredSum_data {
    std::vector<unsigned long long> count;
    std::vector<double> mean;
    std::vector<double> M2;
};

my_bool partitAdd_sum_of_squares_init( UDF_INIT* initid, UDF_ARGS* args, char* message ) {
    //checking stuff to be correct
    if(args->arg_count != 3) {
	strcpy(message, "wrong number of arguments: partitadd_sum_of_squares() requires three parameter");
	return 1;
    }
    
    if(args->arg_type[0] != REAL_RESULT) {
	strcpy(message, "partitadd_sum_of_squares() requires the first parameter to be real/float/double");
	return 1;
    }

    if(args->arg_type[1] != REAL_RESULT) {
	strcpy(message, "partitadd_sum_of_squares() requires the second parameter to be real/float/double");
	return 1;
    }

    if(args->arg_type[2] != INT_RESULT) {
	strcpy(message, "partitadd_sum_of_squares() requires the third parameter to be int/long");
	return 1;
    }

    //no limits on number of decimals
    initid->decimals = 31;
    
    //allocate memory and initialize
    partitSquaredSum_data *memBuf = new partitSquaredSum_data;
    memBuf->count.reserve(NSHARDS);
    memBuf->mean.reserve(NSHARDS);
    memBuf->M2.reserve(NSHARDS);
    memBuf->count.clear();
    memBuf->mean.clear();
    memBuf->M2.clear();
    
    initid->maybe_null = 1;
    initid->max_length = 17 + 31;
    initid->ptr = (char*)memBuf;
    
    return 0;
}

void partitAdd_sum_of_squares_deinit( UDF_INIT* initid ) {
    partitSquaredSum_data *memBuf = (partitSquaredSum_data*)initid->ptr;
    
    if(memBuf != NULL) {
	delete memBuf;
    }
}

void partitAdd_sum_of_squares_clear( UDF_INIT* initid, char* is_null, char* error ) {
    partitSquaredSum_data *memBuf = (partitSquaredSum_data*)initid->ptr;
    
    //reset buffer
    memBuf->count.clear();
    memBuf->mean.clear();
    memBuf->M2.clear();
   
    *is_null = 0;
}

void partitAdd_sum_of_squares_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error ) {
    //this just stores the two values M2 and count of the given partition in the memory for later handling
    
    if(args->args[0] != NULL && args->args[1] != NULL && args->args[2] != NULL) {
	partitSquaredSum_data *memBuf = (partitSquaredSum_data*)initid->ptr;
	
	memBuf->M2.push_back(*(double*)args->args[0]);
	memBuf->mean.push_back(*(double*)args->args[1]);
	memBuf->count.push_back(*(long long*)args->args[2]);
    }
}

double partitAdd_sum_of_squares( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error ) {
    partitSquaredSum_data *memBuf = (partitSquaredSum_data*)initid->ptr;
    
    //check if we have valid data...
    if(memBuf->M2.size() == 0 || memBuf->mean.size() == 0 || 
	    memBuf->count.size() == 0 || *is_error != 0) {
	*is_null = 1;
	return 0.0;
    }
    
    //calculate the sum of the partitions using a tree
    int numOfCalcs = log((double)memBuf->count.size()) / log(2.0) + 1;
    
    int currCount = memBuf->count.size();
    
    for(int i=0; i<numOfCalcs; i++) {

	if(currCount == 1)
	    break;
	
	int currIndex = 0;
	
	for(int j=0; j<currCount; j+=2) {
	    if(j < currCount-1) {
		//always processing two at a time
		unsigned long long sumElements = memBuf->count[j] + memBuf->count[j+1];
		double delta = memBuf->mean[j+1] - memBuf->mean[j];
		double newMean = (memBuf->count[j] * memBuf->mean[j] + memBuf->count[j+1] * memBuf->mean[j+1]) / (double)sumElements;
		double newM2 = memBuf->M2[j] + memBuf->M2[j+1] + delta * delta * (memBuf->count[j] * memBuf->count[j+1]) / (double)sumElements;
		
		memBuf->count[currIndex] = sumElements;
		memBuf->mean[currIndex] = newMean;
		memBuf->M2[currIndex] = newM2;
	    } else {
		//handle special case when there is only one left
		
		memBuf->count[currIndex] = memBuf->count[j];
		memBuf->mean[currIndex] = memBuf->mean[j];
		memBuf->M2[currIndex] = memBuf->M2[j];
	    }

	    currIndex++;
	}

	currCount = currIndex;
    }
    
    return memBuf->M2[0];
}