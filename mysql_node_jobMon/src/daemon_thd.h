/*****************************************************************
 ********                  daemon_thd                      *******
 *****************************************************************
 * (C) 2012 A. Partl, eScience Group AIP - Distributed under GPL
 * 
 * common functions for thread handling
 * 
 *****************************************************************
 */

#ifndef __MYSQL_DAEMON_THD__
#define __MYSQL_DAEMON_THD__

#define MYSQL_SERVER 1

#include <sql_class.h>

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

int init_thread(THD ** thd, const char * threadInfo);
int deinit_thread(THD ** thd);
void sql_kill(THD *thd, ulong id, bool only_kill_query);

#endif