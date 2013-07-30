/*****************************************************************
 ********                  daemon_thd                      *******
 *****************************************************************
 * (C) 2012 A. Partl, eScience Group AIP - Distributed under GPL
 * 
 * common functions for thread handling
 * 
 *****************************************************************
 */

#define MYSQL_SERVER 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sql_priv.h"
#include <my_global.h>
#include <my_sys.h>
#include <mysql_version.h>
#include <sql_class.h>
#include <mysql/plugin.h>
#include <mysql.h>
#include <sql_parse.h>
#include "daemon_thd.h"

#if MYSQL_VERSION_ID >= 50606
#include <global_threads.h>
#endif

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
extern uint kill_one_thread(THD *thd, ulong id, killed_state kill_signal);
#endif

int init_thread(THD ** thd, const char * threadInfo) {
    THD *newThd;
    my_thread_init();
    newThd = new THD;
    *thd = newThd;

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif
    (*thd)->thread_id = thread_id++;
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    (*thd)->thread_stack = (char*) &newThd;
    (*thd)->store_globals();
    (*thd)->system_thread = static_cast<enum_thread_type> (1 << 30UL);

    const NET v = {0};
    (*thd)->net = v;

    thd_proc_info((*thd), threadInfo);

    //(*thd)->security_ctx = NULL;
    (*thd)->db = NULL;

#if MYSQL_VERSION_ID < 50605
    (*thd)->start_time = my_time(0);
#else
    my_micro_time_to_timeval(my_micro_time(), &(*thd)->start_time);
#endif


    (*thd)->real_id = pthread_self();
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
    (*thd)->killed = NOT_KILLED;
#else
    (*thd)->killed = THD::NOT_KILLED;
#endif
#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_lock(&LOCK_thread_count);
#else
    pthread_mutex_lock(&LOCK_thread_count);
#endif

#if MYSQL_VERSION_ID < 50606
    threads.append(*thd);
    ++thread_count;
#else

    add_global_thread(*thd);

#endif

#if MYSQL_VERSION_ID >= 50505
    mysql_mutex_unlock(&LOCK_thread_count);
#else
    pthread_mutex_unlock(&LOCK_thread_count);
#endif

    return 0;
}

int deinit_thread(THD ** thd) {
    if (thd != NULL && *thd != NULL) {

#if MYSQL_VERSION_ID >= 50505
	mysql_mutex_lock(&LOCK_thread_count);
#else
	pthread_mutex_lock(&LOCK_thread_count);
#endif

#if MYSQL_VERSION_ID < 50606
    (*thd)->unlink();
    delete *thd;
	--thread_count;
#else
    remove_global_thread((*thd));
#endif

    *thd = NULL;

#if MYSQL_VERSION_ID >= 50505
	mysql_cond_signal(&COND_thread_count);
	mysql_mutex_unlock(&LOCK_thread_count);
#else
	pthread_cond_signal(&COND_thread_count);
	pthread_mutex_unlock(&LOCK_thread_count);
#endif
	my_pthread_setspecific_ptr(THR_THD, 0);
	my_thread_end();
    }
    
    return 0;
}

/*                                                                                                                    * kills a thread and sends response
 * (shamelessly copy pasted from mysql source sql_parse.cc)
 *
 * SYNOPSIS
 *	sql_kill()
 *	thd                 Thread class
 *	id                  Thread id
 *	only_kill_query     Should it kill the query or the connection
 */

void sql_kill(THD *thd, ulong id, bool only_kill_query) {
    uint error;
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50500
    if (!(error = kill_one_thread(thd, id, (killed_state)only_kill_query))) {
#else
    if (!(error = kill_one_thread(thd, id, (THD::killed_state)only_kill_query))) {
#endif
        if (!thd->killed)
            my_ok(thd);
        } else
            my_error(error, MYF(0), id);
}