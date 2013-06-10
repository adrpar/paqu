/*****************************************************************
 ********            MYSQL_DAEMON_NODEJOBMON               *******
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
 * mysql daemon that will query the show processlist of the head node
 * and check if any jobs are killed. using query id in the query
 * comment (written there by the parallel query optimiser) to compare
 * with local processlist and killing any query marked as killed 
 * on the head node
 * 
 * Heavily inspired by the handlersocket plugin
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

char* headNodeConnectionString = NULL;
long intervalSec;
THD * thd;
#if MYSQL_VERSION_ID >= 50505
mysql_mutex_t paquKillMutex = PTHREAD_MUTEX_INITIALIZER;
mysql_cond_t paquKillCond = PTHREAD_COND_INITIALIZER;
#else
pthread_mutex_t paquKillMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t paquKillCond = PTHREAD_COND_INITIALIZER;
#endif

char* connectStrCpy = NULL;
char* rmtUserName = NULL;
char* rmtPasswd = NULL;
char* rmtPort = NULL;
char* rmtServer = NULL;

static pthread_t daemon_thread;

extern I_List<THD> threads;
extern struct system_variables global_system_variables;

class paquThread_info {
public:

    paquThread_info() {
	user = NULL;
	db = NULL;
	query = NULL;
    }

    virtual ~paquThread_info() {
	if (user)
	    free(user);
	if (db)
	    free(db);
	if (query)
	    free(query);
    }

    ulong thd_id;
    ulong paqu_qid;
    char* user;
    my_bool killed;
    char* db;
    char* query;
    THD * thd;
};

class thd_list {
public:

    thd_list() {
	array = NULL;
    }

    thd_list(int newLen) {
	array = (paquThread_info**) malloc(newLen * sizeof (paquThread_info));
	len = newLen;
    }

    virtual ~thd_list() {
	if (array != NULL) {
	    for (int i = 0; i < len; i++)
		delete array[i];

	    free(array);
	}
    }

    paquThread_info ** array;
    int len;
};

void getLocalProcesslist(thd_list **toThisList);
int getRemoteProcesslist(MYSQL *mysql, thd_list **toThisList);
int parseConnectStr(char* connectStr, int len);
int parsePaQuQID(char* query);
int connectToHead(MYSQL **mysql);
int init_thread();
int deinit_thread();
void sql_kill(THD *thd, ulong id, bool only_kill_query);

void headLink_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save);

MYSQL_SYSVAR_STR(headNodeConnection, headNodeConnectionString, PLUGIN_VAR_RQCMDARG,
	"MySQL connection string to the spider head node", NULL, headLink_update, "mysql://root:spider@127.0.0.1:3306");
MYSQL_SYSVAR_LONG(intervalSec, intervalSec, PLUGIN_VAR_RQCMDARG,
	"PaQu polling frequency of the head node", NULL, NULL, 5, 1, 10000000, 1);


struct st_mysql_sys_var* vars_system[] = {
    MYSQL_SYSVAR(headNodeConnection),
    MYSQL_SYSVAR(intervalSec),
    NULL
};

pthread_handler_t paqu_daemon(void* p) {
    char buffer[1024];
    char * oldHeadLink = NULL;
    MYSQL * mysql = NULL;

    init_thread(&thd, "Running PaQu Kill daemon");

    connectToHead(&mysql);

    oldHeadLink = strdup(headNodeConnectionString);

    int tmp;

    while (thd->killed == 0) {
		if (strcmp(oldHeadLink, headNodeConnectionString) != 0) {
		    free(oldHeadLink);
		    connectToHead(&mysql);
		    oldHeadLink = strdup(headNodeConnectionString);
		}

		if (mysql != NULL) {
		    thd_list *thdList;

		    thd_list *thdRemoteList;

		    paquThread_info * currThd;

		    getLocalProcesslist(&thdList);
		    if (!getRemoteProcesslist(mysql, &thdRemoteList)) {
			//loop through all processes on remote to find a killed query

			for (int i = 0; i < thdRemoteList->len; i++) {
			    currThd = thdRemoteList->array[i];
			    if (currThd->paqu_qid != -1) {
					if (currThd->killed == TRUE) {
					    //loop through all local processes to find query with matching paqu_qid

					    paquThread_info * currLocThd;
					    for (int j = 0; j < thdList->len; j++) {
							currLocThd = thdList->array[j];
							if (currLocThd->paqu_qid == currThd->paqu_qid && currLocThd->paqu_qid != -1) {
							    //KILL THIS THREAD!
							    sql_kill(currLocThd->thd, currLocThd->thd_id, 1);
							}
					    }
					}
			    }
			}
	    }

#ifdef __JOBMON_DEBUG__
	    paquThread_info * currLocThd;
	    for (int i = 0; i < thdList->len; i++) {
		currLocThd = thdList->array[i];
		fprintf(stderr, "id: %i user: %s db: %s query %s\n", currLocThd->thd_id,
			currLocThd->user,
			currLocThd->db,
			currLocThd->query);
	    }
#endif
	    delete thdList;


#ifdef __JOBMON_DEBUG__
	    fprintf(stderr, "Remote list\n");
#endif

#ifdef __JOBMON_DEBUG__
	    for (int i = 0; i < thdRemoteList->len; i++) {
			currThd = thdRemoteList->array[i];
			fprintf(stderr, "id: %i user: %s db: %s query %s\n", currThd->paqu_qid,
				currThd->user,
				currThd->db,
				currThd->query);
		    }
#endif
		    delete thdRemoteList;
		}

		//get the time for sleep
		for (int i = 0; i < intervalSec; i++) {
		    if (thd->killed != 0)
				break;

		    struct timespec deltaTime = {0, 0};
		    deltaTime.tv_sec = time(NULL) + 1;

#if MYSQL_VERSION_ID >= 50505		
		    mysql_mutex_lock(&paquKillMutex);
		    tmp = mysql_cond_timedwait(&paquKillCond, &paquKillMutex, &deltaTime);
		    mysql_mutex_unlock(&paquKillMutex);
#else
		    pthread_mutex_lock(&paquKillMutex);
		    tmp = pthread_cond_timedwait(&paquKillCond, &paquKillMutex, &deltaTime);
		    pthread_mutex_unlock(&paquKillMutex);
#endif

		    if (tmp != ETIMEDOUT)
				break;
		}

    }

    if (mysql)
	mysql_close(mysql);

    deinit_thread(&thd);
    pthread_exit(0);
    return NULL;
}

int parseConnectStr(char* connectStr, int len) {
    char* strStart;

    if (connectStr == NULL)
		return 1;

    if (connectStrCpy != NULL)
		free(connectStrCpy);

    connectStrCpy = strndup(connectStr, len);

    strStart = strstr(connectStrCpy, "://") + 3;
    if (strStart == NULL && (!strchr(connectStrCpy, '@')) &&
	    strstr(connectStrCpy, "mysql") != connectStrCpy)
	return 1;

    //strip any possible table names
    char* tmpTblName;
    if (tmpTblName = strchr(strStart, '/'))
	strStart[tmpTblName - strStart] = '\0';
    tmpTblName++;

    rmtUserName = strStart;
    rmtServer = strchr(strStart, '@') + 1;
    strStart[rmtServer - 1 - strStart] = '\0';
    rmtPort = strchr(rmtServer, ':') + 1;
    rmtServer[rmtPort - 1 - rmtServer] = '\0';
    rmtPasswd = strchr(rmtUserName, ':') + 1;
    rmtUserName[rmtPasswd - 1 - rmtUserName] = '\0';

    return 0;
}

int parsePaQuQID(char* query) {
    char PaQuQID[64];
    char * QIDstart;
    char * QIDend;

    if (query == NULL)
	return -1;

    if (QIDstart = strstr(query, "/* PaQu: QID ")) {
	QIDstart += 13;
	QIDend = strstr(QIDstart, "*/");

	if (QIDend == NULL)
	    return -1;

	strncpy(PaQuQID, QIDstart, (QIDend - QIDstart));
	PaQuQID[(QIDend - QIDstart)] = '\0';

	return atoi(PaQuQID);
    }

    return -1;
}

int getRemoteProcesslist(MYSQL *mysql, thd_list **toThisList) {

    if (mysql != NULL) {
	if (mysql_real_query(mysql, "show full processlist", strlen("show full processlist")) == 0) {
	    MYSQL_RES *result = mysql_store_result(mysql);

	    MYSQL_ROW row;

	    ulonglong thdCount = mysql_num_rows(result);

	    *toThisList = new thd_list(thdCount);

	    thdCount = 0;
	    while ((row = mysql_fetch_row(result))) {
		paquThread_info * currThdInfo = new paquThread_info;

		currThdInfo->thd_id = atoi(row[0]);
		if (row[1]) {
		    currThdInfo->user = strdup(row[1]);
		} else {
		    currThdInfo->user = strdup("(null)");
		}
		if (row[3]) {
		    currThdInfo->db = strdup(row[3]);
		} else {
		    currThdInfo->db = strdup("(null)");
		}

		if (row[4]) {
		    if (strcmp(row[4], "Killed") == 0)
			currThdInfo->killed = TRUE;
		    else
			currThdInfo->killed = FALSE;
		}

		if (row[7]) {
		    currThdInfo->query = strdup(row[7]);
		} else {
		    currThdInfo->query = strdup("(null)");
		}

		currThdInfo->paqu_qid = parsePaQuQID(currThdInfo->query);

		(*toThisList)->array[thdCount] = currThdInfo;

		thdCount++;
	    }

	    mysql_free_result(result);

	    return 0;
	} else {
	    fprintf(stderr, "PaQu - node_jobMon ErrNr %u: %s\n", mysql_errno(mysql), mysql_error(mysql));
	    return 1;
	}
    }

    return 1;
}

int connectToHead(MYSQL **mysql) {
    if (parseConnectStr(headNodeConnectionString, strlen(headNodeConnectionString)))
		return 1;

    if (*mysql)
	mysql_close(*mysql);

    *mysql = mysql_init(NULL);
    if (*mysql == NULL)
	return 1;

    if (mysql_options(*mysql, MYSQL_OPT_COMPRESS, NULL)) {
	fprintf(stderr, "ErrNr %u: %s\n", mysql_errno(*mysql), mysql_error(*mysql));
	mysql_close(*mysql);
	fprintf(stderr, "ERROR: node_jobMon: unable to set compression - unable to connect to head node\n");
	*mysql = NULL;
	return 1;
    }

    my_bool value = true;
    if (mysql_options(*mysql, MYSQL_OPT_RECONNECT, &value)) {
	fprintf(stderr, "ErrNr %u: %s\n", mysql_errno(*mysql), mysql_error(*mysql));
	mysql_close(*mysql);
	fprintf(stderr, "ERROR: node_jobMon: unable to set reconnect - unable to connect to head node\n");
	*mysql = NULL;
	return 1;
    }

    if (!mysql_real_connect(*mysql, rmtServer, rmtUserName, rmtPasswd, "", atoi(rmtPort), "", 0)) {
	fprintf(stderr, "ErrNr %u: %s\n", mysql_errno(*mysql), mysql_error(*mysql));
	mysql_close(*mysql);
	fprintf(stderr, "ERROR: node_jobMon: unable to connect to head node\n");
	*mysql = NULL;
	return 1;
    }

    return 0;
}

//gets local process list and adds them to a I_List 

void getLocalProcesslist(thd_list **toThisList) {
    //TODO: check if mutexes are needed here

    mysql_mutex_lock(&LOCK_thread_count);

#if MYSQL_VERSION_ID < 50606
    THD* currThd;
    I_List_iterator<THD> thdIterCnt(threads);
#endif

    int thdCount = 0;

#if MYSQL_VERSION_ID < 50606
    while ((currThd = thdIterCnt++))
		thdCount++;
#else
	thdCount = get_thread_count();
#endif

    *toThisList = new thd_list(thdCount);


#if MYSQL_VERSION_ID < 50606
    I_List_iterator<THD> thdIter(threads);
#else
    Thread_iterator thdIter= global_thread_list_begin();
    Thread_iterator thdIterEnd= global_thread_list_end();
#endif

    thdCount = 0;

#if MYSQL_VERSION_ID < 50606
    while ((currThd = thdIter++)) {
#else
    for (; thdIter != thdIterEnd; ++thdIter) {
    	THD* currThd = *thdIter;
#endif
		paquThread_info * currThdInfo = new paquThread_info;

		currThdInfo->thd = currThd;
		currThdInfo->thd_id = currThd->thread_id;
		if (currThd->security_ctx)
		    currThdInfo->user = strdup(currThd->security_ctx->user ? currThd->security_ctx->user :
			(currThd->system_thread ?
			"system user" : "unauthenticated user"));
		else
		    currThdInfo->user = strdup("system user");
		if (currThd->db)
		    currThdInfo->db = strdup(currThd->db);
		else
		    currThdInfo->db = NULL;

		currThdInfo->killed = (my_bool) (currThd->killed == THD::KILL_CONNECTION ? TRUE : FALSE);

		char* query = NULL;
		if (currThd->query()) {
		    uint length = global_system_variables.max_allowed_packet;
		    CSET_STRING tmpStr = CSET_STRING(currThd->query(), currThd->query() ? length : 0, currThd->query_charset());
		    currThdInfo->query = strdup(tmpStr.str());

		} else {
		    currThdInfo->query = strdup("(null)");
		}

		currThdInfo->paqu_qid = parsePaQuQID(currThdInfo->query);

		(*toThisList)->array[thdCount] = currThdInfo;

		thdCount++;
    }

    mysql_mutex_unlock(&LOCK_thread_count);
}

static int paqu_plugin_init(void* p) {
    pthread_attr_t attr;
    char daemon_filename[FN_REFLEN];
    char buffer[1024];
    char time_str[20];

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "PaQu Kill daemon started at %s\n", time_str);

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&daemon_thread, &attr, paqu_daemon, NULL) != 0) {
		fprintf(stderr, "PaQu - node_jobMon ERROR: Could not create thread!\n");
		return 1;
    }

    return 0;
}

static int paqu_plugin_deinit(void* p) {
    char buffer[1024];
    char time_str[20];

    thd->killed = THD::KILL_CONNECTION;
#if MYSQL_VERSION_ID >= 50505
    mysql_cond_signal(&paquKillCond);
#else
    pthread_cond_signal(&paquKillCond);
#endif
    pthread_join(daemon_thread, NULL);

    get_date(time_str, GETDATE_DATE_TIME, 0);
    fprintf(stderr, "PaQu Kill daemon stopped at %s\n", time_str);

    if (connectStrCpy != NULL)
		my_free(connectStrCpy);

    return 0;
}

void headLink_update(MYSQL_THD thd, struct st_mysql_sys_var *var, void *var_ptr, const void *save) {
    *(char**) var_ptr = my_strdup(*(const char**) save, MYF(0));
}

struct st_mysql_daemon vars_plugin_info = {MYSQL_DAEMON_INTERFACE_VERSION};

mysql_declare_plugin(vars) {
    MYSQL_DAEMON_PLUGIN,
	    &vars_plugin_info,
	    "PaQu",
	    "Adrian M. Partl",
	    "Monitoring of spider engine and PaQu jobs",
	    PLUGIN_LICENSE_GPL,
	    paqu_plugin_init,
	    paqu_plugin_deinit,
	    0x0100,
	    NULL,
	    vars_system,
	    NULL
}
mysql_declare_plugin_end;
