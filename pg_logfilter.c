/* -------------------------------------------------------------------------
 *
 * pg_logfilter.c
 *
 * Suppress log line using given patterns
 *
 * author ( ckh0618@gmail.com )
 * -------------------------------------------------------------------------
 */

#include <postgres.h>
#include <utils/guc.h>
#include <utils/elog.h>
#include <libpq/libpq-be.h>
#include <utils/varlena.h>
#include <utils/memutils.h>


#define  	SUPPRESSOR_USER_NAME 			"pg_logfilter.user_name"
#define  	SUPPRESSOR_APPLICATION_NAME 	"pg_logfilter.application_name"
#define  	SUPPRESSOR_SQLCODE 				"pg_logfilter.sqlcode"
#define  	SUPPRESSOR_CLIENT_IP 			"pg_logfilter.client_ip"
#define  	SUPPRESSOR_DATABASE_NAME		"pg_logfilter.database_name"


PG_MODULE_MAGIC;

void        _PG_init(void) ;
void        _PG_fini(void ) ;

extern struct Port *MyProcPort;
extern char * application_name;

/* Define variables for GUC */
static char*  g_suppress_log_by_username = NULL;
static char*  g_suppress_log_by_database_name = NULL;
static char*  g_suppress_log_by_sqlcode = NULL;
static char*  g_suppress_log_by_client_ip = NULL;
static char*  g_suppress_log_by_application_name = NULL;

static MemoryContext pg_logfilter_context = NULL; 


/* Hook for emit log */
static emit_log_hook_type original_emit_log_hook = NULL;
static void custom_emit_log_hook(ErrorData *edata);


void _PG_init ( void )
{

		pg_logfilter_context = AllocSetContextCreate (
						TopMemoryContext, 
						"pg_logfilter context", 
#if PG_VERSION_NUM >= 90600
						ALLOCSET_DEFAULT_SIZES
#else
						ALLOCSET_DEFAULT_MINSIZE,
						ALLOCSET_DEFAULT_INITSIZE,
						ALLOCSET_DEFAULT_MAXSIZE
#endif
						) ;




		/* Set custome variable */
		DefineCustomStringVariable (SUPPRESSOR_USER_NAME,
						"Suppress log if it matches given user name. ( comma-seprated list of usernames )",
						NULL,
						&g_suppress_log_by_username,
						"",
						PGC_SIGHUP,
						0x00,
						NULL,
						NULL,
						NULL) ;

		DefineCustomStringVariable (SUPPRESSOR_DATABASE_NAME,
						"Suppress log if it matches given database name. ( comma-seprated list of databases )",
						NULL,
						&g_suppress_log_by_database_name,
						"",
						PGC_SIGHUP,
						0x00,
						NULL,
						NULL,
						NULL ) ;


		DefineCustomStringVariable (SUPPRESSOR_SQLCODE,
						"Suppress log if it matches given sqlcode. ( comma-seprated list of sqlcodes )",
						NULL,
						&g_suppress_log_by_sqlcode,
						"",
						PGC_SIGHUP,
						0x00,
						NULL,
						NULL,
						NULL ) ;

		DefineCustomStringVariable (SUPPRESSOR_CLIENT_IP,
						"Suppress log if it matches given client ip. ( comma-seprated list of client ip address )",
						NULL,
						&g_suppress_log_by_client_ip,
						"",
						PGC_SIGHUP,
						0x00,
						NULL,
						NULL,
						NULL ) ;

		DefineCustomStringVariable (SUPPRESSOR_APPLICATION_NAME,
						"Suppress log if it matches given application_name. ( comma-seprated list of application_name )",
						NULL,
						&g_suppress_log_by_application_name,
						"",
						PGC_SIGHUP,
						0x00,
						NULL,
						NULL,
						NULL ) ;

		if ( emit_log_hook != NULL )
		{
				original_emit_log_hook = emit_log_hook;
		}
		emit_log_hook = custom_emit_log_hook;
} 


void _PG_fini ( void ) 
{
		/* Uninstall hook */ 
		emit_log_hook = original_emit_log_hook ;

		// Delete memory context 
		MemoryContextDelete(pg_logfilter_context);
} 

static bool check_username( ErrorData *edata)
{
		List *user_list = NIL;
		ListCell *c;

		if (g_suppress_log_by_username[0] == '\0' )
		{
				return false;
		}



		/* We need to slice configuration string (cvs style) into element.  */
		if ( !SplitIdentifierString (
								pstrdup(g_suppress_log_by_username),
								',',
								&user_list ))
		{
				/* syntax error in name list */
				ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("parameter \"%s\" must be a list of user names",
										 SUPPRESSOR_USER_NAME)));
				return false; 
		}

		/* Iterate & check the value  */
		foreach(c, user_list)
		{
				char *given_username = (char*)lfirst(c);

				/*  MyProcPort can be NULL, if postmaster process calls this hook.
					I think it would be better to ignore the hook function if the caller is postmaster process. */
				if ( MyProcPort != NULL && strcmp (given_username, MyProcPort->user_name ) == 0 )
				{
						//Matched given list
						return true;
				}
		}
		return false;
}

static bool check_database_name ( ErrorData *edata )
{
		List *database_list = NIL ;
		ListCell *c;

		if (g_suppress_log_by_database_name[0] == '\0' )
		{
				return false;
		}


		/* We need to slice configuration string (cvs style) into element.  */
		if ( !SplitIdentifierString (
								pstrdup(g_suppress_log_by_database_name ), 
								',',
								&database_list ))
		{
				ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("parameter \"%s\" must be a list of database names",
										 SUPPRESSOR_DATABASE_NAME)));
				return false; 
		}

		/* Iterate & check the value  */
		foreach(c, database_list)
		{

				char *given_database_name = (char*)lfirst(c);

				/*  MyProcPort can be NULL, if postmaster process calls this hook.
					I think it would be better to ignore hook function if the caller is postmaster process. */
				if ( MyProcPort != NULL && strcmp (given_database_name, MyProcPort->database_name ) == 0 )
				{
						//Matched given list
						return true;
				}
		}
		return false;
}

static bool check_client_ip ( ErrorData *edata )
{
		List *client_ip_list = NIL ;
		ListCell *c;

		if (g_suppress_log_by_client_ip[0] == '\0' )
		{
				return false;
		}

		/* We need to slice configuration string (cvs style) into element.  */
		if ( !SplitIdentifierString (
								pstrdup( g_suppress_log_by_client_ip) ,
								',',
								&client_ip_list ))
		{
				ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("parameter \"%s\" must be a list of client ip addresses",
										 SUPPRESSOR_APPLICATION_NAME)));
				return false; 
		}

		/* Iterate & check the value  */
		foreach(c, client_ip_list)
		{

				char *given_client_ip = (char*)lfirst(c);

				if ( MyProcPort != NULL && strcmp (given_client_ip, MyProcPort->remote_host ) == 0 )
				{
						//Matched given list
						return true;
				}
		}
		return false;
}


static bool check_application_name ( ErrorData *edata )
{
		List *application_list = NIL ;
		ListCell *c;

		if (g_suppress_log_by_application_name[0] == '\0' )
		{
				return false;
		}

		/* We need to slice configuration string (cvs style) into element.  */
		if ( !SplitIdentifierString (
								pstrdup( g_suppress_log_by_application_name) ,
								',',
								&application_list ))
		{
				ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("parameter \"%s\" must be a list of application names",
										 SUPPRESSOR_APPLICATION_NAME)));
				return false; 
		}

		/* Iterate & check the value  */
		foreach(c, application_list)
		{

				char *given_application_name = (char*)lfirst(c);

				if ( strcmp (application_name, given_application_name ) == 0 )
				{
						//Matched given list
						return true;
				}
		}
		return false;
}

static bool check_sqlcode ( ErrorData *edata )
{
		List *sqlcode_list = NIL;
		ListCell *c;

		if ( g_suppress_log_by_sqlcode[0] == '\0' )
		{
				return false;
		}

		/* We need to slice configuration string (cvs style) into element.  */
		if ( !SplitIdentifierString (
								pstrdup( g_suppress_log_by_sqlcode ) ,
								',',
								&sqlcode_list ))
		{
				ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("parameter \"%s\" must be a list of sqlcodes ",
										 SUPPRESSOR_SQLCODE)));
				return false ;
		}

		/* Iterate & check the value  */
		foreach(c, sqlcode_list)
		{
				char *given_sql_code = (char*)lfirst(c);
				if (strcmp ( given_sql_code, unpack_sql_state(edata->sqlerrcode)) == 0 )
				{
						return true;
				}
		}
		return false;
}


static void custom_emit_log_hook ( ErrorData * edata) {

		MemoryContext old; 

		/* if an original hook already exists, invoke the old hook function before me. */
		if  ( original_emit_log_hook != NULL )
		{
				original_emit_log_hook ( edata ) ;
		}


		old = MemoryContextSwitchTo ( pg_logfilter_context);

		if (edata->output_to_server && check_username ( edata ) )
		{
				edata->output_to_server = false;
		}

		if (edata->output_to_server && check_application_name (edata ))
		{
				edata->output_to_server = false;
		}

		if (edata->output_to_server && check_sqlcode(edata))
		{
				edata->output_to_server = false;
		}

		if (edata->output_to_server && check_database_name(edata))
		{
				edata->output_to_server = false;
		}

		if (edata->output_to_server && check_client_ip( edata ))
		{
				edata->output_to_server = false ;
		}

		pg_logfilter_context = MemoryContextSwitchTo ( old ) ;

		// Release all palloc'ed memory. 
		MemoryContextReset ( pg_logfilter_context ) ;

}
