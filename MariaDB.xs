/* Hej, Emacs, this is -*- C -*- mode!

   Copyright (c) 2018      GoodData Corporation
   Copyright (c) 2015-2017 Pali Rohár
   Copyright (c) 2004-2017 Patrick Galbraith
   Copyright (c) 2013-2017 Michiel Beijen
   Copyright (c) 2004-2007 Alexey Stroganov
   Copyright (c) 2003-2005 Rudolf Lippan
   Copyright (c) 1997-2003 Jochen Wiedmann

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "dbdimp.h"
#include "constants.h"

#include <errno.h>
#include <string.h>

#define ASYNC_CHECK_XS(h)\
  if(imp_dbh->async_query_in_flight) {\
      mariadb_dr_do_error(h, 2000, "Calling a synchronous function on an asynchronous handle", "HY000");\
      XSRETURN_UNDEF;\
  }


DBISTATE_DECLARE;


MODULE = DBD::MariaDB	PACKAGE = DBD::MariaDB

INCLUDE: MariaDB.xsi

MODULE = DBD::MariaDB	PACKAGE = DBD::MariaDB

double
constant(name, arg)
    char* name
    char* arg
  CODE:
    RETVAL = mariadb_constant(name, arg);
  OUTPUT:
    RETVAL


MODULE = DBD::MariaDB    PACKAGE = DBD::MariaDB::db


void
connected(dbh, ...)
  SV* dbh
PPCODE:
  /* Called by DBI when connect method finished */
  D_imp_dbh(dbh);
  imp_dbh->connected = TRUE;
  XSRETURN_EMPTY;


void
type_info_all(dbh)
  SV* dbh
  PPCODE:
{
  /* 	static AV* types = NULL; */
  /* 	if (!types) { */
  /* 	    D_imp_dbh(dbh); */
  /* 	    if (!(types = dbd_db_type_info_all(dbh, imp_dbh))) { */
  /* 	        croak("Cannot create types array (out of memory?)"); */
  /* 	    } */
  /* 	} */
  /* 	ST(0) = sv_2mortal(newRV_inc((SV*) types)); */
  D_imp_dbh(dbh);
  ASYNC_CHECK_XS(dbh);
  ST(0) = sv_2mortal(newRV_noinc((SV*) mariadb_db_type_info_all(dbh,
                                                            imp_dbh)));
  XSRETURN(1);
}


SV *
do(dbh, statement, attr=Nullsv, ...)
  SV *        dbh
  SV *	statement
  SV *        attr
  PROTOTYPE: $$;$@
  CODE:
{
  D_imp_dbh(dbh);
  int num_params= (items > 3 ? items - 3 : 0);
  int i;
  my_ulonglong retval;
  STRLEN slen;
  char *str_ptr;
  struct imp_sth_ph_st* params= NULL;
  MYSQL_RES* result= NULL;
  bool async= FALSE;
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
  int next_result_rc;
#endif
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  bool            has_been_bound = FALSE;
  bool            use_server_side_prepare = FALSE;
  bool            disable_fallback_for_server_prepare = FALSE;
  MYSQL_STMT      *stmt= NULL;
  MYSQL_BIND      *bind= NULL;
  STRLEN          blen;
#endif
    ASYNC_CHECK_XS(dbh);
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
    while (mysql_next_result(imp_dbh->pmysql)==0)
    {
      MYSQL_RES* res = mysql_use_result(imp_dbh->pmysql);
      if (res)
        mysql_free_result(res);
      }
#endif
  if (SvMAGICAL(statement))
    mg_get(statement);
  for (i = 0; i < num_params; i++)
  {
    SV *param= ST(i+3);
    if (SvMAGICAL(param))
      mg_get(param);
  }
  (void)hv_stores((HV*)SvRV(dbh), "Statement", SvREFCNT_inc(statement));
  str_ptr = SvPVutf8_nomg(statement, slen);
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  /*
   * Globally enabled using of server side prepared statement
   * for dbh->do() statements. It is possible to force driver
   * to use server side prepared statement mechanism by adding
   * 'mariadb_server_prepare' attribute to do() method localy:
   * $dbh->do($stmt, {mariadb_server_prepare=>1});
  */
  use_server_side_prepare = imp_dbh->use_server_side_prepare;
  if (attr)
  {
    SV** svp;
    DBD_ATTRIBS_CHECK("do", dbh, attr);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attr, "mariadb_server_prepare");

    use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;

    svp = MARIADB_DR_ATTRIB_GET_SVPS(attr, "mariadb_server_prepare_disable_fallback");
    disable_fallback_for_server_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;
  }
  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                  "mysql.xs do() use_server_side_prepare %d\n",
                  use_server_side_prepare ? 1 : 0);
#endif
  if (attr)
  {
    SV** svp;
    svp   = MARIADB_DR_ATTRIB_GET_SVPS(attr, "async");
    async = (svp) ? SvTRUE(*svp) : FALSE;
  }
  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                  "mysql.xs do() async %d\n",
                  (async ? 1 : 0));
  if(async) {
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
    if (disable_fallback_for_server_prepare)
    {
      mariadb_dr_do_error(dbh, ER_UNSUPPORTED_PS,
               "Async option not supported with server side prepare", "HY000");
      XSRETURN_UNDEF;
    }
    use_server_side_prepare = FALSE; /* for now */
#endif
    imp_dbh->async_query_in_flight = imp_dbh;
  }
#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
  if (use_server_side_prepare)
  {
    stmt= mysql_stmt_init(imp_dbh->pmysql);

    if ((mysql_stmt_prepare(stmt, str_ptr, slen))  &&
        (!mariadb_db_reconnect(dbh) ||
         (mysql_stmt_prepare(stmt, str_ptr, slen))))
    {
      /*
        For commands that are not supported by server side prepared
        statement mechanism lets try to pass them through regular API
      */
      if (!disable_fallback_for_server_prepare && mysql_stmt_errno(stmt) == ER_UNSUPPORTED_PS)
      {
        use_server_side_prepare = FALSE;
      }
      else
      {
        mariadb_dr_do_error(dbh, mysql_stmt_errno(stmt), mysql_stmt_error(stmt)
                 ,mysql_stmt_sqlstate(stmt));
        retval = (my_ulonglong)-1;
      }
      mysql_stmt_close(stmt);
      stmt= NULL;
    }
    else
    {
      /*
        'items' is the number of arguments passed to XSUB, supplied
        by xsubpp compiler, as listed in manpage for perlxs
      */
      if (items > 3)
      {
        /*
          Handle binding supplied values to placeholders assume user has
          passed the correct number of parameters
        */
        Newz(0, bind, (unsigned int) num_params, MYSQL_BIND);

        for (i = 0; i < num_params; i++)
        {
          SV *param= ST(i+3);
          if (SvOK(param))
          {
            bind[i].buffer= SvPVutf8_nomg(param, blen);
            bind[i].buffer_length= blen;
            bind[i].buffer_type= MYSQL_TYPE_STRING;
          }
          else
          {
            bind[i].buffer= NULL;
            bind[i].buffer_length= 0;
            bind[i].buffer_type= MYSQL_TYPE_NULL;
          }
        }
      }
      retval = mariadb_st_internal_execute41(dbh,
                                           num_params,
                                           &result,
                                           stmt,
                                           bind,
                                           &has_been_bound);
      if (bind)
        Safefree(bind);

      mysql_stmt_close(stmt);
      stmt= NULL;

      if (retval == (my_ulonglong)-1) /* -1 means error */
      {
        SV *err = DBIc_ERR(imp_dbh);
        if (!disable_fallback_for_server_prepare && SvIV(err) == ER_UNSUPPORTED_PS)
        {
          use_server_side_prepare = FALSE;
        }
      }
    }
  }

  if (! use_server_side_prepare)
  {
#endif
    if (items > 3)
    {
      /*  Handle binding supplied values to placeholders	   */
      /*  Assume user has passed the correct number of parameters  */
      Newz(0, params, sizeof(*params)*num_params, struct imp_sth_ph_st);
      for (i= 0;  i < num_params;  i++)
      {
        SV *param= ST(i+3);
        if (SvOK(param))
          params[i].value= SvPVutf8_nomg(param, params[i].len);
        else
          params[i].value= NULL;
        params[i].type= SQL_VARCHAR;
      }
    }
    retval = mariadb_st_internal_execute(dbh, str_ptr, slen, attr, num_params,
                                       params, &result, imp_dbh->pmysql, FALSE);
#if MYSQL_VERSION_ID >=SERVER_PREPARE_VERSION
  }
#endif
  if (params)
    Safefree(params);

  if (result)
  {
    mysql_free_result(result);
    result = NULL;
  }
#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
  if (retval != (my_ulonglong)-1 && !async) /* -1 means error */
    {
      /* more results? -1 = no, >0 = error, 0 = yes (keep looping) */
      while ((next_result_rc= mysql_next_result(imp_dbh->pmysql)) == 0)
      {
        result = mysql_use_result(imp_dbh->pmysql);
          if (result)
            mysql_free_result(result);
            result = NULL;
          }
          if (next_result_rc > 0)
          {
            if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
              PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                            "\t<- do() ERROR: %s\n",
                            mysql_error(imp_dbh->pmysql));

              mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql),
                       mysql_error(imp_dbh->pmysql),
                       mysql_sqlstate(imp_dbh->pmysql));
              retval = (my_ulonglong)-1;
          }
    }
#endif

  if (retval == 0)                      /* ok with no rows affected     */
    XSRETURN_PV("0E0");                 /* (true but zero)              */
  else if (retval == (my_ulonglong)-1)  /* -1 means error               */
    XSRETURN_UNDEF;

  RETVAL = my_ulonglong2sv(retval);
}
  OUTPUT:
    RETVAL


bool
ping(dbh)
    SV* dbh;
  PROTOTYPE: $
  CODE:
    {
/* MySQL 5.7 below 5.7.18 is affected by Bug #78778.
 * MySQL 5.7.18 and higher (including 8.0.3) is affected by Bug #89139.
 *
 * Once Bug #89139 is fixed we can adjust the upper bound of this check.
 *
 * https://bugs.mysql.com/bug.php?id=78778
 * https://bugs.mysql.com/bug.php?id=89139 */
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50718
      unsigned long long insertid;
#endif
      D_imp_dbh(dbh);
      ASYNC_CHECK_XS(dbh);
      if (!imp_dbh->pmysql)
        XSRETURN_NO;
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50718
      insertid = mysql_insert_id(imp_dbh->pmysql);
#endif
      RETVAL = (mysql_ping(imp_dbh->pmysql) == 0);
      if (!RETVAL)
      {
        if (mariadb_db_reconnect(dbh))
          RETVAL = (mysql_ping(imp_dbh->pmysql) == 0);
      }
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50718
      imp_dbh->pmysql->insert_id = insertid;
#endif
    }
  OUTPUT:
    RETVAL



void
quote(dbh, str, type=NULL)
    SV* dbh
    SV* str
    SV* type
  PROTOTYPE: $$;$
  PPCODE:
    {
        SV* quoted;

        D_imp_dbh(dbh);
        ASYNC_CHECK_XS(dbh);

        quoted = mariadb_db_quote(dbh, str, type);
	ST(0) = quoted ? sv_2mortal(quoted) : str;
	XSRETURN(1);
    }

void mariadb_sockfd(dbh)
    SV* dbh
  PPCODE:
    {
        D_imp_dbh(dbh);
        if(imp_dbh->pmysql->net.fd != -1) {
            XSRETURN_IV(imp_dbh->pmysql->net.fd);
        } else {
            XSRETURN_UNDEF;
        }
    }

SV *
mariadb_async_result(dbh)
    SV* dbh
  CODE:
    {
        my_ulonglong retval;

        retval = mariadb_db_async_result(dbh, NULL);

        if (retval == 0)
            XSRETURN_PV("0E0");
        else if (retval == (my_ulonglong)-1)
            XSRETURN_UNDEF;

        RETVAL = my_ulonglong2sv(retval);
    }
  OUTPUT:
    RETVAL

void mariadb_async_ready(dbh)
    SV* dbh
  PPCODE:
    {
        int retval;

        retval = mariadb_db_async_ready(dbh);
        if(retval > 0) {
            XSRETURN_YES;
        } else if(retval == 0) {
            XSRETURN_NO;
        } else {
            XSRETURN_UNDEF;
        }
    }

void _async_check(dbh)
    SV* dbh
  PPCODE:
    {
        D_imp_dbh(dbh);
        ASYNC_CHECK_XS(dbh);
        XSRETURN_YES;
    }

MODULE = DBD::MariaDB    PACKAGE = DBD::MariaDB::st

bool
more_results(sth)
    SV *	sth
    CODE:
{
#if (MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION)
  D_imp_sth(sth);
  RETVAL = mariadb_st_more_results(sth, imp_sth);
#else
  PERL_UNUSED_ARG(sth);
  RETVAL = FALSE;
#endif
}
    OUTPUT:
      RETVAL

SV *
rows(sth)
    SV* sth
  CODE:
    D_imp_sth(sth);
    D_imp_dbh_from_sth;
    if(imp_dbh->async_query_in_flight) {
        if (mariadb_db_async_result(sth, &imp_sth->result) == (my_ulonglong)-1) {
            XSRETURN_UNDEF;
        }
    }
    RETVAL = my_ulonglong2sv(imp_sth->row_num);
  OUTPUT:
    RETVAL

SV *
mariadb_async_result(sth)
    SV* sth
  CODE:
    {
        D_imp_sth(sth);
        my_ulonglong retval;

        retval= mariadb_db_async_result(sth, &imp_sth->result);

        if (retval == (my_ulonglong)-1)
            XSRETURN_UNDEF;

        imp_sth->row_num = retval;

        if (retval == 0)
            XSRETURN_PV("0E0");

        RETVAL = my_ulonglong2sv(retval);
    }
  OUTPUT:
    RETVAL

void mariadb_async_ready(sth)
    SV* sth
  PPCODE:
    {
        int retval;

        retval = mariadb_db_async_ready(sth);
        if(retval > 0) {
            XSRETURN_YES;
        } else if(retval == 0) {
            XSRETURN_NO;
        } else {
            XSRETURN_UNDEF;
        }
    }

void _async_check(sth)
    SV* sth
  PPCODE:
    {
        D_imp_sth(sth);
        D_imp_dbh_from_sth;
        ASYNC_CHECK_XS(sth);
        XSRETURN_YES;
    }


MODULE = DBD::MariaDB    PACKAGE = DBD::MariaDB::GetInfo

# This probably should be grabed out of some ODBC types header file
#define SQL_CATALOG_NAME_SEPARATOR 41
#define SQL_CATALOG_TERM 42
#define SQL_DBMS_VER 18
#define SQL_IDENTIFIER_QUOTE_CHAR 29
#define SQL_MAXIMUM_STATEMENT_LENGTH 105
#define SQL_MAXIMUM_TABLES_IN_SELECT 106
#define SQL_MAX_TABLE_NAME_LEN 35
#define SQL_SERVER_NAME 13
#define SQL_ASYNC_MODE 10021
#define SQL_MAX_ASYNC_CONCURRENT_STATEMENTS 10022

#define SQL_AM_NONE       0
#define SQL_AM_CONNECTION 1
#define SQL_AM_STATEMENT  2


#  dbd_mariadb_get_info()
#  Return ODBC get_info() information that must needs be accessed from C
#  This is an undocumented function that should only
#  be used by DBD::MariaDB::GetInfo.

void
dbd_mariadb_get_info(dbh, sql_info_type)
    SV* dbh
    SV* sql_info_type
  CODE:
    D_imp_dbh(dbh);
    IV type = 0;
    SV* retsv=NULL;

    if (SvGMAGICAL(sql_info_type))
        mg_get(sql_info_type);

    if (SvOK(sql_info_type))
    	type = SvIV_nomg(sql_info_type);
    else
    {
        mariadb_dr_do_error(dbh, JW_ERR_INVALID_ATTRIBUTE, "get_info called with an invalied parameter", "HY000");
        XSRETURN_UNDEF;
    }
    
    switch(type) {
    	case SQL_CATALOG_NAME_SEPARATOR:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpvs(".");
	    break;
	case SQL_CATALOG_TERM:
	    /* (dbc->flag & FLAG_NO_CATALOG) ? WTF is in flag ? */
	    retsv = newSVpvs("database");
	    break;
	case SQL_DBMS_VER:
	    retsv = newSVpv(mysql_get_server_info(imp_dbh->pmysql), 0);
	    sv_utf8_decode(retsv);
	    break;
	case SQL_IDENTIFIER_QUOTE_CHAR:
	    retsv = newSVpvs("`");
	    break;
	case SQL_MAXIMUM_STATEMENT_LENGTH:
	{
#if (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50709 && MYSQL_VERSION_ID != 60000) || (defined(MARIADB_VERSION_ID) && MARIADB_VERSION_ID >= 100202)
	    /* mysql_get_option() was added in mysql 5.7.3 */
	    /* MYSQL_OPT_NET_BUFFER_LENGTH was added in mysql 5.7.9 */
	    /* MYSQL_OPT_NET_BUFFER_LENGTH was added in MariaDB 10.2.2 */
	    unsigned long buffer_len;
	    mysql_get_option(NULL, MYSQL_OPT_NET_BUFFER_LENGTH, &buffer_len);
	    retsv = newSViv(buffer_len);
#else
	    /* before MySQL 5.7.9 and MariaDB 10.2.2 use net_buffer_length macro */
	    retsv = newSViv(net_buffer_length);
#endif
	    break;
	}
	case SQL_MAXIMUM_TABLES_IN_SELECT:
	    /* newSViv((sizeof(int) > 32) ? sizeof(int)-1 : 31 ); in general? */
	    retsv= newSViv((sizeof(int) == 64 ) ? 63 : 31 );
	    break;
	case SQL_MAX_TABLE_NAME_LEN:
	    retsv= newSViv(NAME_LEN);
	    break;
	case SQL_SERVER_NAME:
	    retsv = newSVpv(mysql_get_host_info(imp_dbh->pmysql), 0);
	    sv_utf8_decode(retsv);
	    break;
        case SQL_ASYNC_MODE:
            retsv = newSViv(SQL_AM_STATEMENT);
            break;
        case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
            retsv = newSViv(1);
            break;
    	default:
	    mariadb_dr_do_error(dbh, JW_ERR_INVALID_ATTRIBUTE, SvPVX(sv_2mortal(newSVpvf("Unknown SQL Info type %" IVdf, type))), "HY000");
	    XSRETURN_UNDEF;
    }
    ST(0) = sv_2mortal(retsv);

