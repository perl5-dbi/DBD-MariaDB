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

BOOT:
  HV *stash = gv_stashpvs("DBD::MariaDB", GV_ADD);
#define newTypeSub(stash, type) newCONSTSUB((stash), #type + sizeof("MYSQL_")-1, newSViv(type))
  newTypeSub(stash, MYSQL_TYPE_DECIMAL);
  newTypeSub(stash, MYSQL_TYPE_TINY);
  newTypeSub(stash, MYSQL_TYPE_SHORT);
  newTypeSub(stash, MYSQL_TYPE_LONG);
  newTypeSub(stash, MYSQL_TYPE_FLOAT);
  newTypeSub(stash, MYSQL_TYPE_DOUBLE);
  newTypeSub(stash, MYSQL_TYPE_NULL);
  newTypeSub(stash, MYSQL_TYPE_TIMESTAMP);
  newTypeSub(stash, MYSQL_TYPE_LONGLONG);
  newTypeSub(stash, MYSQL_TYPE_INT24);
  newTypeSub(stash, MYSQL_TYPE_DATE);
  newTypeSub(stash, MYSQL_TYPE_TIME);
  newTypeSub(stash, MYSQL_TYPE_DATETIME);
  newTypeSub(stash, MYSQL_TYPE_YEAR);
  newTypeSub(stash, MYSQL_TYPE_NEWDATE);
  newTypeSub(stash, MYSQL_TYPE_VARCHAR);
  newTypeSub(stash, MYSQL_TYPE_BIT);
  newTypeSub(stash, MYSQL_TYPE_NEWDECIMAL);
  newTypeSub(stash, MYSQL_TYPE_ENUM);
  newTypeSub(stash, MYSQL_TYPE_SET);
  newTypeSub(stash, MYSQL_TYPE_TINY_BLOB);
  newTypeSub(stash, MYSQL_TYPE_MEDIUM_BLOB);
  newTypeSub(stash, MYSQL_TYPE_LONG_BLOB);
  newTypeSub(stash, MYSQL_TYPE_BLOB);
  newTypeSub(stash, MYSQL_TYPE_VAR_STRING);
  newTypeSub(stash, MYSQL_TYPE_STRING);
#undef newTypeSub

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
  CODE:
{
  D_imp_dbh(dbh);
  I32 num_params= (items > 3 ? items - 3 : 0);
  I32 i;
  my_ulonglong retval;
  STRLEN slen;
  char *str_ptr;
  struct imp_sth_ph_st* params= NULL;
  MYSQL_RES* result= NULL;
  bool async= FALSE;
  int next_result_rc;
  bool failed = FALSE;
  bool            has_been_bound = FALSE;
  bool            use_server_side_prepare = FALSE;
  bool            disable_fallback_for_server_prepare = FALSE;
  MYSQL_STMT      *stmt= NULL;
  MYSQL_BIND      *bind= NULL;
  STRLEN          blen;
    ASYNC_CHECK_XS(dbh);
    while (mysql_next_result(imp_dbh->pmysql)==0)
    {
      MYSQL_RES* res = mysql_use_result(imp_dbh->pmysql);
      if (res)
        mysql_free_result(res);
      }
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
  /*
   * Globally enabled using of server side prepared statement
   * for dbh->do() statements. It is possible to force driver
   * to use server side prepared statement mechanism by adding
   * 'mariadb_server_prepare' attribute to do() method localy:
   * $dbh->do($stmt, {mariadb_server_prepare=>1});
  */
  use_server_side_prepare = imp_dbh->use_server_side_prepare;
  DBD_ATTRIBS_CHECK("do", dbh, attr);
  if (attr)
  {
    HV *hv;
    HE *he;
    SV **svp;
    HV *processed;
    processed = newHV();
    sv_2mortal(newRV_noinc((SV *)processed)); /* Automatically free HV processed */
    (void)hv_stores(processed, "mariadb_server_prepare", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attr, "mariadb_server_prepare");
    use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;
    (void)hv_stores(processed, "mariadb_server_prepare_disable_fallback", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attr, "mariadb_server_prepare_disable_fallback");
    disable_fallback_for_server_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;
    (void)hv_stores(processed, "mariadb_async", &PL_sv_yes);
    svp   = MARIADB_DR_ATTRIB_GET_SVPS(attr, "mariadb_async");
    async = (svp) ? SvTRUE(*svp) : FALSE;
    hv = (HV*) SvRV(attr);
    hv_iterinit(hv);
    while ((he = hv_iternext(hv)) != NULL)
    {
      I32 len;
      const char *key;
      key = hv_iterkey(he, &len);
      if (hv_exists(processed, key, len))
        continue;
      mariadb_dr_do_error(dbh, JW_ERR_INVALID_ATTRIBUTE, SvPVX(sv_2mortal(newSVpvf("Unknown attribute %s", key))), "HY000");
      XSRETURN_UNDEF;
    }
  }
  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                  "mysql.xs do() use_server_side_prepare %d\n",
                  use_server_side_prepare ? 1 : 0);
  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh),
                  "mysql.xs do() async %d\n",
                  (async ? 1 : 0));
  if(async) {
    if (disable_fallback_for_server_prepare)
    {
      mariadb_dr_do_error(dbh, ER_UNSUPPORTED_PS,
               "Async option not supported with server side prepare", "HY000");
      XSRETURN_UNDEF;
    }
    use_server_side_prepare = FALSE; /* for now */
    imp_dbh->async_query_in_flight = imp_dbh;
  }
  if (use_server_side_prepare)
  {
    stmt= mysql_stmt_init(imp_dbh->pmysql);

    if (stmt && mysql_stmt_prepare(stmt, str_ptr, slen))
    {
      if (mariadb_db_reconnect(dbh, stmt))
      {
        mysql_stmt_close(stmt);
        stmt = mysql_stmt_init(imp_dbh->pmysql);
        if (stmt && mysql_stmt_prepare(stmt, str_ptr, slen))
          failed = TRUE;
      }
      else
      {
        failed = TRUE;
      }
    }

    if (!stmt)
    {
      mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
      retval = (my_ulonglong)-1;
    }
    else if (failed)
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
        Newz(0, bind, num_params, MYSQL_BIND);

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
                                           str_ptr,
                                           slen,
                                           num_params,
                                           &result,
                                           &stmt,
                                           bind,
                                           imp_dbh->pmysql,
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
    retval = mariadb_st_internal_execute(dbh, str_ptr, slen, num_params,
                                       params, &result, imp_dbh->pmysql, FALSE);
  }
  if (params)
    Safefree(params);

  if (result)
  {
    mysql_free_result(result);
    result = NULL;
  }
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
      my_ulonglong insertid;
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
        if (mariadb_db_reconnect(dbh, NULL))
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
  D_imp_sth(sth);
  RETVAL = mariadb_st_more_results(sth, imp_sth);
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
#define SQL_DBMS_VER 18
#define SQL_MAXIMUM_STATEMENT_LENGTH 105
#define SQL_MAXIMUM_TABLES_IN_SELECT 106
#define SQL_SERVER_NAME 13

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
	case SQL_DBMS_VER:
	    retsv = newSVpv(mysql_get_server_info(imp_dbh->pmysql), 0);
	    sv_utf8_decode(retsv);
	    break;
	case SQL_MAXIMUM_STATEMENT_LENGTH:
	{
#if (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50709 && MYSQL_VERSION_ID != 60000) || (defined(MARIADB_VERSION_ID) && MARIADB_VERSION_ID >= 100202)
	    /* mysql_get_option() was added in mysql 5.7.3 */
	    /* MYSQL_OPT_NET_BUFFER_LENGTH was added in mysql 5.7.9 */
	    /* MYSQL_OPT_NET_BUFFER_LENGTH was added in MariaDB 10.2.2 */
	    unsigned long buffer_len;
	    mysql_get_option(NULL, MYSQL_OPT_NET_BUFFER_LENGTH, &buffer_len);
	    retsv = newSVuv(buffer_len);
#else
	    /* before MySQL 5.7.9 and MariaDB 10.2.2 use net_buffer_length macro */
	    retsv = newSVuv(net_buffer_length);
#endif
	    break;
	}
	case SQL_MAXIMUM_TABLES_IN_SELECT:
	    retsv = newSViv(mysql_get_server_version(imp_dbh->pmysql) >= 50000 ? 63 : 31);
	    break;
	case SQL_SERVER_NAME:
	    retsv = newSVpv(mysql_get_host_info(imp_dbh->pmysql), 0);
	    sv_utf8_decode(retsv);
	    break;
    	default:
	    mariadb_dr_do_error(dbh, JW_ERR_INVALID_ATTRIBUTE, SvPVX(sv_2mortal(newSVpvf("Unknown SQL Info type %" IVdf, type))), "HY000");
	    XSRETURN_UNDEF;
    }
    ST(0) = sv_2mortal(retsv);

