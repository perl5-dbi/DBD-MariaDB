/* Hej, Emacs, this is -*- C -*- mode!

   Copyright (c) 2018      GoodData Corporation
   Copyright (c) 2015-2022 Pali Rohár
   Copyright (c) 2004-2017 Patrick Galbraith
   Copyright (c) 2013-2017 Michiel Beijen
   Copyright (c) 2004-2007 Alexey Stroganov
   Copyright (c) 2003-2005 Rudolf Lippan
   Copyright (c) 1997-2003 Jochen Wiedmann

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "dbdimp.h"

#define ASYNC_CHECK_XS(h)\
  if(imp_dbh->async_query_in_flight) {\
      mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Calling a synchronous function on an asynchronous handle", "HY000");\
      XSRETURN_UNDEF;\
  }


DBISTATE_DECLARE;


MODULE = DBD::MariaDB	PACKAGE = DBD::MariaDB

INCLUDE: MariaDB.xsi

MODULE = DBD::MariaDB	PACKAGE = DBD::MariaDB

BOOT:
{
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
#if defined(HAVE_DEINITIALIZE_SSL) && defined(HAVE_PROBLEM_WITH_OPENSSL)
  /* Do not deinitialize OpenSSL library after mysql_server_end()
   * See: https://github.com/perl5-dbi/DBD-MariaDB/issues/119 */
  mariadb_deinitialize_ssl = 0;
#endif
#ifndef _WIN32
  /* Calling mysql_thread_init() on WIN32 cause crash */
  mysql_thread_init();
#endif
}

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
  PERL_UNUSED_VAR(dbh);
  ST(0) = sv_2mortal(newRV_noinc((SV*) mariadb_db_type_info_all()));
  XSRETURN(1);
}


#ifndef HAVE_DBI_1_642

IV
do(dbh, statement, attr=Nullsv, ...)
  SV *dbh
  SV *statement
  SV *attr
CODE:
{
  /* Compatibility for DBI version prior to 1.642 which does not support dbd_db_do6 API */
  D_imp_dbh(dbh);
  RETVAL = mariadb_db_do6(dbh, imp_dbh, statement, attr, items-3, ax+3);
  if (RETVAL == 0)              /* ok with no rows affected     */
    XSRETURN_PV("0E0");         /* (true but zero)              */
  else if (RETVAL < -1)         /* -1 == unknown number of rows */
    XSRETURN_UNDEF;
}
OUTPUT:
  RETVAL

#endif


bool
ping(dbh)
    SV* dbh;
  CODE:
    {
      D_imp_dbh(dbh);
      ASYNC_CHECK_XS(dbh);
      if (!imp_dbh->pmysql)
        XSRETURN_NO;
      RETVAL = (mysql_ping(imp_dbh->pmysql) == 0);
      if (!RETVAL)
      {
        if (mariadb_db_reconnect(dbh, NULL))
          RETVAL = (mysql_ping(imp_dbh->pmysql) == 0);
      }
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

SV *
mariadb_sockfd(dbh)
    SV* dbh
  CODE:
    D_imp_dbh(dbh);
    RETVAL = (imp_dbh->sock_fd >= 0) ? newSViv(imp_dbh->sock_fd) : &PL_sv_undef;
  OUTPUT:
    RETVAL

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
        else if (retval == (my_ulonglong)-2)
            XSRETURN_IV(-1);

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
    if (imp_sth->row_num == (my_ulonglong)-1 || imp_sth->row_num == (my_ulonglong)-2)
        XSRETURN_IV(-1);
    RETVAL = my_ulonglong2sv(imp_sth->row_num);
  OUTPUT:
    RETVAL

#ifndef HAVE_DBI_1_642

void
last_insert_id(sth, catalog=&PL_sv_undef, schema=&PL_sv_undef, table=&PL_sv_undef, field=&PL_sv_undef, attr=Nullsv)
  SV *sth
  SV *catalog
  SV *schema
  SV *table
  SV *field
  SV *attr
PPCODE:
{
  /* Compatibility for DBI version prior to 1.642 which does not support dbd_st_last_insert_id API */
  D_imp_sth(sth);
  ST(0) = mariadb_st_last_insert_id(sth, imp_sth, catalog, schema, table, field, attr);
  XSRETURN(1);
}

#endif

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

        if (retval == (my_ulonglong)-2)
            XSRETURN_IV(-1);

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
