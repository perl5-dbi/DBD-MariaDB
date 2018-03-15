/*
 *  DBD::MariaDB - DBI driver for the MariaDB and MySQL database
 *
 *  Copyright (c) 2005       Patrick Galbraith
 *  Copyright (c) 2003       Rudolf Lippan
 *  Copyright (c) 1997-2003  Jochen Wiedmann
 *
 *  Based on DBD::Oracle; DBD::Oracle is
 *
 *  Copyright (c) 1994,1995  Tim Bunce
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 */

#define PERL_NO_GET_CONTEXT
/*
 *  Header files we use
 */
#include <DBIXS.h>  /* installed by the DBI module                        */
#include <mysql.h>  /* Comes with MySQL-devel */
#include <mysqld_error.h>  /* Comes MySQL */

#include <errmsg.h> /* Comes with MySQL-devel */
#include <stdint.h> /* For uint32_t */

#ifndef MYSQL_VERSION_ID
#include <mysql_version.h> /* Comes with MySQL-devel */
#endif

#if !defined(MARIADB_BASE_VERSION) && defined(MARIADB_PACKAGE_VERSION)
#define MARIADB_BASE_VERSION
#endif

/* Macro is available in my_global.h which is not included or present in some versions of MariaDB */
#ifndef NOT_FIXED_DEC
#define NOT_FIXED_DEC 31
#endif

/* Macro is available in m_ctype.h which is not included in some versions of MySQL */
#ifndef MY_CS_PRIMARY
#define MY_CS_PRIMARY 32
#endif

#ifndef PERL_STATIC_INLINE
#define PERL_STATIC_INLINE static
#endif

#ifndef SVfARG
#define SVfARG(p) ((void*)(p))
#endif

#ifndef SSize_t_MAX
#define SSize_t_MAX (SSize_t)(~(Size_t)0 >> 1)
#endif

/* PERL_UNUSED_ARG does not exist prior to perl 5.9.3 */
#ifndef PERL_UNUSED_ARG
#  if defined(lint) && defined(S_SPLINT_S) /* www.splint.org */
#    include <note.h>
#    define PERL_UNUSED_ARG(x) NOTE(ARGUNUSED(x))
#  else
#    define PERL_UNUSED_ARG(x) ((void)x)
#  endif
#endif

/* assert_not_ROK is broken prior to perl 5.8.2 */
#if PERL_VERSION < 8 || (PERL_VERSION == 8 && PERL_SUBVERSION < 2)
#undef assert_not_ROK
#define assert_not_ROK(sv)
#endif

#ifndef SvPV_nomg_nolen
#define SvPV_nomg_nolen(sv) ((SvFLAGS(sv) & (SVf_POK)) == SVf_POK ? SvPVX(sv) : sv_2pv_flags(sv, &PL_na, 0))
#endif

/* looks_like_number() process get magic prior to perl 5.15.4, so reimplement it */
#if PERL_VERSION < 15 || (PERL_VERSION == 15 && PERL_SUBVERSION < 4)
#undef looks_like_number
PERL_STATIC_INLINE I32 looks_like_number(pTHX_ SV *sv)
{
  char *sbegin;
  STRLEN len;
  if (!SvPOK(sv) && !SvPOKp(sv))
    return SvFLAGS(sv) & (SVf_NOK|SVp_NOK|SVf_IOK|SVp_IOK);
  sbegin = SvPV_nomg(sv, len);
  return grok_number(sbegin, len, NULL);
}
#define looks_like_number(sv) looks_like_number(aTHX_ (sv))
#endif

#ifndef SvPVutf8_nomg
PERL_STATIC_INLINE char * SvPVutf8_nomg(pTHX_ SV *sv, STRLEN *len)
{
  char *buf = SvPV_nomg(sv, *len);
  if (SvUTF8(sv))
    return buf;
  if (SvGMAGICAL(sv))
    sv = sv_2mortal(newSVpvn(buf, *len));
  /* There is sv_utf8_upgrade_nomg(), but it is broken prior to Perl version 5.13.10 */
  return SvPVutf8(sv, *len);
}
#define SvPVutf8_nomg(sv, len) SvPVutf8_nomg(aTHX_ (sv), &(len))
#endif

#ifndef SvPVbyte_nomg
PERL_STATIC_INLINE char * SvPVbyte_nomg(pTHX_ SV *sv, STRLEN *len)
{
  char *buf = SvPV_nomg(sv, *len);
  if (!SvUTF8(sv))
    return buf;
  if (SvGMAGICAL(sv))
  {
    sv = sv_2mortal(newSVpvn(buf, *len));
    SvUTF8_on(sv);
  }
  return SvPVbyte(sv, *len);
}
#define SvPVbyte_nomg(sv, len) SvPVbyte_nomg(aTHX_ (sv), &(len))
#endif

#ifndef SvTRUE_nomg
#define SvTRUE_nomg SvTRUE /* SvTRUE does not process get magic for scalars with already cached values, so we are safe */
#endif

/* Sorry, there is no way to handle integer magic scalars properly prior to perl 5.9.1 */

/* Remove wrong SvIV_nomg macro defined by ppport.h */
#if defined(SvIV_nomg) && (PERL_VERSION < 9 | (PERL_VERSION == 9 && PERL_SUBVERSION < 1))
#undef SvIV_nomg
#endif

#ifndef SvIV_nomg
PERL_STATIC_INLINE IV SvIV_nomg(pTHX_ SV *sv)
{
  UV uv;
  char *str;
  STRLEN len;
  int num_type;
  if (SvIOK(sv) || SvIOKp(sv))
  {
    if (!SvIsUV(sv))
      return SvIVX(sv);
    uv = SvUVX(sv);
    if (uv > (UV)IV_MAX)
      return IV_MAX;
    return (IV)uv;
  }
  if (SvNOK(sv) || SvNOKp(sv))
    return (IV)SvNVX(sv);
  str = SvPV_nomg(sv, len);
  num_type = grok_number(str, len, &uv);
  if (!(num_type & (IS_NUMBER_IN_UV)) || (num_type & IS_NUMBER_NOT_INT))
  {
    warner(packWARN(WARN_NUMERIC), "Argument \"%s\" isn't numeric", str);
    return 0;
  }
  if (num_type & IS_NUMBER_NEG)
  {
    if (uv > (UV)IV_MAX+1)
      return IV_MIN;
    return -(IV)uv;
  }
  else
  {
    if (uv > (UV)IV_MAX)
      return IV_MAX;
    return (IV)uv;
  }
}
#define SvIV_nomg(sv) SvIV_nomg(aTHX_ sv)
#endif

/* Remove wrong SvUV_nomg macro defined by ppport.h */
#if defined(SvUV_nomg) && (PERL_VERSION < 9 | (PERL_VERSION == 9 && PERL_SUBVERSION < 1))
#undef SvUV_nomg
#endif

#ifndef SvUV_nomg
PERL_STATIC_INLINE UV SvUV_nomg(pTHX_ SV *sv)
{
  IV iv;
  UV uv;
  char *str;
  STRLEN len;
  int num_type;
  if (SvIOK(sv) || SvIOKp(sv))
  {
    if (SvIsUV(sv))
      return SvUVX(sv);
    iv = SvIVX(sv);
    if (iv < 0)
      return 0;
    return (UV)iv;
  }
  if (SvNOK(sv) || SvNOKp(sv))
    return (UV)SvNVX(sv);
  str = SvPV_nomg(sv, len);
  num_type = grok_number(str, len, &uv);
  if (!(num_type & (IS_NUMBER_IN_UV)) || (num_type & IS_NUMBER_NOT_INT))
  {
    warner(packWARN(WARN_NUMERIC), "Argument \"%s\" isn't numeric", str);
    return 0;
  }
  if (num_type & IS_NUMBER_NEG)
    return 0;
  return uv;
}
#define SvUV_nomg(sv) SvUV_nomg(aTHX_ sv)
#endif

/* Sorry, there is no way to handle numeric magic scalars properly prior to perl 5.13.2 */
#ifndef SvNV_nomg
#define SvNV_nomg(sv)                       \
  ((SvNOK(sv) || SvNOKp(sv))                \
    ? SvNVX(sv)                             \
    : (SvIOK(sv) || SvIOKp(sv))             \
      ? (SvIsUV(sv)                         \
        ? ((NV)SvUVX(sv))                   \
        : ((NV)SvIVX(sv)))                  \
      : (SvPOK(sv) || SvPOKp(sv))           \
        ? ((NV)Atof(SvPVX(sv)))             \
        : ((NV)Atof(SvPV_nomg_nolen(sv))))
#endif

#ifndef sv_cmp_flags
#define sv_cmp_flags(a,b,c) sv_cmp(a,b) /* Sorry, there is no way to compare magic scalars properly prior to perl 5.13.6 */
#endif


/*
 * This is the version of MariaDB or MySQL wherer
 * the server will be used to process prepare
 * statements as opposed to emulation in the driver
*/
#define SQL_STATE_VERSION 40101
#define WARNING_COUNT_VERSION 40101
#define FIELD_CHARSETNR_VERSION 40101 /* should equivalent to 4.1.0  */
#define MULTIPLE_RESULT_SET_VERSION 40102
#define SERVER_PREPARE_VERSION 40103
#define CALL_PLACEHOLDER_VERSION 50503
#define LIMIT_PLACEHOLDER_VERSION 50007
#define GEO_DATATYPE_VERSION 50007
#define NEW_DATATYPE_VERSION 50003
#define MYSQL_VERSION_5_0 50001
/* This is to avoid the ugly #ifdef mess in dbdimp.c */
#if MYSQL_VERSION_ID < SQL_STATE_VERSION
#define mysql_sqlstate(svsock) (NULL)
#endif
/*
 * This is the versions of libmysql that supports MySQL Fabric.
*/
#define LIBMYSQL_FABRIC_VERSION 60200
#define LIBMYSQL_LAST_FABRIC_VERSION 69999

#if LIBMYSQL_VERSION_ID >= LIBMYSQL_FABRIC_VERSION && LIBMYSQL_VERSION_ID <= LIBMYSQL_LAST_FABRIC_VERSION
#define FABRIC_SUPPORT 1
#else
#define FABRIC_SUPPORT 0
#endif

#if MYSQL_VERSION_ID < WARNING_COUNT_VERSION
#define mysql_warning_count(svsock) 0
#endif

#if MYSQL_VERSION_ID < WARNING_COUNT_VERSION
#define mysql_warning_count(svsock) 0
#endif

#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 80001
#define my_bool bool
#endif

#define true 1
#define false 0

/* MYSQL_SECURE_AUTH became a no-op from MySQL 5.7.5 and is removed from MySQL 8.0.3 */
#if defined(MARIADB_BASE_VERSION) || MYSQL_VERSION_ID <= 50704
#define HAVE_SECURE_AUTH
#endif

/*
 * Check which SSL settings are supported by API at compile time
 */

/* Use mysql_options with MYSQL_OPT_SSL_VERIFY_SERVER_CERT */
#if ((MYSQL_VERSION_ID >= 50023 && MYSQL_VERSION_ID < 50100) || MYSQL_VERSION_ID >= 50111) && (MYSQL_VERSION_ID < 80000 || defined(MARIADB_BASE_VERSION))
#define HAVE_SSL_VERIFY
#endif

/* Use mysql_options with MYSQL_OPT_SSL_ENFORCE */
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50703 && MYSQL_VERSION_ID < 80000 && MYSQL_VERSION_ID != 60000
#define HAVE_SSL_ENFORCE
#endif

/* Use mysql_options with MYSQL_OPT_SSL_MODE */
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50711 && MYSQL_VERSION_ID != 60000
#define HAVE_SSL_MODE
#endif

/* Use mysql_options with MYSQL_OPT_SSL_MODE, but only SSL_MODE_REQUIRED is supported */
#if !defined(MARIADB_BASE_VERSION) && ((MYSQL_VERSION_ID >= 50636 && MYSQL_VERSION_ID < 50700) || (MYSQL_VERSION_ID >= 50555 && MYSQL_VERSION_ID < 50600))
#define HAVE_SSL_MODE_ONLY_REQUIRED
#endif

/*
 * Check which SSL settings are supported by API at runtime
 */

/* MYSQL_OPT_SSL_VERIFY_SERVER_CERT automatically enforce SSL mode */
PERL_STATIC_INLINE bool ssl_verify_also_enforce_ssl(void) {
#ifdef MARIADB_BASE_VERSION
	my_ulonglong version = mysql_get_client_version();
	return ((version >= 50544 && version < 50600) || (version >= 100020 && version < 100100) || version >= 100106);
#else
	return false;
#endif
}

/* MYSQL_OPT_SSL_VERIFY_SERVER_CERT is not vulnerable (CVE-2016-2047) and can be used */
PERL_STATIC_INLINE bool ssl_verify_usable(void) {
	my_ulonglong version = mysql_get_client_version();
#ifdef MARIADB_BASE_VERSION
	return ((version >= 50547 && version < 50600) || (version >= 100023 && version < 100100) || version >= 100110);
#else
	return ((version >= 50549 && version < 50600) || (version >= 50630 && version < 50700) || version >= 50712);
#endif
}

/*
 *  The following are return codes passed in $h->err in case of
 *  errors by DBD::MariaDB.
 */
enum errMsgs {
    JW_ERR_CONNECT = 1,
    JW_ERR_SELECT_DB,
    JW_ERR_STORE_RESULT,
    JW_ERR_NOT_ACTIVE,
    JW_ERR_QUERY,
    JW_ERR_FETCH_ROW,
    JW_ERR_LIST_DB,
    JW_ERR_CREATE_DB,
    JW_ERR_DROP_DB,
    JW_ERR_LIST_TABLES,
    JW_ERR_LIST_FIELDS,
    JW_ERR_LIST_FIELDS_INT,
    JW_ERR_LIST_SEL_FIELDS,
    JW_ERR_NO_RESULT,
    JW_ERR_NOT_IMPLEMENTED,
    JW_ERR_ILLEGAL_PARAM_NUM,
    JW_ERR_MEM,
    JW_ERR_LIST_INDEX,
    JW_ERR_SEQUENCE,
    AS_ERR_EMBEDDED,
    TX_ERR_AUTOCOMMIT,
    TX_ERR_COMMIT,
    TX_ERR_ROLLBACK,
    JW_ERR_INVALID_ATTRIBUTE
};


/*
 *  Internal constants, used for fetching array attributes
 */
enum av_attribs {
    AV_ATTRIB_NAME = 0,
    AV_ATTRIB_TABLE,
    AV_ATTRIB_TYPE,
    AV_ATTRIB_SQL_TYPE,
    AV_ATTRIB_IS_PRI_KEY,
    AV_ATTRIB_IS_NOT_NULL,
    AV_ATTRIB_NULLABLE,
    AV_ATTRIB_LENGTH,
    AV_ATTRIB_IS_NUM,
    AV_ATTRIB_TYPE_NAME,
    AV_ATTRIB_PRECISION,
    AV_ATTRIB_SCALE,
    AV_ATTRIB_MAX_LENGTH,
    AV_ATTRIB_IS_KEY,
    AV_ATTRIB_IS_BLOB,
    AV_ATTRIB_IS_AUTO_INCREMENT,
    AV_ATTRIB_LAST         /*  Dummy attribute, never used, for allocation  */
};                         /*  purposes only                                */


/*
 *  This is our part of the driver handle. We receive the handle as
 *  an "SV*", say "drh", and receive a pointer to the structure below
 *  by declaring
 *
 *    D_imp_drh(drh);
 *
 *  This declares a variable called "imp_drh" of type
 *  "struct imp_drh_st *".
 */
typedef struct imp_drh_embedded_st {
    int state;
    SV * args;
    SV * groups;
} imp_drh_embedded_t;

struct imp_drh_st {
    dbih_drc_t com;         /* MUST be first element in structure   */
#if defined(DBD_MYSQL_EMBEDDED)
    imp_drh_embedded_t embedded;     /* */
#endif
};


/*
 *  Likewise, this is our part of the database handle, as returned
 *  by DBI->connect. We receive the handle as an "SV*", say "dbh",
 *  and receive a pointer to the structure below by declaring
 *
 *    D_imp_dbh(dbh);
 *
 *  This declares a variable called "imp_dbh" of type
 *  "struct imp_dbh_st *".
 */
struct imp_dbh_st {
    dbih_dbc_t com;         /*  MUST be first element in structure   */

    MYSQL *pmysql;
    bool connected;          /* Set to true after DBI->connect finished */
    bool has_transactions;   /*  boolean indicating support for
			     *  transactions, currently always  TRUE for MySQL
			     */
    bool auto_reconnect;
    bool bind_type_guessing;
    bool bind_comment_placeholders;
    bool no_autocommit_cmd;
    bool use_mysql_use_result; /* TRUE if execute should use
                               * mysql_use_result rather than
                               * mysql_store_result
                               */
    bool use_server_side_prepare;
    bool disable_fallback_for_server_prepare;
    void* async_query_in_flight;
    struct {
	    unsigned int auto_reconnects_ok;
	    unsigned int auto_reconnects_failed;
    } stats;
};


/*
 *  The bind_param method internally uses this structure for storing
 *  parameters.
 */
typedef struct imp_sth_ph_st {
    char* value;
    STRLEN len;
    int type;
    bool bound;
} imp_sth_ph_t;

/*
 *  Storage for numeric value in prepared statement
 */
typedef union numeric_val_u {
    unsigned char tval;
    unsigned short sval;
    uint32_t lval;
    my_ulonglong llval;
    float fval;
    double dval;
} numeric_val_t;

/*
 *  The bind_param method internally uses this structure for storing
 *  parameters.
 */
typedef struct imp_sth_phb_st {
    numeric_val_t   numeric_val;
    unsigned long   length;
    my_bool         is_null;
} imp_sth_phb_t;

/*
 *  The mariadb_st_describe uses this structure for storing
 *  fields meta info.
 */
typedef struct imp_sth_fbh_st {
    unsigned long  length;
    my_bool        is_null;
    bool           error;
    char           *data;
    numeric_val_t  numeric_val;
    bool           is_utf8;
} imp_sth_fbh_t;


typedef struct imp_sth_fbind_st {
   unsigned long   * length;
   my_bool         * is_null;
} imp_sth_fbind_t;


/*
 *  Finally our part of the statement handle. We receive the handle as
 *  an "SV*", say "dbh", and receive a pointer to the structure below
 *  by declaring
 *
 *    D_imp_sth(sth);
 *
 *  This declares a variable called "imp_sth" of type
 *  "struct imp_sth_st *".
 */
struct imp_sth_st {
    dbih_stc_t com;       /* MUST be first element in structure     */
    char *statement;
    STRLEN statement_len;

#if (MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION)
    MYSQL_STMT       *stmt;
    MYSQL_BIND       *bind;
    MYSQL_BIND       *buffer;
    imp_sth_phb_t    *fbind;
    imp_sth_fbh_t    *fbh;
    int              has_been_bound;
    int use_server_side_prepare;  /* server side prepare statements? */
    int disable_fallback_for_server_prepare;
#endif

    MYSQL_RES* result;       /* result                                 */
    int currow;           /* number of current row                  */
    int fetch_done;       /* mark that fetch done                   */
    my_ulonglong row_num;         /* total number of rows                   */

    int   done_desc;      /* have we described this sth yet ?	    */
    long  long_buflen;    /* length for long/longraw (if >0)	    */
    bool  long_trunc_ok;  /* is truncating a long an error	    */
    my_ulonglong insertid; /* ID of auto insert                      */
    int   warning_count;  /* Number of warnings after execute()     */
    imp_sth_ph_t* params; /* Pointer to parameter array             */
    AV* av_attr[AV_ATTRIB_LAST];/*  For caching array attributes        */
    int   use_mysql_use_result;  /*  TRUE if execute should use     */
                          /* mysql_use_result rather than           */
                          /* mysql_store_result */

    bool is_async;
};


/*
 *  And last, not least: The prototype definitions.
 *
 * These defines avoid name clashes for multiple statically linked DBD's	*/
#define dbd_init		mariadb_dr_init
#define dbd_discon_all		mariadb_dr_discon_all
#define dbd_db_login6_sv	mariadb_db_login6_sv
#define dbd_db_commit		mariadb_db_commit
#define dbd_db_rollback		mariadb_db_rollback
#define dbd_db_disconnect	mariadb_db_disconnect
#define dbd_db_destroy		mariadb_db_destroy
#define dbd_db_STORE_attrib	mariadb_db_STORE_attrib
#define dbd_db_FETCH_attrib	mariadb_db_FETCH_attrib
#define dbd_db_last_insert_id   mariadb_db_last_insert_id
#define dbd_db_data_sources	mariadb_db_data_sources
#define dbd_st_prepare_sv	mariadb_st_prepare_sv
#define dbd_st_execute		mariadb_st_execute
#define dbd_st_fetch		mariadb_st_fetch
#define dbd_st_finish		mariadb_st_finish
#define dbd_st_destroy		mariadb_st_destroy
#define dbd_st_blob_read	mariadb_st_blob_read
#define dbd_st_STORE_attrib	mariadb_st_STORE_attrib
#define dbd_st_FETCH_attrib	mariadb_st_FETCH_attrib
#define dbd_bind_ph		mariadb_st_bind_ph

#include <dbd_xsh.h>

void    mariadb_dr_do_error (SV* h, int rc, const char *what, const char *sqlstate);

my_ulonglong mariadb_st_internal_execute(SV *,
                                       char *,
                                       STRLEN,
                                       SV *,
                                       int,
                                       imp_sth_ph_t *,
                                       MYSQL_RES **,
                                       MYSQL *,
                                       int);

#if MYSQL_VERSION_ID >= SERVER_PREPARE_VERSION
my_ulonglong mariadb_st_internal_execute41(SV *,
                                         int,
                                         MYSQL_RES **,
                                         MYSQL_STMT *,
                                         MYSQL_BIND *,
                                         int *);
#endif

#if MYSQL_VERSION_ID >= MULTIPLE_RESULT_SET_VERSION
int mariadb_st_more_results(SV*, imp_sth_t*);
#endif

AV* mariadb_db_type_info_all (SV* dbh, imp_dbh_t* imp_dbh);
SV* mariadb_db_quote(SV*, SV*, SV*);
MYSQL* mariadb_dr_connect(SV*, MYSQL*, char*, char*, char*, char*, char*,
			       char*, imp_dbh_t*);

int mariadb_db_reconnect(SV*);

int mariadb_db_async_result(SV* h, MYSQL_RES** resp);
int mariadb_db_async_ready(SV* h);

int mariadb_dr_socket_ready(my_socket fd);
