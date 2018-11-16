/*
 *  DBD::MariaDB - DBI driver for the MariaDB and MySQL database
 *
 *  Copyright (c) 2018      GoodData Corporation
 *  Copyright (c) 2015-2017 Pali Rohár
 *  Copyright (c) 2004-2017 Patrick Galbraith
 *  Copyright (c) 2013-2017 Michiel Beijen 
 *  Copyright (c) 2004-2007 Alexey Stroganov 
 *  Copyright (c) 2003-2005  Rudolf Lippan
 *  Copyright (c) 1997-2003  Jochen Wiedmann
 *
 *  You may distribute this under the terms of either the GNU General Public
 *  License or the Artistic License, as specified in the Perl README file.
 */

#include "dbdimp.h"

#define ASYNC_CHECK_RETURN(h, value)\
  if(imp_dbh->async_query_in_flight) {\
      mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Calling a synchronous function on an asynchronous handle", "HY000");\
      return (value);\
  }

static bool is_mysql_number(char *string, STRLEN len);

DBISTATE_DECLARE;

typedef struct sql_type_info_s
{
    const char *type_name;
    int data_type;
    int column_size;
    const char *literal_prefix;
    const char *literal_suffix;
    const char *create_params;
    int nullable;
    int case_sensitive;
    int searchable;
    int unsigned_attribute;
    int fixed_prec_scale;
    int auto_unique_value;
    const char *local_type_name;
    int minimum_scale;
    int maximum_scale;
    int num_prec_radix;
    int sql_datatype;
    int sql_datetime_sub;
    int interval_precision;
    int native_type;
    bool is_num;
} sql_type_info_t;


/*

  This function manually counts the number of placeholders in an SQL statement,
  used for emulated prepare statements.

*/
static unsigned long int
count_params(imp_dbh_t *imp_dbh, pTHX_ char *statement, STRLEN statement_len, bool bind_comment_placeholders)
{
  bool comment_end = FALSE;
  char* ptr= statement;
  unsigned long int num_params = 0;
  int comment_length= 0;
  char *end = statement + statement_len;
  char c;

  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh), ">count_params statement %.1000s%s\n", statement, statement_len > 1000 ? "..." : "");

  while (ptr < end)
  {
    c = *ptr++;
    switch (c) {
      /* so, this is a -- comment, so let's burn up characters */
    case '-':
      {
          if (ptr >= end)
              break;
          if (bind_comment_placeholders)
          {
              ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              /* let's see if the next one is a dash */
              c = *ptr++;
              if  (c == '-') {
                  /* if two dashes, ignore everything until newline */
                  while (ptr < end)
                  {
                      c = *ptr++;
                      if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
                          PerlIO_printf(DBIc_LOGPIO(imp_dbh), "%c\n", c);
                      comment_length++;
                      if (c == '\n')
                      {
                          comment_end = TRUE;
                          break;
                      }
                  }
                  /*
                    if not comment_end, the comment never ended and we need to iterate
                    back to the beginning of where we started and let the database 
                    handle whatever is in the statement
                */
                  if (! comment_end)
                      ptr-= comment_length;
              }
              /* otherwise, only one dash/hyphen, backtrack by one */
              else
                  ptr--;
              break;
          }
      }
    /* c-type comments */
    case '/':
      {
          if (ptr >= end)
              break;
          if (bind_comment_placeholders)
          {
              ptr++;
              break;
          }
          else
          {
              c = *ptr++;
              /* let's check if the next one is an asterisk */
              if  (c == '*')
              {
                  comment_length= 0;
                  comment_end = FALSE;
                  /* ignore everything until closing comment */
                  while (ptr < end)
                  {
                      c = *ptr++;
                      comment_length++;

                      if (c == '*' && ptr < end)
                      {
                          c = *ptr++;
                          /* alas, end of comment */
                          if (c == '/')
                          {
                              comment_end = TRUE;
                              break;
                          }
                          /*
                            nope, just an asterisk, not so fast, not
                            end of comment, go back one
                        */
                          else
                              ptr--;
                      }
                  }
                  /*
                    if the end of the comment was never found, we have
                    to backtrack to wherever we first started skipping
                    over the possible comment.
                    This means we will pass the statement to the database
                    to see its own fate and issue the error
                */
                  if (!comment_end)
                      ptr -= comment_length;
              }
              else
                  ptr--;
              break;
          }
      }
    case '`':
    case '"':
    case '\'':
      /* Skip string */
      {
        char end_token = c;
        if (ptr >= end)
          break;
        while (ptr < end && *ptr != end_token)
        {
          if (*ptr == '\\' && ptr+1 < end)
            ++ptr;
          ++ptr;
        }
        if (ptr < end)
          ++ptr;
        break;
      }

    case '?':
      ++num_params;
      if (num_params == ULONG_MAX)
        return ULONG_MAX;
      break;

    default:
      break;
    }
  }
  return num_params;
}

/*
  allocate memory in statement handle per number of placeholders
*/
static imp_sth_ph_t *alloc_param(int num_params)
{
  imp_sth_ph_t *params;

  if (num_params)
    Newz(908, params, num_params, imp_sth_ph_t);
  else
    params= NULL;

  return params;
}


/*
  allocate memory in MYSQL_BIND bind structure per
  number of placeholders
*/
static MYSQL_BIND *alloc_bind(int num_params)
{
  MYSQL_BIND *bind;

  if (num_params)
    Newz(908, bind, num_params, MYSQL_BIND);
  else
    bind= NULL;

  return bind;
}

/*
  allocate memory in fbind imp_sth_phb_t structure per
  number of placeholders
*/
static imp_sth_phb_t *alloc_fbind(int num_params)
{
  imp_sth_phb_t *fbind;

  if (num_params)
    Newz(908, fbind, num_params, imp_sth_phb_t);
  else
    fbind= NULL;

  return fbind;
}

/*
  alloc memory for imp_sth_fbh_t fbuffer per number of fields
*/
static imp_sth_fbh_t *alloc_fbuffer(int num_fields)
{
  imp_sth_fbh_t *fbh;

  if (num_fields)
    Newz(908, fbh, num_fields, imp_sth_fbh_t);
  else
    fbh= NULL;

  return fbh;
}

/*
  free MYSQL_BIND bind struct
*/
static void free_bind(MYSQL_BIND *bind)
{
  if (bind)
    Safefree(bind);
}

/*
   free imp_sth_phb_t fbind structure
*/
static void free_fbind(imp_sth_phb_t *fbind)
{
  if (fbind)
    Safefree(fbind);
}

/*
  free imp_sth_fbh_t fbh structure
*/
static void free_fbuffer(imp_sth_fbh_t *fbh)
{
  if (fbh)
    Safefree(fbh);
}

/*
  free statement param structure per num_params
*/
static void
free_param(pTHX_ imp_sth_ph_t *params, int num_params)
{
  if (params)
  {
    int i;
    for (i= 0;  i < num_params;  i++)
    {
      imp_sth_ph_t *ph= params+i;
      if (ph->value)
        Safefree(ph->value);
    }
    Safefree(params);
  }
}

enum perl_type {
  PERL_TYPE_UNDEF,
  PERL_TYPE_INTEGER,
  PERL_TYPE_NUMERIC,
  PERL_TYPE_BINARY,
  PERL_TYPE_STRING
};

/* 
  Convert a MySQL type to a type that perl can handle
*/
static enum perl_type mysql_to_perl_type(enum enum_field_types type)
{
  switch (type) {
  case MYSQL_TYPE_NULL:
    return PERL_TYPE_UNDEF;

  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_LONG:
#if IVSIZE >= 8
  case MYSQL_TYPE_LONGLONG:
#endif
  case MYSQL_TYPE_YEAR:
    return PERL_TYPE_INTEGER;

  case MYSQL_TYPE_FLOAT:
#if NVSIZE >= 8
  case MYSQL_TYPE_DOUBLE:
#endif
    return PERL_TYPE_NUMERIC;

  case MYSQL_TYPE_BIT:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_LONG_BLOB:
    return PERL_TYPE_BINARY;

  default:
    return PERL_TYPE_STRING;
  }
}

/*
  Convert a DBI SQL type to a MySQL type for prepared statement storage
  See: http://dev.mysql.com/doc/refman/5.7/en/c-api-prepared-statement-type-codes.html
*/
static enum enum_field_types sql_to_mysql_type(IV sql_type)
{
  switch (sql_type) {
  case SQL_BOOLEAN:
  case SQL_TINYINT:
    return MYSQL_TYPE_TINY;
  case SQL_SMALLINT:
    return MYSQL_TYPE_SHORT;
  case SQL_INTEGER:
    return MYSQL_TYPE_LONG;
  case SQL_BIGINT:
    return MYSQL_TYPE_LONGLONG;

  case SQL_FLOAT:
    return MYSQL_TYPE_FLOAT;
  case SQL_DOUBLE:
  case SQL_REAL:
    return MYSQL_TYPE_DOUBLE;

  /* TODO: datetime structures */
#if 0
  case SQL_TIME:
    return MYSQL_TYPE_TIME;
  case SQL_DATE:
    return MYSQL_TYPE_DATE;
  case SQL_DATETIME:
    return MYSQL_TYPE_DATETIME;
  case SQL_TIMESTAMP:
    return MYSQL_TYPE_TIMESTAMP;
#endif

  case SQL_BIT:
  case SQL_BLOB:
  case SQL_BINARY:
  case SQL_VARBINARY:
  case SQL_LONGVARBINARY:
    return MYSQL_TYPE_BLOB;

  default:
    return MYSQL_TYPE_STRING;
  }
}

/*
  Returns true if MySQL type for prepared statement storage uses dynamically allocated buffer
*/
static bool mysql_type_needs_allocated_buffer(enum enum_field_types type)
{
  switch (type) {
  case MYSQL_TYPE_NULL:
  case MYSQL_TYPE_TINY:
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_LONG:
  case MYSQL_TYPE_LONGLONG:
  case MYSQL_TYPE_FLOAT:
  case MYSQL_TYPE_DOUBLE:
    return FALSE;

  default:
    return TRUE;
  }
}

/*
  Numeric types with leading zeros or with fixed length of decimals in fractional part cannot be represented by IV or NV
*/
static bool mysql_field_needs_string_type(MYSQL_FIELD *field)
{
  if (field->flags & ZEROFILL_FLAG)
    return TRUE;
  if ((field->type == MYSQL_TYPE_FLOAT || field->type == MYSQL_TYPE_DOUBLE) && field->decimals < NOT_FIXED_DEC)
    return TRUE;
  return FALSE;
}

/*
  Allocated buffer is needed by all non-primitive types (which have non-fixed length)
 */
static bool mysql_field_needs_allocated_buffer(MYSQL_FIELD *field)
{
  if (mysql_type_needs_allocated_buffer(field->type) || mysql_field_needs_string_type(field))
    return TRUE;
  else
    return FALSE;
}

/*
  Returns true if DBI SQL type should be treated as binary sequence of octets, not UNICODE string
*/
static bool sql_type_is_binary(IV sql_type)
{
  switch (sql_type) {
  case SQL_BIT:
  case SQL_BLOB:
  case SQL_BINARY:
  case SQL_VARBINARY:
  case SQL_LONGVARBINARY:
    return TRUE;

  default:
    return FALSE;
  }
}

/*
  Returns true if DBI SQL type represents numeric value (regardless of how is stored)
*/
static bool sql_type_is_numeric(IV sql_type)
{
  switch (sql_type) {
  case SQL_BOOLEAN:
  case SQL_TINYINT:
  case SQL_SMALLINT:
  case SQL_INTEGER:
  case SQL_BIGINT:
  case SQL_FLOAT:
  case SQL_DOUBLE:
  case SQL_REAL:
  case SQL_NUMERIC:
  case SQL_DECIMAL:
    return TRUE;

  default:
    return FALSE;
  }
}

/*
  Check if attribute can be skipped by driver and handled by DBI itself
*/
static bool skip_attribute(const char *key)
{
  return strBEGINs(key, "private_") || strBEGINs(key, "dbd_") || strBEGINs(key, "dbi_") || isUPPER(*key);
}

PERL_STATIC_INLINE bool mysql_charsetnr_is_utf8(unsigned int id)
{
  /* Check if supplied id belongs to some UTF-8 related MySQL charset number */
  /* List of all utf8 charset numbers is harcoded in MySQL and MariaDB source code, to retrieve it grep source code */
  /* Shell fragment for MySQL or MariaDB server source code: grep -E '^(CHARSET_INFO|struct charset_info_st).*utf8' -A 2 -r strings | grep number | sed -E 's/^.*-  *([^,]+),.*$/\1/' | sort -n */
  /* Shell fragment for MariaDB Connector/C source code: sed -n '/mariadb_compiled_charsets\[\]/,/^};$/{/utf8/I{s%,.*%%;s%\s*{\s*%%p}}' libmariadb/ma_charset.c | sort -n */
  /* Runtime SQL statement (returns only subset selected at compile time): SELECT ID FROM INFORMATION_SCHEMA.COLLATIONS WHERE CHARACTER_SET_NAME LIKE 'utf8%' ORDER BY ID */
  return (id == 33 || id == 45 || id == 46 || id == 56 || id == 76 || id == 83 || (id >= 192 && id <= 215) || (id >= 223 && id <= 247) || (id >= 254 && id <= 307) || (id >= 576 && id <= 578)
      || (id >= 608 && id <= 610) || id == 1057 || (id >= 1069 && id <= 1070) || id == 1107 || id == 1216 || id == 1238 || id == 1248 || id == 1270);
}

/* 
  count embedded options
*/
int count_embedded_options(char *st)
{
  int rc;
  char c;
  char *ptr;

  ptr= st;
  rc= 0;

  if (st)
  {
    while ((c= *ptr++))
    {
      if (c == ',')
        rc++;
    }
    rc++;
  }

  return rc;
}

/*
  Free embedded options
*/
int free_embedded_options(char ** options_list, int options_count)
{
  int i;

  for (i= 0; i < options_count; i++)
  {
    if (options_list[i])
      Safefree(options_list[i]);
  }
  Safefree(options_list);

  return 1;
}

/*
 Print out embedded option settings

*/
int print_embedded_options(PerlIO *stream, char ** options_list, int options_count)
{
  int i;

  for (i=0; i<options_count; i++)
  {
    if (options_list[i])
        PerlIO_printf(stream,
                      "Embedded server, parameter[%d]=%s\n",
                      i, options_list[i]);
  }
  return 1;
}

/*

*/
char **fill_out_embedded_options(char *options,
                                 int options_type,
                                 STRLEN slen, int cnt)
{
  int  ind, len;
  char c;
  char *ptr;
  char **options_list= NULL;
  dTHX;

  Newz(908, options_list, cnt, char *);

  ptr= options;
  ind= 0;

  if (options_type == 0)
  {
    /* server_groups list NULL terminated */
    options_list[cnt]= (char *) NULL;
  }

  if (options_type == 1)
  {
    /* first item in server_options list is ignored. fill it with \0 */
    Newz(908, options_list[0], 1, char);
    ind++;
  }

  while ((c= *ptr++))
  {
    slen--;
    if (c == ',' || !slen)
    {
      len= ptr - options;
      if (c == ',')
        len--;
      options_list[ind] = savepvn(options, len);
      ind++;
      options= ptr;
    }
  }
  return options_list;
}

/*
  constructs an SQL statement previously prepared with
  actual values replacing placeholders
*/
static char *parse_params(
                          imp_xxh_t *imp_xxh,
                          pTHX_ MYSQL *sock,
                          char *statement,
                          STRLEN *slen_ptr,
                          imp_sth_ph_t* params,
                          int num_params,
                          bool bind_type_guessing,
                          bool bind_comment_placeholders)
{
  bool comment_end = FALSE;
  char *salloc, *statement_ptr;
  char *statement_ptr_end, *ptr;
  int i;
  STRLEN alen;
  STRLEN slen = *slen_ptr;
  bool limit_flag = FALSE;
  int comment_length=0;
  imp_sth_ph_t *ph;

  if (DBIc_DBISTATE(imp_xxh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), ">parse_params statement %.1000s%s\n", statement, slen > 1000 ? "..." : "");

  if (num_params == 0)
    return NULL;

  while (isspace(*statement))
  {
    ++statement;
    --slen;
  }

  /* Calculate the number of bytes being allocated for the statement */
  alen= slen;
  for (i= 0, ph= params; i < num_params; i++, ph++)
  {
    alen--; /* Erase '?' */
    if (!ph->value)
      alen += 4;  /* insert 'NULL' */
    else
      alen += 3 + 2*ph->len; /* 2 bytes for quotes, one for 'X' and in the worst case two bytes for each character */
  }

  /* +1 for null term byte */
  New(908, salloc, alen+1, char);
  ptr= salloc;

  i= 0;
 /* Now create the statement string; compare count_params above */
  statement_ptr_end= (statement_ptr= statement)+ slen;

  while (statement_ptr < statement_ptr_end)
  {
    /* LIMIT should be the last part of the query, in most cases */
    if (! limit_flag)
    {
      if (statement_ptr+4 < statement_ptr_end)
      {
        char *s = statement_ptr;
        if ((s[0] == 'l' || s[0] == 'L') &&
            (s[1] == 'i' || s[1] == 'I') &&
            (s[2] == 'm' || s[2] == 'M') &&
            (s[3] == 'i' || s[3] == 'I') &&
            (s[4] == 't' || s[4] == 'T'))
        {
          limit_flag = TRUE;
        }
      }
    }
    switch (*statement_ptr)
    {
      /* comment detection. Anything goes in a comment */
      case '-':
      {
          if (bind_comment_placeholders)
          {
              *ptr++= *statement_ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              comment_end = FALSE;
              *ptr++ = *statement_ptr++;
              if  (*statement_ptr == '-')
              {
                  /* ignore everything until newline or end of string */
                  while (*statement_ptr)
                  {
                      comment_length++;
                      *ptr++ = *statement_ptr++;
                      if (!*statement_ptr || *statement_ptr == '\n')
                      {
                          comment_end = TRUE;
                          break;
                      }
                  }
                  /* if not end of comment, go back to where we started, no end found */
                  if (! comment_end)
                  {
                      statement_ptr -= comment_length;
                      ptr -= comment_length;
                  }
              }
              break;
          }
      }
      /* c-type comments */
      case '/':
      {
          if (bind_comment_placeholders)
          {
              *ptr++= *statement_ptr++;
              break;
          }
          else
          {
              comment_length= 1;
              comment_end = FALSE;
              *ptr++ = *statement_ptr++;
              if  (*statement_ptr == '*')
              {
                  /* use up characters everything until newline */
                  while (*statement_ptr)
                  {
                      *ptr++ = *statement_ptr++;
                      comment_length++;
                      if (!strncmp(statement_ptr, "*/", 2))
                      {
                          comment_length += 2;
                          comment_end = TRUE;
                          break;
                      }
                  }
                  /* Go back to where started if comment end not found */
                  if (! comment_end)
                  {
                      statement_ptr -= comment_length;
                      ptr -= comment_length;
                  }
              }
              break;
          }
      }
      case '`':
      case '\'':
      case '"':
      /* Skip string */
      {
        char endToken = *statement_ptr++;
        *ptr++ = endToken;
        while (statement_ptr != statement_ptr_end &&
               *statement_ptr != endToken)
        {
          if (*statement_ptr == '\\')
          {
            *ptr++ = *statement_ptr++;
            if (statement_ptr == statement_ptr_end)
	      break;
	  }
          *ptr++= *statement_ptr++;
	}
	if (statement_ptr != statement_ptr_end)
          *ptr++= *statement_ptr++;
      }
      break;

      case '?':
        /* Insert parameter */
        statement_ptr++;
        if (i >= num_params)
        {
          break;
        }

        ph = params+ (i++);
        if (!ph->value)
        {
          *ptr++ = 'N';
          *ptr++ = 'U';
          *ptr++ = 'L';
          *ptr++ = 'L';
        }
        else
        {
          bool quote_value, is_value_num;

          is_value_num = is_mysql_number(ph->value, ph->len);

          if (limit_flag && is_value_num)
            /* After a LIMIT clause must be unquoted numeric value */
            quote_value = FALSE;
          else if (bind_type_guessing && !ph->type)
            /* If SQL type was not specified and bind_type_guessing is enabled, then quote only if needed */
            quote_value = !is_value_num;
          else if (sql_type_is_numeric(ph->type))
            /* If SQL type is numeric then quote only in case value is not numeric */
            quote_value = !is_value_num;
          else
            /* Otherwise always quote */
            quote_value = TRUE;

          if (quote_value)
          {
#if MYSQL_VERSION_ID < 50001
            if (sock->server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES)
            {
              *ptr++ = 'X';
              *ptr++ = '\'';
              ptr += mysql_hex_string(ptr, ph->value, ph->len);
              *ptr++ = '\'';
            }
            else
#endif
            {
            *ptr++ = '\'';
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50706 && MYSQL_VERSION_ID != 60000
            ptr += mysql_real_escape_string_quote(sock, ptr, ph->value, ph->len, '\'');
#else
            ptr += mysql_real_escape_string(sock, ptr, ph->value, ph->len);
#endif
            *ptr++ = '\'';
            }
          }
          else
          {
            memcpy(ptr, ph->value, ph->len);
            ptr += ph->len;
          }
        }
        break;

	/* in case this is a nested LIMIT */
      case ')':
        limit_flag = FALSE;
	*ptr++ = *statement_ptr++;
        break;

      default:
        *ptr++ = *statement_ptr++;
        break;

    }
  }

  *slen_ptr = ptr - salloc;
  *ptr++ = '\0';

  return(salloc);
}

static void bind_param(imp_sth_ph_t *ph, SV *value, IV sql_type)
{
  dTHX;
  char *buf;

  if (ph->value)
  {
    Safefree(ph->value);
    ph->value = NULL;
  }

  ph->bound = TRUE;

  if (sql_type)
    ph->type = sql_type;

  if (SvOK(value))
  {
    if (sql_type_is_binary(ph->type))
      buf = SvPVbyte_nomg(value, ph->len); /* Ensure that buf is always byte orientated */
    else
      buf = SvPVutf8_nomg(value, ph->len); /* Ensure that buf is always UTF-8 encoded */
    ph->value = savepvn(buf, ph->len);
  }
}

static const sql_type_info_t SQL_GET_TYPE_INFO_values[]= {
  { "varchar",    SQL_VARCHAR,                    255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "variable length string",
    0, 0, 0,
    SQL_VARCHAR, 0, 0,
    MYSQL_TYPE_STRING,  0,
  },
  { "decimal",   SQL_DECIMAL,                      15, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_DECIMAL, 0, 0,
    MYSQL_TYPE_DECIMAL,     1
  },
  { "tinyint",   SQL_TINYINT,                       3, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Tiny integer",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    MYSQL_TYPE_TINY,     1
  },
  { "smallint",  SQL_SMALLINT,                      5, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Short integer",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    MYSQL_TYPE_SHORT,     1
  },
  { "integer",   SQL_INTEGER,                      10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_LONG,     1
  },
  { "float",     SQL_FLOAT,                         7, NULL, NULL, NULL,
    1, 0, 0, 0, 0, 0, "float",
    0, 2, 10,
    SQL_FLOAT, 0, 0,
    MYSQL_TYPE_FLOAT,     1
  },
  { "null",      SQL_CHAR,                          0, NULL, NULL, NULL,
    1, 0, 0, 0, 0, 1, "null",
    0, 0, 0,
    SQL_CHAR, 0, 0,
    MYSQL_TYPE_NULL,      1
  },
  { "double",    SQL_DOUBLE,                       15,  NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "double",
    0, 4, 10,
    SQL_DOUBLE, 0, 0,
    MYSQL_TYPE_DOUBLE,     1
  },
  { "timestamp", SQL_TIMESTAMP,                    14, "'", "'", NULL,
    0, 0, 3, 0, 0, 0, "timestamp",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    MYSQL_TYPE_TIMESTAMP,     0
  },
  { "bigint",    SQL_BIGINT,                       19, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Longlong integer",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    MYSQL_TYPE_LONGLONG,     1
  },
  { "mediumint", SQL_INTEGER,                       8, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_INT24,     1
  },
  { "date", SQL_DATE, 10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
    MYSQL_TYPE_DATE, 0
  },
  { "time", SQL_TIME, 6, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "time",
    0, 0, 0,
    SQL_TIME, 0, 0,
    MYSQL_TYPE_TIME,     0
  },
  { "datetime",  SQL_TIMESTAMP, 21, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "datetime",
    0, 0, 0,
    SQL_TIMESTAMP, 0, 0,
    MYSQL_TYPE_DATETIME,     0
  },
  { "year", SQL_SMALLINT, 4, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "year",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    MYSQL_TYPE_YEAR,     1
  },
  { "date", SQL_DATE, 10, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "date",
    0, 0, 0,
    SQL_DATE, 0, 0,
    MYSQL_TYPE_NEWDATE,     0
  },
  { "enum",      SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "enum(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
    MYSQL_TYPE_ENUM,     0
  },
  { "set",       SQL_VARCHAR,                     255, "'",  "'",  NULL,
    1, 0, 1, 0, 0, 0, "set(value1,value2,value3...)",
    0, 0, 0,
    0, 0, 0,
    MYSQL_TYPE_SET,     0
  },
  { "blob",       SQL_LONGVARBINARY,              65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-65535)",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    MYSQL_TYPE_BLOB,     0
  },
  { "tinyblob",  SQL_VARBINARY,                 255, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object (0-255) ",
    0, 0, 0,
    SQL_VARBINARY, 0, 0,
    MYSQL_TYPE_TINY_BLOB,        0
  },
  { "mediumblob", SQL_LONGVARBINARY,           16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    MYSQL_TYPE_MEDIUM_BLOB, 0
  },
  { "longblob",   SQL_LONGVARBINARY,         2147483647, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "binary large object, use mediumblob instead",
    0, 0, 0,
    SQL_LONGVARBINARY, 0, 0,
    MYSQL_TYPE_LONG_BLOB,   0
  },
  { "char",       SQL_CHAR,                       255, "'",  "'",  "max length",
    1, 0, 3, 0, 0, 0, "string",
    0, 0, 0,
    SQL_CHAR, 0, 0,
    MYSQL_TYPE_STRING,   0
  },

  { "decimal",            SQL_NUMERIC,            15,  NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "double",
    0, 6, 2,
    SQL_NUMERIC, 0, 0,
    MYSQL_TYPE_DECIMAL,   1 
  },
  { "tinyint unsigned",   SQL_TINYINT,              3, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Tiny integer unsigned",
    0, 0, 10,
    SQL_TINYINT, 0, 0,
    MYSQL_TYPE_TINY,        1
  },
  { "smallint unsigned",  SQL_SMALLINT,             5, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Short integer unsigned",
    0, 0, 10,
    SQL_SMALLINT, 0, 0,
    MYSQL_TYPE_SHORT,       1
  },
  { "mediumint unsigned", SQL_INTEGER,              8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_INT24,       1
  },
  { "int unsigned",       SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer unsigned",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_LONG,        1
  },
  { "int",                SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_LONG,        1
  },
  { "integer unsigned",   SQL_INTEGER,             10, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "integer",
    0, 0, 10,
    SQL_INTEGER, 0, 0,
    MYSQL_TYPE_LONG,        1
  },
  { "bigint unsigned",    SQL_BIGINT,              20, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Longlong integer unsigned",
    0, 0, 10,
    SQL_BIGINT, 0, 0,
    MYSQL_TYPE_LONGLONG,    1
  },
  { "text",               SQL_LONGVARCHAR,      65535, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object (0-65535)",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    MYSQL_TYPE_BLOB,        0
  },
  { "mediumtext",         SQL_LONGVARCHAR,   16777215, "'",  "'",  NULL,
    1, 0, 3, 0, 0, 0, "large text object",
    0, 0, 0,
    SQL_LONGVARCHAR, 0, 0,
    MYSQL_TYPE_MEDIUM_BLOB, 0
  },
  { "mediumint unsigned auto_increment", SQL_INTEGER, 8, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "Medium integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1,
  },
  { "tinyint unsigned auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "tinyint unsigned auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, MYSQL_TYPE_TINY, 1
  },

  { "smallint auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "smallint auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, MYSQL_TYPE_SHORT, 1
  },

  { "int unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1
  },

  { "mediumint", SQL_INTEGER, 7, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "Medium integer", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
  },

  { "bit", SQL_BIT, 1, NULL, NULL, NULL,
    1, 0, 3, 0, 0, 0, "bit", 0, 0, 0,
    SQL_BIT, 0, 0,
#if MYSQL_VERSION_ID >= NEW_DATATYPE_VERSION
    MYSQL_TYPE_BIT,
#else
    MYSQL_TYPE_TINY,
#endif
    0
  },

  { "numeric", SQL_NUMERIC, 19, NULL, NULL, "precision,scale",
    1, 0, 3, 0, 0, 0, "numeric", 0, 19, 10,
    SQL_NUMERIC, 0, 0, MYSQL_TYPE_DECIMAL, 1,
  },

  { "integer unsigned auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "integer unsigned auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1,
  },

  { "mediumint unsigned", SQL_INTEGER, 8, NULL, NULL, NULL,
    1, 0, 3, 1, 0, 0, "Medium integer unsigned", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
  },

  { "smallint unsigned auto_increment", SQL_SMALLINT, 5, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "smallint unsigned auto_increment", 0, 0, 10,
    SQL_SMALLINT, 0, 0, MYSQL_TYPE_SHORT, 1
  },

  { "int auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1
  },

  { "long varbinary", SQL_LONGVARBINARY, 16777215, "0x", NULL, NULL,
    1, 0, 3, 0, 0, 0, "mediumblob", 0, 0, 0,
    SQL_LONGVARBINARY, 0, 0, MYSQL_TYPE_LONG_BLOB, 0
  },

  { "double auto_increment", SQL_FLOAT, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 2,
    SQL_FLOAT, 0, 0, MYSQL_TYPE_DOUBLE, 1
  },

  { "double auto_increment", SQL_DOUBLE, 15, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "double auto_increment", 0, 4, 10,
    SQL_DOUBLE, 0, 0, MYSQL_TYPE_DOUBLE, 1
  },

  { "integer auto_increment", SQL_INTEGER, 10, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_LONG, 1,
  },

  { "bigint auto_increment", SQL_BIGINT, 19, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "bigint auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, MYSQL_TYPE_LONGLONG,
#if IVSIZE < 8
    0
#else
    1
#endif
  },

  { "bit auto_increment", SQL_BIT, 1, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "bit auto_increment", 0, 0, 0,
    SQL_BIT, 0, 0,
#if MYSQL_VERSION_ID >= NEW_DATATYPE_VERSION
    MYSQL_TYPE_BIT,
#else
    MYSQL_TYPE_TINY,
#endif
    0
  },

  { "mediumint auto_increment", SQL_INTEGER, 7, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "Medium integer auto_increment", 0, 0, 10,
    SQL_INTEGER, 0, 0, MYSQL_TYPE_INT24, 1
  },

  { "float auto_increment", SQL_REAL, 7, NULL, NULL, NULL,
    0, 0, 0, 0, 0, 1, "float auto_increment", 0, 2, 10,
    SQL_FLOAT, 0, 0, MYSQL_TYPE_FLOAT, 1
  },

  { "long varchar", SQL_LONGVARCHAR, 16777215, "'", "'", NULL,
    1, 0, 3, 0, 0, 0, "mediumtext", 0, 0, 0,
    SQL_LONGVARCHAR, 0, 0, MYSQL_TYPE_MEDIUM_BLOB, 1
  },

  { "tinyint auto_increment", SQL_TINYINT, 3, NULL, NULL, NULL,
    0, 0, 3, 0, 0, 1, "tinyint auto_increment", 0, 0, 10,
    SQL_TINYINT, 0, 0, MYSQL_TYPE_TINY, 1
  },

  { "bigint unsigned auto_increment", SQL_BIGINT, 20, NULL, NULL, NULL,
    0, 0, 3, 1, 0, 1, "bigint unsigned auto_increment", 0, 0, 10,
    SQL_BIGINT, 0, 0, MYSQL_TYPE_LONGLONG,
#if IVSIZE < 8
    0
#else
    1
#endif
  },

/* END MORE STUFF */
};

/* 
  static const sql_type_info_t* native2sql (int t)
*/
static const sql_type_info_t *native2sql(int t)
{
  switch (t) {
    case MYSQL_TYPE_VAR_STRING:  return &SQL_GET_TYPE_INFO_values[0];
    case MYSQL_TYPE_DECIMAL:     return &SQL_GET_TYPE_INFO_values[1];
    case MYSQL_TYPE_NEWDECIMAL:  return &SQL_GET_TYPE_INFO_values[1];
    case MYSQL_TYPE_TINY:        return &SQL_GET_TYPE_INFO_values[2];
    case MYSQL_TYPE_SHORT:       return &SQL_GET_TYPE_INFO_values[3];
    case MYSQL_TYPE_LONG:        return &SQL_GET_TYPE_INFO_values[4];
    case MYSQL_TYPE_FLOAT:       return &SQL_GET_TYPE_INFO_values[5];
    case MYSQL_TYPE_NULL:        return &SQL_GET_TYPE_INFO_values[6];
    case MYSQL_TYPE_DOUBLE:      return &SQL_GET_TYPE_INFO_values[7];
    case MYSQL_TYPE_TIMESTAMP:   return &SQL_GET_TYPE_INFO_values[8];
    case MYSQL_TYPE_LONGLONG:    return &SQL_GET_TYPE_INFO_values[9];
    case MYSQL_TYPE_INT24:       return &SQL_GET_TYPE_INFO_values[10];
    case MYSQL_TYPE_DATE:        return &SQL_GET_TYPE_INFO_values[11];
    case MYSQL_TYPE_TIME:        return &SQL_GET_TYPE_INFO_values[12];
    case MYSQL_TYPE_DATETIME:    return &SQL_GET_TYPE_INFO_values[13];
    case MYSQL_TYPE_YEAR:        return &SQL_GET_TYPE_INFO_values[14];
    case MYSQL_TYPE_NEWDATE:     return &SQL_GET_TYPE_INFO_values[15];
    case MYSQL_TYPE_ENUM:        return &SQL_GET_TYPE_INFO_values[16];
    case MYSQL_TYPE_SET:         return &SQL_GET_TYPE_INFO_values[17];
    case MYSQL_TYPE_BLOB:        return &SQL_GET_TYPE_INFO_values[18];
    case MYSQL_TYPE_TINY_BLOB:   return &SQL_GET_TYPE_INFO_values[19];
    case MYSQL_TYPE_MEDIUM_BLOB: return &SQL_GET_TYPE_INFO_values[20];
    case MYSQL_TYPE_LONG_BLOB:   return &SQL_GET_TYPE_INFO_values[21];
    case MYSQL_TYPE_STRING:      return &SQL_GET_TYPE_INFO_values[22];
    case MYSQL_TYPE_BIT:         return &SQL_GET_TYPE_INFO_values[38];
    default:                     return &SQL_GET_TYPE_INFO_values[0];
  }
}


#define SQL_GET_TYPE_INFO_num \
	(sizeof(SQL_GET_TYPE_INFO_values)/sizeof(sql_type_info_t))


/***************************************************************************
 *
 *  Name:    mariadb_dr_init
 *
 *  Purpose: Called when the driver is installed by DBI
 *
 *  Input:   dbistate - pointer to the DBI state variable, used for some
 *               DBI internal things
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void mariadb_dr_init(dbistate_t* dbistate)
{
    dTHX;
    DBISTATE_INIT;
    PERL_UNUSED_ARG(dbistate);
}


/**************************************************************************
 *
 *  Name:    mariadb_dr_do_error
 *
 *  Purpose: Called to associate an error code and an error message
 *           to some handle
 *
 *  Input:   h - the handle in error condition
 *           rc - the error code
 *           what - the error message
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void mariadb_dr_do_error(SV* h, unsigned int rc, const char *what, const char *sqlstate)
{
  dTHX;
  D_imp_xxh(h);
  SV *errstr;
  SV *errstate;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\t--> mariadb_dr_do_error\n");
  errstr= DBIc_ERRSTR(imp_xxh);
  sv_setuv(DBIc_ERR(imp_xxh), rc);	/* set err early	*/
  SvUTF8_off(errstr);
  sv_setpv(errstr, what);
  sv_utf8_decode(errstr);

  if (sqlstate)
  {
    errstate= DBIc_STATE(imp_xxh);
    sv_setpv(errstate, sqlstate);
  }

  /* NO EFFECT DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), errstr); */
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "error %u recorded: %" SVf "\n", rc, SVfARG(errstr));
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\t<-- mariadb_dr_do_error\n");
}

static void error_unknown_attribute(SV *h, const char *key)
{
  dTHX;
  mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, SvPVX(sv_2mortal(newSVpvf("Unknown attribute %s", key))), "HY000");
}

static void error_nul_character(SV *h, const char *key)
{
  dTHX;
  mariadb_dr_do_error(h, CR_CONNECTION_ERROR, SvPVX(sv_2mortal(newSVpvf("Connection error: %s contains nul character", key))), "HY000");
}

static void error_no_connection(SV *h, const char *msg)
{
  dTHX;
#ifdef HAVE_LAST_ERROR
  /* Some versions of MySQL/MariaDB clients provides last error via NULL parameter, so use it instead of generic error */
  unsigned int last_errno = mysql_errno(NULL);
  const char *last_error = mysql_error(NULL);
  if (last_errno && last_error && last_error[0])
  {
    mariadb_dr_do_error(h, last_errno, last_error, mysql_sqlstate(NULL));
    return;
  }
#endif
  mariadb_dr_do_error(h, CR_CONNECTION_ERROR, msg, "HY000");
}

#ifdef HAVE_GET_CHARSET_NUMBER
/* Available only in some clients and declared in header file my_sys.h which cannot be included */
unsigned int get_charset_number(const char *cs_name, unsigned int cs_flags);
#endif

/***************************************************************************
 *
 *  Name:    mariadb_dr_connect
 *
 *  Purpose: Replacement for mysql_connect
 *
 *  Input:   dbh - database handle being connected
 *           imp_dbh_t *imp_dbh - Pointer to internal dbh structure
 *           char* mysql_socket - Name of a UNIX socket being used
 *             or NULL
 *           char* host - Host name being used or NULL for localhost
 *           unsigned int port - Port number being used or 0 for default
 *           char* user - User name being used or NULL
 *           char* password - Password being used or NULL
 *           char* dbname - Database name being used or NULL
 *
 *  Returns: TRUE for success, FALSE otherwise; mariadb_dr_do_error will
 *           be called in the latter case
 *
 **************************************************************************/

static bool mariadb_dr_connect(
                        SV* dbh,
                        imp_dbh_t *imp_dbh,
                        char* mysql_socket,
                        char* host,
                        unsigned int port,
                        char* user,
                        char* password,
                        char* dbname)
{
  unsigned int client_flag;
  bool client_supports_utf8mb4;
  bool connected;
  unsigned int read_timeout, write_timeout;
  MYSQL *sock;
  dTHX;
  D_imp_xxh(dbh);
  SV *sv = DBIc_IMP_DATA(imp_dbh);
  D_imp_drh_from_dbh;

#ifdef MARIADB_PACKAGE_VERSION
  bool read_timeout_set = FALSE;
  bool write_timeout_set = FALSE;
#endif

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		  "imp_dbh->mariadb_dr_connect: host = |%s|, port = %u,"
		  " uid = %s, pwd = %s\n",
		  host ? host : "NULL", port,
		  user ? user : "NULL",
		  !password ? "NULL" : !password[0] ? "" : "****");

  if (imp_drh->non_embedded_finished)
  {
    mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Method disconnect_all() was already called and library functions unloaded", "HY000");
    return FALSE;
  }

  /* host=localhost means to connect via unix socket, host=embedded means to use embedded server, so do not allow specifying port */
  if (port && host && (strcmp(host, "localhost") == 0 || strcmp(host, "embedded") == 0))
  {
    mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: port cannot be specified when host is localhost or embedded", "HY000");
    return FALSE;
  }

  /* when connecting via unix socket do not allow specifying port or host != localhost */
  if (mysql_socket && (port || (host && strcmp(host, "localhost") != 0)))
  {
    mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: host or port cannot be specified together with mariadb_socket", "HY000");
    return FALSE;
  }

  if (host && strcmp(host, "embedded") == 0)
  {
#ifndef HAVE_EMBEDDED
    mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Embedded server is not supported", "HY000");
    return FALSE;
#endif
      if (sv  &&  SvROK(sv))
      {
        SV** svp;
        SV *embedded_groups = NULL;
        SV *embedded_args = NULL;
        STRLEN options_len;
        char * options;
        int server_args_cnt= 0;
        int server_groups_cnt= 0;
        int rc= 0;

        char ** server_args = NULL;
        char ** server_groups = NULL;

        HV* hv = (HV*) SvRV(sv);

        if (SvTYPE(hv) != SVt_PVHV)
          return FALSE;

        if (imp_drh->instances == 0 && !imp_drh->non_embedded_started)
        {
          if ((svp = hv_fetchs(hv, "mariadb_embedded_groups", FALSE)) && *svp && SvTRUE(*svp))
          {
            embedded_groups = *svp;
            options = SvPVutf8_nomg(embedded_groups, options_len);
            if (strlen(options) != options_len)
            {
              error_nul_character(dbh, "mariadb_embedded_groups");
              return FALSE;
            }

            if ((server_groups_cnt=count_embedded_options(options)))
            {
              /* number of server_groups always server_groups+1 */
              server_groups = fill_out_embedded_options(options, 0, options_len, ++server_groups_cnt);
              if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              {
                PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                              "Groups names passed to embedded server:\n");
                print_embedded_options(DBIc_LOGPIO(imp_xxh), server_groups, server_groups_cnt);
              }
            }
          }

          if ((svp = hv_fetchs(hv, "mariadb_embedded_options", FALSE)) && *svp && SvTRUE(*svp))
          {
            embedded_args = *svp;
            options = SvPVutf8_nomg(embedded_args, options_len);
            if (strlen(options) != options_len)
            {
              if (server_groups_cnt)
                free_embedded_options(server_groups, server_groups_cnt);
              error_nul_character(dbh, "mariadb_embedded_options");
              return FALSE;
            }

            if ((server_args_cnt=count_embedded_options(options)))
            {
              /* number of server_options always server_options+1 */
              server_args = fill_out_embedded_options(options, 1, options_len, ++server_args_cnt);
              if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              {
                PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Server options passed to embedded server:\n");
                print_embedded_options(DBIc_LOGPIO(imp_xxh), server_args, server_args_cnt);
              }
            }
          }

          /* Init embedded server */
          rc = mysql_server_init(server_args_cnt, server_args, server_groups);

          if (server_args_cnt)
            free_embedded_options(server_args, server_args_cnt);
          if (server_groups_cnt)
            free_embedded_options(server_groups, server_groups_cnt);

          if (rc)
          {
            error_no_connection(dbh, "Connection error: Cannot start embedded server");
            return FALSE;
          }

          if (embedded_groups)
            imp_drh->embedded_groups = newSVsv(embedded_groups);
          if (embedded_args)
            imp_drh->embedded_args = newSVsv(embedded_args);

          imp_drh->embedded_started = TRUE;
        }
        else if (imp_drh->non_embedded_started)
        {
          mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Non-embedded connection was already established", "HY000");
          return FALSE;
        }
        else if (imp_drh->embedded_started)
        {
         /*
          * Check if embedded parameters passed to connect() differ from
          * first ones
          */

          if ( ((svp = hv_fetchs(hv, "mariadb_embedded_groups", FALSE)) && *svp && SvTRUE(*svp)))
            rc += abs(sv_cmp_flags(*svp, imp_drh->embedded_groups, 0));

          if ( ((svp = hv_fetchs(hv, "mariadb_embedded_options", FALSE)) && *svp && SvTRUE(*svp)) )
            rc += abs(sv_cmp_flags(*svp, imp_drh->embedded_args, 0));

          if (rc)
          {
            mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Embedded server was already started, cannot pass different init parameters to embedded server", "HY000");
            return FALSE;
          }
        }
        else
        {
          mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Unknown internal state (possible bug in driver)", "HY000");
          return FALSE;
        }
      }
  }
  else
  {
    if (imp_drh->instances == 0 && !imp_drh->non_embedded_started)
    {
      /* negative value means to not start embedded server and just to initialize client library */
      /* initializing client library is needed prior to any other mysql_* call from client library */
      if (mysql_server_init(-1, NULL, NULL))
      {
        error_no_connection(dbh, "Connection error: Cannot initialize client library");
        return FALSE;
      }
      imp_drh->non_embedded_started = TRUE;
    }
  }

  sock = imp_dbh->pmysql = mysql_init(NULL);
  if (!sock)
  {
    error_no_connection(dbh, "Connection error: Cannot initialize client structures");
    mariadb_db_disconnect(dbh, imp_dbh);
    return FALSE;
  }
  imp_drh->instances++;

    client_flag = CLIENT_FOUND_ROWS;

      DBIc_set(imp_dbh, DBIcf_AutoCommit, TRUE);

      if (sv  &&  SvROK(sv))
      {
        HV* hv = (HV*) SvRV(sv);
        HV* processed = newHV();
        HE* he;
        SV** svp;

        sv_2mortal(newRV_noinc((SV *)processed)); /* Automatically free HV processed */

        /* These options are already handled and processed */
        (void)hv_stores(processed, "host", &PL_sv_yes);
        (void)hv_stores(processed, "port", &PL_sv_yes);
        (void)hv_stores(processed, "user", &PL_sv_yes);
        (void)hv_stores(processed, "password", &PL_sv_yes);
        (void)hv_stores(processed, "database", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_socket", &PL_sv_yes);

#ifdef HAVE_EMBEDDED
      if (host && strcmp(host, "embedded") == 0)
      {
        (void)hv_stores(processed, "mariadb_embedded_groups", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_embedded_options", &PL_sv_yes);

        mysql_options(sock, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
        host = NULL;
      }
      else
      {
        mysql_options(sock, MYSQL_OPT_USE_REMOTE_CONNECTION, NULL);
      }
#endif

        /* thanks to Peter John Edwards for mysql_init_command */ 
        (void)hv_stores(processed, "mariadb_init_command", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_init_command", FALSE)) && *svp && SvTRUE(*svp))
        {
          STRLEN len;
          char* df = SvPVutf8_nomg(*svp, len);
          if (strlen(df) != len)
          {
            error_nul_character(dbh, "mariadb_init_command");
            mariadb_db_disconnect(dbh, imp_dbh);
            return FALSE;
          }
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                           "imp_dbh->mariadb_dr_connect: Setting"
                           " init command (%s).\n", df);
          mysql_options(sock, MYSQL_INIT_COMMAND, df);
        }

        (void)hv_stores(processed, "mariadb_compression", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_compression", FALSE)) && *svp && SvTRUE(*svp))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Enabling"
                          " compression.\n");
          mysql_options(sock, MYSQL_OPT_COMPRESS, NULL);
        }

        (void)hv_stores(processed, "mariadb_connect_timeout", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_connect_timeout", FALSE)) && *svp)
        {
          UV uv = SvUV(*svp);
          unsigned int to = (uv <= UINT_MAX ? uv : UINT_MAX);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Setting"
                          " connect timeout (%u).\n", to);
          mysql_options(sock, MYSQL_OPT_CONNECT_TIMEOUT,
                        (const char *)&to);
        }

        (void)hv_stores(processed, "mariadb_write_timeout", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_write_timeout", FALSE)) && *svp)
        {
          UV uv = SvUV(*svp);
          write_timeout = (uv <= UINT_MAX ? uv : UINT_MAX);
#ifdef MARIADB_PACKAGE_VERSION
          write_timeout_set = TRUE;
#endif
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Setting"
                          " write timeout (%u).\n", write_timeout);
          mysql_options(sock, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&write_timeout);
        }

        (void)hv_stores(processed, "mariadb_read_timeout", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_read_timeout", FALSE)) && *svp)
        {
          UV uv = SvUV(*svp);
          read_timeout = (uv <= UINT_MAX ? uv : UINT_MAX);
#ifdef MARIADB_PACKAGE_VERSION
          read_timeout_set = TRUE;
#endif
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Setting"
                          " read timeout (%u).\n", read_timeout);
          mysql_options(sock, MYSQL_OPT_READ_TIMEOUT, (const char *)&read_timeout);
        }

#ifdef MARIADB_PACKAGE_VERSION
        {
          /* Version is in format: "2.3.2" */
          const char version[] = MARIADB_PACKAGE_VERSION;
          bool broken_timeouts;

          if (sizeof(version) != 6 || version[0] != '2' || version[1] != '.' || version[3] != '.' || version[5] != '\0')
            broken_timeouts = FALSE;
          else if (version[2] >= '0' && version[2] < '3' && version[4] >= '0' && version[4] <= '9')
            broken_timeouts = TRUE;
          else if (version[2] == '3' && version[4] >= '0' && version[4] < '3')
            broken_timeouts = TRUE;
          else
            broken_timeouts = FALSE;

          if (broken_timeouts)
          {
            /*
              MariaDB Connector/C prior to version 2.3.3 has broken MYSQL_OPT_WRITE_TIMEOUT.
              Write timeout is set by MYSQL_OPT_READ_TIMEOUT option.
              See file libmariadb/libmariadb.c, line "vio_write_timeout(net->vio, mysql->options.read_timeout);".
              Therefore specifying different read and write timeout is not possible.
             */
            if ((read_timeout_set || write_timeout_set) && (!read_timeout_set || !write_timeout_set || read_timeout != write_timeout))
            {
              mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: Specifying different mariadb_read_timeout and mariadb_write_timeout is broken by current version of MariaDB/MySQL client", "HY000");
              mariadb_db_disconnect(dbh, imp_dbh);
              return FALSE;
            }

            /*
              MariaDB Connector/C prior to version 2.3.3 has broken zero write timeout.
              See file libmariadb/violite.c, function vio_write(). Variable vio->write_timeout
              is signed and on some places zero value is tested as "boolean" and on some as "> 0".
              Therefore we need to force non-zero timeout, otherwise some operation fail.
              Maximal possible value is INT_MAX/1000. So use it for simulation of infinite timeout.
              Also set this timeout when no timeout was provided.
             */
            if ((!read_timeout_set || !write_timeout_set) || write_timeout == 0)
            {
              read_timeout = INT_MAX/1000;
              write_timeout = INT_MAX/1000;
              mysql_options(sock, MYSQL_OPT_READ_TIMEOUT, (const char *)&read_timeout);
              mysql_options(sock, MYSQL_OPT_WRITE_TIMEOUT, (const char *)&write_timeout);
            }
          }
        }
#endif

        (void)hv_stores(processed, "mariadb_max_allowed_packet", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_max_allowed_packet", FALSE)) && *svp && SvTRUE(*svp))
        {
          UV uv = SvUV_nomg(*svp);
          unsigned long packet_size = (uv <= ULONG_MAX ? uv : ULONG_MAX);
#if (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50709 && MYSQL_VERSION_ID != 60000) || (defined(MARIADB_VERSION_ID) && MARIADB_VERSION_ID >= 100202)
          /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in mysql 5.7.9 */
          /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in MariaDB 10.2.2 */
          mysql_options(sock, MYSQL_OPT_MAX_ALLOWED_PACKET, &packet_size);
#else
          /* before MySQL 5.7.9 and MariaDB 10.2.2 use max_allowed_packet macro */
          max_allowed_packet = packet_size;
#endif
        }

        (void)hv_stores(processed, "mariadb_skip_secure_auth", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_skip_secure_auth", FALSE)) && *svp && SvTRUE(*svp))
        {
          int error = 1;
#ifdef HAVE_SECURE_AUTH
          my_bool secauth = FALSE;
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Skipping"
                          " secure auth\n");
          error = mysql_options(sock, MYSQL_SECURE_AUTH, &secauth);
#endif
          if (error)
          {
            mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: mariadb_skip_secure_auth=1 is not supported", "HY000");
            mariadb_db_disconnect(dbh, imp_dbh);
            return FALSE;
          }
        }

        (void)hv_stores(processed, "mariadb_read_default_file", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_read_default_file", FALSE)) && *svp && SvTRUE(*svp))
        {
          STRLEN len;
          char* df = SvPVutf8_nomg(*svp, len);
          if (strlen(df) != len)
          {
            error_nul_character(dbh, "mariadb_read_default_file");
            mariadb_db_disconnect(dbh, imp_dbh);
            return FALSE;
          }
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->mariadb_dr_connect: Reading"
                          " default file %s.\n", df);
          mysql_options(sock, MYSQL_READ_DEFAULT_FILE, df);
        }

        (void)hv_stores(processed, "mariadb_read_default_group", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_read_default_group", FALSE)) && *svp && SvTRUE(*svp))
        {
          STRLEN len;
          char* gr = SvPVutf8_nomg(*svp, len);
          if (strlen(gr) != len)
          {
            error_nul_character(dbh, "mariadb_read_default_group");
            mariadb_db_disconnect(dbh, imp_dbh);
            return FALSE;
          }
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "imp_dbh->mariadb_dr_connect: Using"
                    " default group %s.\n", gr);

          mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, gr);
        }

        (void)hv_stores(processed, "mariadb_conn_attrs", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_conn_attrs", FALSE)) && *svp)
        {
        /* mysql_options4 with MYSQL_OPT_CONNECT_ATTR_ADD is supported by libraries:
         * MariaDB 10.0.5+ and any MariaDB Connector/C; MySQL 5.6.6+ except MySQL Connector/C 6.0.0
         */
        #if (defined(MARIADB_BASE_VERSION) && (MYSQL_VERSION_ID >= 100005 || defined(MARIADB_PACKAGE_VERSION))) || (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50606 && MYSQL_VERSION_ID != 60000)
              HV* attrs = (HV*) SvRV(*svp);
              HE* entry = NULL;
              I32 num_entries = hv_iterinit(attrs);
              while (num_entries && (entry = hv_iternext(attrs))) {
                  I32 attr_name_len = 0;
                  char *attr_name = hv_iterkey(entry, &attr_name_len);
                  SV *sv_attr_val = hv_iterval(attrs, entry);
                  STRLEN attr_val_len;
                  char *attr_val  = SvPVutf8(sv_attr_val, attr_val_len);
                  if (strlen(attr_name) != (STRLEN)attr_name_len || strlen(attr_val) != attr_val_len)
                  {
                    error_nul_character(dbh, "mariadb_conn_attrs");
                    mariadb_db_disconnect(dbh, imp_dbh);
                    return FALSE;
                  }
                  mysql_options4(sock, MYSQL_OPT_CONNECT_ATTR_ADD, attr_name, attr_val);
              }
        #else
              mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: mariadb_conn_attrs is not supported", "HY000");
              mariadb_db_disconnect(dbh, imp_dbh);
              return FALSE;
        #endif
        }

        (void)hv_stores(processed, "mariadb_client_found_rows", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_client_found_rows", FALSE)) && *svp)
        {
          if (SvTRUE(*svp))
            client_flag |= CLIENT_FOUND_ROWS;
          else
            client_flag &= ~CLIENT_FOUND_ROWS;
        }

        (void)hv_stores(processed, "mariadb_auto_reconnect", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_auto_reconnect", FALSE)) && *svp)
        {
          imp_dbh->auto_reconnect = SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->auto_reconnect: %d\n",
                          imp_dbh->auto_reconnect);
        }

        (void)hv_stores(processed, "mariadb_use_result", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_use_result", FALSE)) && *svp)
        {
          imp_dbh->use_mysql_use_result = SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->use_mysql_use_result: %d\n",
                          imp_dbh->use_mysql_use_result ? 1 : 0);
        }

        (void)hv_stores(processed, "mariadb_bind_type_guessing", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_bind_type_guessing", FALSE)) && *svp)
        {
          imp_dbh->bind_type_guessing= SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->bind_type_guessing: %d\n",
                          imp_dbh->bind_type_guessing);
        }

        (void)hv_stores(processed, "mariadb_bind_comment_placeholders", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_bind_comment_placeholders", FALSE)) && *svp)
        {
          imp_dbh->bind_comment_placeholders = SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->bind_comment_placeholders: %d\n",
                          imp_dbh->bind_comment_placeholders);
        }

        (void)hv_stores(processed, "mariadb_no_autocommit_cmd", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_no_autocommit_cmd", FALSE)) && *svp)
        {
          imp_dbh->no_autocommit_cmd= SvTRUE(*svp);
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->no_autocommit_cmd: %d\n",
                          imp_dbh->no_autocommit_cmd);
        }

        (void)hv_stores(processed, "mariadb_use_fabric", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_use_fabric", FALSE)) && *svp && SvTRUE(*svp))
        {
#ifdef HAVE_FABRIC
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "imp_dbh->use_fabric: Enabling use of"
                          " MySQL Fabric.\n");
          mysql_options(sock, MYSQL_OPT_USE_FABRIC, NULL);
#else
          mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: mariadb_use_fabric is not supported", "HY000");
          mariadb_db_disconnect(dbh, imp_dbh);
          return FALSE;
#endif
        }

        (void)hv_stores(processed, "mariadb_multi_statements", &PL_sv_yes);
	if ((svp = hv_fetchs(hv, "mariadb_multi_statements", FALSE)) && *svp)
        {
	  if (SvTRUE(*svp))
	    client_flag |= CLIENT_MULTI_STATEMENTS;
          else
            client_flag &= ~CLIENT_MULTI_STATEMENTS;
	}

        (void)hv_stores(processed, "mariadb_server_prepare", &PL_sv_yes);
	/* took out  client_flag |= CLIENT_PROTOCOL_41; */
	/* because libmysql.c already sets this no matter what */
	if ((svp = hv_fetchs(hv, "mariadb_server_prepare", FALSE)) && *svp)
        {
	  if (SvTRUE(*svp))
          {
	    client_flag |= CLIENT_PROTOCOL_41;
            imp_dbh->use_server_side_prepare = TRUE;
	  }
          else
          {
	    client_flag &= ~CLIENT_PROTOCOL_41;
            imp_dbh->use_server_side_prepare = FALSE;
	  }
	}
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "imp_dbh->use_server_side_prepare: %d\n",
                        imp_dbh->use_server_side_prepare ? 1 : 0);

        if (imp_dbh->bind_type_guessing && imp_dbh->use_server_side_prepare)
        {
          mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: mariadb_bind_type_guessing and mariadb_server_prepare cannot be enabled together", "HY000");
          mariadb_db_disconnect(dbh, imp_dbh);
          return FALSE;
        }

        (void)hv_stores(processed, "mariadb_server_prepare_disable_fallback", &PL_sv_yes);
        if ((svp = hv_fetchs(hv, "mariadb_server_prepare_disable_fallback", FALSE)) && *svp)
          imp_dbh->disable_fallback_for_server_prepare = SvTRUE(*svp);
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "imp_dbh->disable_fallback_for_server_prepare: %d\n",
                        imp_dbh->disable_fallback_for_server_prepare ? 1 : 0);

        (void)hv_stores(processed, "mariadb_ssl", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_optional", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_verify_server_cert", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_client_key", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_client_cert", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_ca_file", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_ca_path", &PL_sv_yes);
        (void)hv_stores(processed, "mariadb_ssl_cipher", &PL_sv_yes);
	if ((svp = hv_fetchs(hv, "mariadb_ssl", FALSE)) && *svp && SvTRUE(*svp))
          {
	    char *client_key = NULL;
	    char *client_cert = NULL;
	    char *ca_file = NULL;
	    char *ca_path = NULL;
	    char *cipher = NULL;
	    STRLEN len = 0;
  #if defined(HAVE_SSL_MODE) || defined(HAVE_SSL_MODE_ONLY_REQUIRED)
	    unsigned int ssl_mode;
  #endif
	    my_bool ssl_enforce = TRUE;
	    my_bool ssl_verify = FALSE;
  #ifndef HAVE_SSL_MODE
	    my_bool ssl_verify_set = FALSE;
  #endif

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_optional", FALSE)) && *svp)
	      ssl_enforce = !SvTRUE(*svp);

            /* Verify if the hostname we connect to matches the hostname in the certificate */
	    if ((svp = hv_fetchs(hv, "mariadb_ssl_verify_server_cert", FALSE)) && *svp)
	    {
  #if defined(HAVE_SSL_VERIFY) || defined(HAVE_SSL_MODE)
	      ssl_verify = SvTRUE(*svp);
    #ifndef HAVE_SSL_MODE
	      ssl_verify_set = TRUE;
    #endif
  #else
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_verify_server_cert=1 is not supported", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
  #endif
	    }

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_client_key", FALSE)) && *svp)
	    {
	      client_key = SvPVutf8(*svp, len);
	      if (strlen(client_key) != len)
	      {
	        error_nul_character(dbh, "mariadb_ssl_client_key");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	    }

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_client_cert", FALSE)) && *svp)
	    {
	      client_cert = SvPVutf8(*svp, len);
	      if (strlen(client_cert) != len)
	      {
	        error_nul_character(dbh, "mariadb_ssl_client_cert");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	    }

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_ca_file", FALSE)) && *svp)
	    {
	      ca_file = SvPVutf8(*svp, len);
	      if (strlen(ca_file) != len)
	      {
	        error_nul_character(dbh, "mariadb_ssl_ca_file");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	    }

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_ca_path", FALSE)) && *svp)
	    {
	      ca_path = SvPVutf8(*svp, len);
	      if (strlen(ca_path) != len)
	      {
	        error_nul_character(dbh, "mariadb_ssl_ca_path");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	    }

	    if ((svp = hv_fetchs(hv, "mariadb_ssl_cipher", FALSE)) && *svp)
	    {
	      cipher = SvPVutf8(*svp, len);
	      if (strlen(cipher) != len)
	      {
	        error_nul_character(dbh, "mariadb_ssl_cipher");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	    }

	    mysql_ssl_set(sock, client_key, client_cert, ca_file,
			  ca_path, cipher);

	    if (ssl_verify && !(ca_file || ca_path)) {
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_verify_server_cert=1 is not supported without mariadb_ssl_ca_file or mariadb_ssl_ca_path", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
	    }

  #ifdef HAVE_SSL_MODE

	    if (!ssl_enforce)
	      ssl_mode = SSL_MODE_PREFERRED;
	    else if (ssl_verify)
	      ssl_mode = SSL_MODE_VERIFY_IDENTITY;
	    else if (ca_file || ca_path)
	      ssl_mode = SSL_MODE_VERIFY_CA;
	    else
	      ssl_mode = SSL_MODE_REQUIRED;
	    if (mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
	    }

  #else

	    if (ssl_enforce) {
    #if defined(HAVE_SSL_MODE_ONLY_REQUIRED)
	      ssl_mode = SSL_MODE_REQUIRED;
	      if (mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode) != 0) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
    #elif defined(HAVE_SSL_ENFORCE)
	      if (mysql_options(sock, MYSQL_OPT_SSL_ENFORCE, &ssl_enforce) != 0) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
    #elif defined(HAVE_SSL_VERIFY)
	      if (!ssl_verify_also_enforce_ssl()) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	      if (ssl_verify_set && !ssl_verify) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported without mariadb_ssl_verify_server_cert=1", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
	      ssl_verify = TRUE;
    #else
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: Enforcing SSL encryption is not supported", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
    #endif
	    }

    #ifdef HAVE_SSL_VERIFY
	    if (!ssl_enforce && ssl_verify && ssl_verify_also_enforce_ssl()) {
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_optional=1 with mariadb_ssl_verify_server_cert=1 is not supported", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
	    }
    #endif

	    if (ssl_verify) {
	      if (!ssl_verify_usable() && ssl_enforce && ssl_verify_set) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_verify_server_cert=1 is broken by current version of MariaDB/MySQL client", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
    #ifdef HAVE_SSL_VERIFY
	      if (mysql_options(sock, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &ssl_verify) != 0) {
	        mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_verify_server_cert=1 is not supported", "HY000");
	        mariadb_db_disconnect(dbh, imp_dbh);
	        return FALSE;
	      }
    #else
	      mariadb_dr_do_error(dbh, CR_SSL_CONNECTION_ERROR, "SSL connection error: mariadb_ssl_verify_server_cert=1 is not supported", "HY000");
	      mariadb_db_disconnect(dbh, imp_dbh);
	      return FALSE;
    #endif
	    }

  #endif

	    client_flag |= CLIENT_SSL;
	  }
	else
	  {
#ifdef HAVE_SSL_MODE
	    unsigned int ssl_mode = SSL_MODE_DISABLED;
	    mysql_options(sock, MYSQL_OPT_SSL_MODE, &ssl_mode);
#endif
	  }
        (void)hv_stores(processed, "mariadb_local_infile", &PL_sv_yes);
     if ((svp = hv_fetchs(hv, "mariadb_local_infile", FALSE)) && *svp)
     {
	  unsigned int flag = SvTRUE(*svp);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
	    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
        "imp_dbh->mariadb_dr_connect: Using"
        " local infile %u.\n", flag);
	  mysql_options(sock, MYSQL_OPT_LOCAL_INFILE, (const char *) &flag);
	}

        hv_iterinit(hv);
        while ((he = hv_iternext(hv)) != NULL)
        {
          I32 len;
          const char *key;
          key = hv_iterkey(he, &len);
          if (skip_attribute(key) || hv_exists(processed, key, len))
            continue;
          error_unknown_attribute(dbh, key);
          mariadb_db_disconnect(dbh, imp_dbh);
          return FALSE;
        }
      }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->mariadb_dr_connect: client_flags = %d\n",
		    client_flag);

    client_flag|= CLIENT_MULTI_RESULTS;

    /*
      MySQL's "utf8mb4" charset is capable of handling 4-byte UTF-8 characters.
      MySQL's "utf8" charset is capable of handling only up to 3-byte UTF-8 characters.
      MySQL's "utf8mb4" charset was introduced in MySQL server version 5.5.3.
      If MySQL's "utf8mb4" is not supported by server, fallback to MySQL's "utf8".
      If MySQL's "utf8mb4" is not supported by client, connect with "utf8" and issue SET NAMES 'utf8mb4'.
      MYSQL_SET_CHARSET_NAME option (prior to establishing connection) sets client's charset.
      Some clients think that they were connected with MYSQL_SET_CHARSET_NAME, but reality can be different. So issue SET NAMES.
      To enable UTF-8 storage on server it is needed to configure it via session variable character_set_server.
      Some clients provides function get_charset_number() to check if charset is supported.
      If MySQL client does not support specified charset it used to print error message to stdout or stderr.
      DBD::MariaDB expects that whole communication with server is encoded in UTF-8.
      By default broken utf8mb4_general_ci collation is used. So change it to utf8mb4_unicode_ci which is according to the Unicode 4.0.0.
    */
#ifdef HAVE_GET_CHARSET_NUMBER
    client_supports_utf8mb4 = get_charset_number("utf8mb4", MY_CS_PRIMARY) ? TRUE : FALSE;
#elif MYSQL_VERSION_ID < 50503
    client_supports_utf8mb4 = FALSE;
#else
    client_supports_utf8mb4 = TRUE;
#endif
    connected = FALSE;
    if (client_supports_utf8mb4)
    {
      mysql_options(sock, MYSQL_SET_CHARSET_NAME, "utf8mb4");
      connected = mysql_real_connect(sock, host, user, password, dbname, port, mysql_socket, client_flag | CLIENT_REMEMBER_OPTIONS) ? TRUE : FALSE;
      if (!connected && mysql_errno(sock) != CR_CANT_READ_CHARSET)
      {
        mariadb_dr_do_error(dbh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
      if (connected && mysql_get_server_version(sock) < 40100)
      {
        mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: MariaDB or MySQL server version is older then 4.1.0", "HY000");
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
    }
    if (!connected)
    {
      mysql_options(sock, MYSQL_SET_CHARSET_NAME, "utf8");
      connected = mysql_real_connect(sock, host, user, password, dbname, port, mysql_socket, client_flag) ? TRUE : FALSE;
      if (!connected)
      {
        mariadb_dr_do_error(dbh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
      if (mysql_get_server_version(sock) < 40100)
      {
        mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: MariaDB or MySQL server version is older then 4.1.0", "HY000");
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
    }
    if (mysql_query(sock, "SET NAMES 'utf8mb4'") != 0 ||
        mysql_query(sock, "SET character_set_server = 'utf8mb4'") != 0)
    {
      if (mysql_errno(sock) != ER_UNKNOWN_CHARACTER_SET)
      {
        mariadb_dr_do_error(dbh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
      if (mysql_query(sock, "SET NAMES 'utf8'") != 0 ||
          mysql_query(sock, "SET character_set_server = 'utf8'") != 0 ||
          mysql_query(sock, "SET collation_connection = 'utf8_unicode_ci'") != 0 ||
          mysql_query(sock, "SET collation_server = 'utf8_unicode_ci'") != 0)
      {
        mariadb_dr_do_error(dbh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
    }
    else
    {
      if (mysql_query(sock, "SET collation_connection = 'utf8mb4_unicode_ci'") != 0 ||
          mysql_query(sock, "SET collation_server = 'utf8mb4_unicode_ci'") != 0)
      {
        mariadb_dr_do_error(dbh, mysql_errno(sock), mysql_error(sock), mysql_sqlstate(sock));
        mariadb_db_disconnect(dbh, imp_dbh);
        return FALSE;
      }
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->mariadb_dr_connect: <-");

      /*
        we turn off Mysql's auto reconnect and handle re-connecting ourselves
        so that we can keep track of when this happens.
      */
#if MYSQL_VERSION_ID >= 50013
    {
      my_bool reconnect = FALSE;
      mysql_options(sock, MYSQL_OPT_RECONNECT, &reconnect);
    }
#else
      sock->reconnect = FALSE;
#endif

      /* connection succeeded. */
      if (!(sock->client_flag & CLIENT_PROTOCOL_41))
        imp_dbh->use_server_side_prepare = FALSE;

          imp_dbh->async_query_in_flight = NULL;

    return TRUE;
}

/*
  safe_hv_fetch
*/
static char *safe_hv_fetch(pTHX_ SV *dbh, HV *hv, const char *name, int name_length)
{
  SV **svp;
  char *str;
  STRLEN len;

  svp = hv_fetch(hv, name, name_length, FALSE);
  if (!svp || !*svp)
    return NULL;

  SvGETMAGIC(*svp);
  if (!SvOK(*svp))
    return NULL;

  str = SvPVutf8_nomg(*svp, len);
  if (strlen(str) != len)
  {
    error_nul_character(dbh, name);
    return (void *)-1;
  }

  return str;
}
#define safe_hv_fetchs(dbh, hv, name) safe_hv_fetch(aTHX_ (dbh), (hv), "" name "", sizeof((name))-1)

/*
 Frontend for mariadb_dr_connect
*/
static bool mariadb_db_my_login(pTHX_ SV* dbh, imp_dbh_t *imp_dbh)
{
  SV* sv;
  HV* hv;
  SV **svp;
  UV port;
  char* dbname;
  char* host;
  char* user;
  char* password;
  char* mysql_socket;
  I32 i;
  bool found_taken_pmysql;
  D_imp_xxh(dbh);
  D_imp_drh_from_dbh;

  if (DBIc_has(imp_dbh, DBIcf_IMPSET))
  { /* eg from take_imp_data() */
    if (DBIc_has(imp_dbh, DBIcf_ACTIVE))
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh), "mariadb_db_my_login skip connect\n");
      /* tell our parent we've adopted an active child */
      ++DBIc_ACTIVE_KIDS(DBIc_PARENT_COM(imp_dbh));
      found_taken_pmysql = FALSE;
      if (imp_drh->taken_pmysqls)
      {
        for (i = AvFILL(imp_drh->taken_pmysqls); i >= 0; --i)
        {
          svp = av_fetch(imp_drh->taken_pmysqls, i, FALSE);
          if (!svp || !*svp)
            continue;
          SvGETMAGIC(*svp);
          if (!SvIOK(*svp))
            continue;
          if (imp_dbh->pmysql == INT2PTR(MYSQL *, SvIVX(*svp)))
          {
            found_taken_pmysql = TRUE;
            av_delete(imp_drh->taken_pmysqls, i, G_DISCARD);
            break;
          }
        }
      }
      if (!found_taken_pmysql)
      {
        imp_dbh->pmysql = NULL;
        mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: dbi_imp_data is not valid", "HY000");
        return FALSE;
      }
      return TRUE;
    }
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "mariadb_db_my_login IMPSET but not ACTIVE so connect not skipped\n");
  }

  sv = DBIc_IMP_DATA(imp_dbh);

  if (!sv  ||  !SvROK(sv))
    return FALSE;

  hv = (HV*) SvRV(sv);
  if (SvTYPE(hv) != SVt_PVHV)
    return FALSE;

  host = safe_hv_fetchs(dbh, hv, "host");
  if (host == (void *)-1)
    return FALSE;

  user = safe_hv_fetchs(dbh, hv, "user");
  if (user == (void *)-1)
    return FALSE;

  password = safe_hv_fetchs(dbh, hv, "password");
  if (password == (void *)-1)
    return FALSE;

  dbname = safe_hv_fetchs(dbh, hv, "database");
  if (dbname == (void *)-1)
    return FALSE;

  mysql_socket = safe_hv_fetchs(dbh, hv, "mariadb_socket");
  if (mysql_socket == (void *)-1)
    return FALSE;

  port = 0;
  svp = hv_fetchs(hv, "port", FALSE);
  if (svp && *svp)
  {
    SvGETMAGIC(*svp);
    if (SvOK(*svp))
    {
      port = SvUV_nomg(*svp);
      if (port == 0 || port > UINT_MAX)
      {
        mariadb_dr_do_error(dbh, CR_CONNECTION_ERROR, "Connection error: port is not valid number", "HY000");
        return FALSE;
      }
    }
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		  "imp_dbh->mariadb_db_my_login : dbname = %s, uid = %s, pwd = %s,"
		  "host = %s, port = %u\n",
		  dbname ? dbname : "NULL",
		  user ? user : "NULL",
		  !password ? "NULL" : !password[0] ? "" : "****",
		  host ? host : "NULL",
		  (unsigned int)port);

  return mariadb_dr_connect(dbh, imp_dbh, mysql_socket, host, port, user, password, dbname);
}


SV *mariadb_db_take_imp_data(SV *dbh, imp_xxh_t *imp_xxh, void *foo)
{
  dTHX;
  D_imp_dbh(dbh);
  D_imp_drh_from_dbh;
  PERL_UNUSED_ARG(imp_xxh);
  PERL_UNUSED_ARG(foo);

  if (!imp_drh->taken_pmysqls)
    imp_drh->taken_pmysqls = newAV();
  av_push(imp_drh->taken_pmysqls, newSViv(PTR2IV(imp_dbh->pmysql)));

  return &PL_sv_no;
}


/**************************************************************************
 *
 *  Name:    mariadb_db_login6_sv
 *
 *  Purpose: Called for connecting to a database and logging in.
 *
 *  Input:   dbh - database handle being initialized
 *           imp_dbh - drivers private database handle data
 *           dsn - the database we want to log into; may be like
 *               "dbname:host" or "dbname:host:port"
 *           user - user name to connect as
 *           password - password to connect with
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int mariadb_db_login6_sv(SV *dbh, imp_dbh_t *imp_dbh, SV *dsn, SV *user, SV *password, SV *attribs)
{
  dTHX; 
  D_imp_xxh(dbh);
  PERL_UNUSED_ARG(attribs);

  SvGETMAGIC(dsn);
  SvGETMAGIC(user);
  SvGETMAGIC(password);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		  "imp_dbh->connect: dsn = %s, uid = %s, pwd = %s\n",
                  SvOK(dsn) ? neatsvpv(dsn, 0) : "NULL",
                  SvOK(user) ? neatsvpv(user, 0) : "NULL",
                  !SvOK(password) ? "NULL" : !(SvPV_nomg_nolen(password))[0] ? "''" : "****");

  imp_dbh->stats.auto_reconnects_ok= 0;
  imp_dbh->stats.auto_reconnects_failed= 0;
  imp_dbh->bind_type_guessing= FALSE;
  imp_dbh->bind_comment_placeholders= FALSE;
  imp_dbh->auto_reconnect = FALSE;
  imp_dbh->connected = FALSE;       /* Will be switched to TRUE after DBI->connect finish */

  if (!mariadb_db_my_login(aTHX_ dbh, imp_dbh))
    return 0;

    /*
     *  Tell DBI, that dbh->disconnect should be called for this handle
     */
    DBIc_ACTIVE_on(imp_dbh);

    /* Tell DBI, that dbh->destroy should be called for this handle */
    DBIc_on(imp_dbh, DBIcf_IMPSET);

    return 1;
}


/**************************************************************************
 *
 *  Name:    mariadb_db_do6
 *
 *  Purpose: Function for preparing and executing simple statement.
 *
 *  Input:   dbh - database handle being initialized
 *           imp_dbh - drivers private database handle data
 *           statement - query being executed
 *           attribs - statement attributes
 *           items - number of arguments passed to XSUB
 *           ax - stack base offset used by ST() macro
 *
 *  Returns: -2 for errors, -1 for unknown number of rows, otherwise number
 *           of rows; mariadb_dr_do_error will be called for errors
 *
 **************************************************************************/

IV mariadb_db_do6(SV *dbh, imp_dbh_t *imp_dbh, SV *statement_sv, SV *attribs, I32 items, I32 ax)
{
  dTHX;
  I32 i;
  my_ulonglong retval;
  char *statement;
  STRLEN statement_len;
  struct imp_sth_ph_st *params = NULL;
  MYSQL_RES *result = NULL;
  bool async = FALSE;
  int next_result_rc;
  bool failed = FALSE;
  bool has_been_bound = FALSE;
  bool use_server_side_prepare = FALSE;
  bool disable_fallback_for_server_prepare = FALSE;
  MYSQL_STMT *stmt = NULL;
  MYSQL_BIND *bind = NULL;
  MYSQL_RES *res;
  STRLEN blen;

  ASYNC_CHECK_RETURN(dbh, -2);

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(dbh, NULL))
  {
    mariadb_dr_do_error(dbh, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return -2;
  }

  SvGETMAGIC(statement_sv);

  for (i = 0; i < items; i++)
  {
    SV *param = ST(i);
    SvGETMAGIC(param);
  }

  (void)hv_stores((HV *)SvRV(dbh), "Statement", SvREFCNT_inc(statement_sv));
  statement = SvPVutf8_nomg(statement_sv, statement_len);

  /*
   * Globally enabled using of server side prepared statement
   * for dbh->do() statements. It is possible to force driver
   * to use server side prepared statement mechanism by adding
   * 'mariadb_server_prepare' attribute to do() method localy:
   * $dbh->do($stmt, {mariadb_server_prepare=>1});
  */
  use_server_side_prepare = imp_dbh->use_server_side_prepare;

  DBD_ATTRIBS_CHECK("do", dbh, attribs);
  if (attribs)
  {
    HV *hv;
    HE *he;
    SV **svp;
    HV *processed;

    processed = newHV();
    sv_2mortal(newRV_noinc((SV *)processed)); /* Automatically free HV processed */

    (void)hv_stores(processed, "mariadb_server_prepare", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_server_prepare");
    use_server_side_prepare = svp ? SvTRUE(*svp) : imp_dbh->use_server_side_prepare;

    (void)hv_stores(processed, "mariadb_server_prepare_disable_fallback", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_server_prepare_disable_fallback");
    disable_fallback_for_server_prepare = svp ? SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;

    (void)hv_stores(processed, "mariadb_async", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_async");
    async = svp ? SvTRUE(*svp) : FALSE;

    hv = (HV *)SvRV(attribs);
    hv_iterinit(hv);
    while ((he = hv_iternext(hv)) != NULL)
    {
      I32 len;
      const char *key;

      key = hv_iterkey(he, &len);
      if (hv_exists(processed, key, len))
        continue;

      mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, SvPVX(sv_2mortal(newSVpvf("Unknown attribute %s", key))), "HY000");
      return -2;
    }
  }

  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh), "mysql.xs do() use_server_side_prepare %d\n", use_server_side_prepare ? 1 : 0);

  if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_dbh), "mysql.xs do() async %d\n", (async ? 1 : 0));

  if (async)
  {
    if (disable_fallback_for_server_prepare)
    {
      mariadb_dr_do_error(dbh, ER_UNSUPPORTED_PS, "Async option not supported with server side prepare", "HY000");
      return -2;
    }
    use_server_side_prepare = FALSE; /* for now */
    imp_dbh->async_query_in_flight = imp_dbh;
  }

  while (mysql_next_result(imp_dbh->pmysql) == 0)
  {
    res = mysql_use_result(imp_dbh->pmysql);
    if (res)
      mysql_free_result(res);
  }

  if (use_server_side_prepare)
  {
    stmt = mysql_stmt_init(imp_dbh->pmysql);

    if (stmt && mysql_stmt_prepare(stmt, statement, statement_len))
    {
      if (mariadb_db_reconnect(dbh, stmt))
      {
        mysql_stmt_close(stmt);
        stmt = mysql_stmt_init(imp_dbh->pmysql);
        if (stmt && mysql_stmt_prepare(stmt, statement, statement_len))
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
      /* For commands that are not supported by server side prepared statement
         mechanism lets try to pass them through regular API */
      if (!disable_fallback_for_server_prepare &&
          (mysql_stmt_errno(stmt) == ER_UNSUPPORTED_PS ||
          /* And also fallback when placeholder is used in unsupported
           * construction with old server versions (e.g. LIMIT ?) */
          (mysql_stmt_errno(stmt) == ER_PARSE_ERROR &&
           mysql_get_server_version(imp_dbh->pmysql) < 50007 &&
           strstr(mysql_stmt_error(stmt), "'?"))))
      {
        use_server_side_prepare = FALSE;
      }
      else
      {
        mariadb_dr_do_error(dbh, mysql_stmt_errno(stmt), mysql_stmt_error(stmt), mysql_stmt_sqlstate(stmt));
        retval = (my_ulonglong)-1;
      }
      mysql_stmt_close(stmt);
      stmt = NULL;
    }
    else
    {
      if (items > 0)
      {
        Newz(0, bind, items, MYSQL_BIND);

        for (i = 0; i < items; i++)
        {
          SV *param = ST(i);
          if (SvOK(param))
          {
            bind[i].buffer = SvPVutf8_nomg(param, blen);
            bind[i].buffer_length = blen;
            bind[i].buffer_type = MYSQL_TYPE_STRING;
          }
          else
          {
            bind[i].buffer = NULL;
            bind[i].buffer_length = 0;
            bind[i].buffer_type = MYSQL_TYPE_NULL;
          }
        }
      }

      retval = mariadb_st_internal_execute41(dbh, statement, statement_len, !!(items > 0), &result, &stmt, bind, &imp_dbh->pmysql, &has_been_bound);

      if (bind)
        Safefree(bind);

      mysql_stmt_close(stmt);
      stmt = NULL;

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

  if (!use_server_side_prepare)
  {
    if (items > 0)
    {
      Newz(0, params, items, struct imp_sth_ph_st);

      for (i = 0; i < items; i++)
      {
        SV *param = ST(i);
        if (SvOK(param))
          params[i].value = SvPVutf8_nomg(param, params[i].len);
        else
          params[i].value = NULL;
        params[i].type = SQL_VARCHAR;
      }
    }

    retval = mariadb_st_internal_execute(dbh, statement, statement_len, items, params, &result, &imp_dbh->pmysql, FALSE);
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
    while ((next_result_rc = mysql_next_result(imp_dbh->pmysql)) == 0)
    {
      result = mysql_use_result(imp_dbh->pmysql);
      if (result)
        mysql_free_result(result);
      result = NULL;
    }

    if (next_result_rc > 0)
    {
      if (DBIc_DBISTATE(imp_dbh)->debug >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_dbh), "\t<- do() ERROR: %s\n", mysql_error(imp_dbh->pmysql));

      mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
      retval = (my_ulonglong)-1;
    }
  }

  if (retval == (my_ulonglong)-1)
    return -2;
  else if (retval <= IV_MAX)
    return retval;
  else
    return -1;
}


/***************************************************************************
 *
 *  Name:    mariadb_db_commit
 *           mariadb_db_rollback
 *
 *  Purpose: You guess what they should do. 
 *
 *  Input:   dbh - database handle being committed or rolled back
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error has already
 *           been called in the latter case
 *
 **************************************************************************/

int
mariadb_db_commit(SV* dbh, imp_dbh_t* imp_dbh)
{
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    return 0;

  ASYNC_CHECK_RETURN(dbh, 0);

  if (!imp_dbh->pmysql)
  {
    mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "No connection to server", "HY000");
    return 0;
  }

    if (mysql_commit(imp_dbh->pmysql))
    {
      mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql)
               ,mysql_sqlstate(imp_dbh->pmysql));
      return 0;
    }

  return 1;
}

/*
 mariadb_db_rollback
*/
int
mariadb_db_rollback(SV* dbh, imp_dbh_t* imp_dbh) {
  /* report error, if not in AutoCommit mode */
  if (DBIc_has(imp_dbh, DBIcf_AutoCommit))
    return 0;

  ASYNC_CHECK_RETURN(dbh, 0);

  /* No connection to server, nothing to rollback */
  if (!imp_dbh->pmysql)
    return 1;

      if (mysql_rollback(imp_dbh->pmysql))
      {
        mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql),
                 mysql_error(imp_dbh->pmysql) ,mysql_sqlstate(imp_dbh->pmysql));
        return 0;
      }

  return 1;
}

static void mariadb_dr_close_mysql(pTHX_ imp_drh_t *imp_drh, MYSQL *pmysql)
{
  if (pmysql)
  {
    mysql_close(pmysql);
    imp_drh->instances--;
  }
  if (imp_drh->instances == 0)
  {
    /*
     * Some MariaDB and MySQL clients have a bug which cause:
     * - inability to successfully initialize a new network connection, even after mysql_server_init()
     * - infinite loop when calling mysql_server_end() more then once in case Embedded server was not started
     * Therefore do not call mysql_server_end() when Embedded server was not in used.
     */
    if (imp_drh->embedded_started)
    {
      mysql_server_end();
      imp_drh->embedded_started = FALSE;
    }
    if (imp_drh->embedded_args)
    {
      (void)SvREFCNT_dec(imp_drh->embedded_args);
      imp_drh->embedded_args = NULL;
    }
    if (imp_drh->embedded_groups)
    {
      (void)SvREFCNT_dec(imp_drh->embedded_groups);
      imp_drh->embedded_groups = NULL;
    }
  }
}

/*
 ***************************************************************************
 *
 *  Name:    mariadb_db_disconnect
 *
 *  Purpose: Disconnect a database handle from its database
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: 1 for success (always)
 *
 **************************************************************************/

int mariadb_db_disconnect(SV* dbh, imp_dbh_t* imp_dbh)
{
  dTHX;
  AV *av;
  I32 i;
  MAGIC *mg;
  SV **svp;
  SV *sv;
  SV *sth;
  imp_sth_t *imp_sth;
  D_imp_xxh(dbh);
  D_imp_drh_from_dbh;

  /* We assume that disconnect will always work       */
  /* since most errors imply already disconnected.    */
  DBIc_ACTIVE_off(imp_dbh);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "imp_dbh->pmysql: %p\n",
		              imp_dbh->pmysql);

  if (imp_dbh->pmysql)
  {
    mariadb_dr_close_mysql(aTHX_ imp_drh, imp_dbh->pmysql);
    imp_dbh->pmysql = NULL;
    svp = hv_fetchs((HV*)DBIc_MY_H(imp_dbh), "ChildHandles", FALSE);
    if (svp && *svp)
    {
      SvGETMAGIC(*svp);
      if (SvROK(*svp) && SvTYPE(SvRV(*svp)) == SVt_PVAV)
      {
        av = (AV *)SvRV(*svp);
        for (i = AvFILL(av); i >= 0; --i)
        {
          svp = av_fetch(av, i, FALSE);
          if (!svp || !*svp || !sv_isobject(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVHV)
            continue;
          sv = SvRV(*svp);
          /* get inner DBI handle (sth) from outer DBI handle (sv) */
          if (!SvMAGICAL(sv))
            continue;
          mg = mg_find(sv, 'P');
          if (!mg)
            continue;
          sth = mg->mg_obj;
          imp_sth = (imp_sth_t *)DBIh_COM(sth);
          if (DBIc_TYPE(imp_sth) != DBIt_ST)
            continue;
          /* mysql_close() should properly invalidates MYSQL* pointers in
           * MYSQL_STMT structures. But MariaDB and MySQL clients affected by
           * CVE 2017-3302 do not do it. So do it manually to prevent crash. */
          if (imp_sth->stmt && imp_sth->stmt->mysql)
          {
            if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Applying CVE 2017-3302 workaround for sth=0x%p\n", imp_sth);
            imp_sth->stmt->mysql = NULL;
          }
        }
      }
    }
  }

  /* We don't free imp_dbh since a reference still exists    */
  /* The DESTROY method is the only one to 'free' memory.    */
  return 1;
}


/***************************************************************************
 *
 *  Name:    mariadb_dr_discon_all
 *
 *  Purpose: Disconnect all database handles at shutdown time
 *
 *  Input:   dbh - database handle being disconnected
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: 1 for success, 0 otherwise
 *
 **************************************************************************/

int mariadb_dr_discon_all (SV *drh, imp_drh_t *imp_drh) {
  dTHX;
  dSP;
  int ret;
  SV **svp;
  AV *av;
  I32 i;
  PERL_UNUSED_ARG(drh);

  if (imp_drh->taken_pmysqls)
  {
    for (i = AvFILL(imp_drh->taken_pmysqls); i >= 0; --i)
    {
      svp = av_fetch(imp_drh->taken_pmysqls, i, FALSE);
      if (!svp || !*svp)
        continue;
      SvGETMAGIC(*svp);
      if (!SvIOK(*svp))
        continue;
      mariadb_dr_close_mysql(aTHX_ imp_drh, INT2PTR(MYSQL *, SvIVX(*svp)));
    }
    av_undef(imp_drh->taken_pmysqls);
    imp_drh->taken_pmysqls = NULL;
  }

  svp = hv_fetchs((HV*)DBIc_MY_H(imp_drh), "ChildHandles", FALSE);
  if (svp && *svp)
  {
    SvGETMAGIC(*svp);
    if (SvROK(*svp) && SvTYPE(SvRV(*svp)) == SVt_PVAV)
    {
      av = (AV *)SvRV(*svp);
      for (i = AvFILL(av); i >= 0; --i)
      {
        svp = av_fetch(av, i, FALSE);
        if (!svp || !*svp || !sv_isobject(*svp))
          continue;

        ENTER;
        SAVETMPS;

        PUSHMARK(SP);
        EXTEND(SP, 1);
        PUSHs(sv_2mortal(newSVsv(*svp)));
        PUTBACK;

        call_method("disconnect", G_VOID|G_DISCARD|G_EVAL|G_KEEPERR);

        FREETMPS;
        LEAVE;
      }
    }
  }

  ret = 1;

  if (imp_drh->instances)
  {
    warn("DBD::MariaDB disconnect_all: %lu database handlers were not released (possible bug in driver)", imp_drh->instances);
    ret = 0;
  }

  if (imp_drh->embedded_started)
  {
    warn("DBD::MariaDB disconnect_all: Embedded server was not properly stopped (possible bug in driver)");
    ret = 0;
  }

  /* Some MariaDB and MySQL clients with Embedded server support have a bug which cause segfault
   * when trying to call mysql_server_end() when Embedded server was not started. So do not call
   * mysql_server_end() for normal connections when we have Embedded server support. */
#ifndef HAVE_EMBEDDED
  if (imp_drh->non_embedded_started)
  {
    mysql_server_end();
    imp_drh->non_embedded_started = FALSE;
    imp_drh->non_embedded_finished = TRUE;
  }
#endif

  if (imp_drh->embedded_args)
  {
    warn("DBD::MariaDB disconnect_all: mariadb_embedded_options was not released (possible bug in driver)");
    ret = 0;
  }

  if (imp_drh->embedded_groups)
  {
    warn("DBD::MariaDB disconnect_all: mariadb_embedded_groups was not released (possible bug in driver)");
    ret = 0;
  }

  return ret;
}


/****************************************************************************
 *
 *  Name:    mariadb_db_destroy
 *
 *  Purpose: Our part of the dbh destructor
 *
 *  Input:   dbh - database handle being destroyed
 *           imp_dbh - drivers private database handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void mariadb_db_destroy(SV* dbh, imp_dbh_t* imp_dbh) {

    /*
     *  Being on the safe side never hurts ...
     */
  if (DBIc_ACTIVE(imp_dbh))
  {
      if (!DBIc_has(imp_dbh, DBIcf_AutoCommit) && imp_dbh->pmysql)
        if (mysql_rollback(imp_dbh->pmysql))
          mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
    mariadb_db_disconnect(dbh, imp_dbh);
  }

  /* Tell DBI, that dbh->destroy must no longer be called */
  DBIc_off(imp_dbh, DBIcf_IMPSET);
}

/* 
 ***************************************************************************
 *
 *  Name:    mariadb_db_STORE_attrib
 *
 *  Purpose: Function for storing dbh attributes; we currently support
 *           just nothing. :-)
 *
 *  Input:   dbh - database handle being modified
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *           valuesv - the attribute value
 *
 *  Returns: 1 for success, 0 otherwise
 *
 **************************************************************************/
int
mariadb_db_STORE_attrib(
                    SV* dbh,
                    imp_dbh_t* imp_dbh,
                    SV* keysv,
                    SV* valuesv
                   )
{
  dTHX;
  STRLEN kl;
  char *key = SvPV(keysv, kl); /* needs to process get magic */
  const bool bool_value = SvTRUE_nomg(valuesv);

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(dbh, NULL))
  {
    mariadb_dr_do_error(dbh, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return 0;
  }

  if (memEQs(key, kl, "AutoCommit"))
  {
      bool oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit) ? TRUE : FALSE;

      if (bool_value == oldval)
        return 1;

      /* if setting AutoCommit on ... */
      if (!imp_dbh->no_autocommit_cmd)
      {
        if (
            mysql_autocommit(imp_dbh->pmysql, bool_value)
           )
        {
          mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
          return 1;  /* 1 means we handled it - important to avoid spurious errors */
        }
      }
      DBIc_set(imp_dbh, DBIcf_AutoCommit, bool_value);
  }
  else if (strBEGINs(key, "mariadb_"))
  {
    if (memEQs(key, kl, "mariadb_use_result"))
      imp_dbh->use_mysql_use_result = bool_value;
    else if (memEQs(key, kl, "mariadb_auto_reconnect"))
      imp_dbh->auto_reconnect = bool_value;
    else if (memEQs(key, kl, "mariadb_server_prepare"))
    {
      if (bool_value && imp_dbh->bind_type_guessing)
      {
        mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "mariadb_bind_type_guessing and mariadb_server_prepare cannot be enabled together", "HY000");
        return 0;
      }
      imp_dbh->use_server_side_prepare = bool_value;
    }
    else if (memEQs(key, kl, "mariadb_server_prepare_disable_fallback"))
      imp_dbh->disable_fallback_for_server_prepare = bool_value;
    else if (memEQs(key, kl, "mariadb_no_autocommit_cmd"))
      imp_dbh->no_autocommit_cmd = bool_value;
    else if (memEQs(key, kl, "mariadb_bind_type_guessing"))
    {
      if (bool_value && imp_dbh->use_server_side_prepare)
      {
        mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "mariadb_bind_type_guessing and mariadb_server_prepare cannot be enabled together", "HY000");
        return 0;
      }
      imp_dbh->bind_type_guessing = bool_value;
    }
    else if (memEQs(key, kl, "mariadb_bind_comment_placeholders"))
      imp_dbh->bind_comment_placeholders = bool_value;
  #ifdef HAVE_FABRIC
    else if (memEQs(key, kl, "mariadb_fabric_opt_group"))
    {
      STRLEN len;
      char *str = SvPVutf8_nomg(valuesv, len);
      if (strlen(str) != len)
      {
        error_nul_character(dbh, "mariadb_fabric_opt_group");
        return 0;
      }
      mysql_options(imp_dbh->pmysql, FABRIC_OPT_GROUP, str);
    }
    else if (memEQs(key, kl, "mariadb_fabric_opt_default_mode"))
    {
      if (SvOK(valuesv)) {
        STRLEN len;
        const char *str = SvPV_nomg(valuesv, len);
        if (len == 0 || memEQs(str, len, "ro") || memEQs(str, len, "rw"))
          mysql_options(imp_dbh->pmysql, FABRIC_OPT_DEFAULT_MODE, len == 0 ? NULL : str);
        else
        {
          mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "Valid settings for FABRIC_OPT_DEFAULT_MODE are 'ro', 'rw', or undef/empty string", "HY000");
          return 0;
        }
      }
      else {
        mysql_options(imp_dbh->pmysql, FABRIC_OPT_DEFAULT_MODE, NULL);
      }
    }
    else if (memEQs(key, kl, "mariadb_fabric_opt_mode"))
    {
      STRLEN len;
      const char *str = SvPV_nomg(valuesv, len);
      if (!memEQs(str, len, "ro") && !memEQs(str, len, "rw"))
      {
        mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "Valid settings for FABRIC_OPT_MODE are 'ro' or 'rw'", "HY000");
        return 0;
      }
      mysql_options(imp_dbh->pmysql, FABRIC_OPT_MODE, str);
    }
    else if (memEQs(key, kl, "mariadb_fabric_opt_group_credentials"))
    {
      mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "'fabric_opt_group_credentials' is not supported", "HY000");
      return 0;
    }
  #endif
    else if (memEQs(key, kl, "mariadb_max_allowed_packet"))
    {
#if (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50709 && MYSQL_VERSION_ID != 60000) || (defined(MARIADB_VERSION_ID) && MARIADB_VERSION_ID >= 100202)
      /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in mysql 5.7.9 */
      /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in MariaDB 10.2.2 */
      UV uv = SvUV_nomg(valuesv);
      unsigned long packet_size = (uv <= ULONG_MAX ? uv : ULONG_MAX);
      mysql_options(imp_dbh->pmysql, MYSQL_OPT_MAX_ALLOWED_PACKET, &packet_size);
#else
      /* before MySQL 5.7.9 and MariaDB 10.2.2 it is not possible to change max_allowed_packet after connection was established */
      if (imp_dbh->connected)
        mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "Changing mariadb_max_allowed_packet is not supported after connection was established", "HY000");
      return 0;
#endif
    }
    else
    {
      if (imp_dbh->connected) /* Ignore unknown attributes passed by DBI->connect, they are handled in mariadb_dr_connect() */
        error_unknown_attribute(dbh, key);
      return 0;
    }
  }
  else
  {
    if (!skip_attribute(key)) /* Not handled by this driver */
      error_unknown_attribute(dbh, key);
    return 0;
  }

  return 1;
}

#if IVSIZE < 8
static char *
my_ulonglong2str(my_ulonglong val, char *buf, STRLEN *len)
{
  char *ptr = buf + *len - 1;

  if (*len < 2)
  {
    *len = 0;
    return NULL;
  }

  if (val == 0)
  {
    buf[0] = '0';
    buf[1] = '\0';
    *len = 1;
    return buf;
  }

  *ptr = '\0';
  while (val > 0)
  {
    if (ptr == buf)
    {
      *len = 0;
      return NULL;
    }
    *(--ptr) = ('0' + (val % 10));
    val = val / 10;
  }

  *len = (buf + *len - 1) - ptr;
  return ptr;
}

static char*
signed_my_ulonglong2str(my_ulonglong val, char *buf, STRLEN *len)
{
  char *ptr;

  if (val <= (~(my_ulonglong)0 >> 1))
    return my_ulonglong2str(val, buf, len);

  ptr = my_ulonglong2str(-val, buf, len);
  if (!ptr || ptr == buf) {
    *len = 0;
    return NULL;
  }

  *(--ptr) = '-';
  *len += 1;
  return ptr;
}
#endif

SV*
mariadb_dr_my_ulonglong2sv(pTHX_ my_ulonglong val)
{
#if IVSIZE >= 8
  return newSVuv(val);
#else
  if (val <= UV_MAX)
  {
    return newSVuv(val);
  }
  else
  {
    char buf[64];
    STRLEN len = sizeof(buf);
    char *ptr = my_ulonglong2str(val, buf, &len);
    return newSVpvn(ptr, len);
  }
#endif
}

/***************************************************************************
 *
 *  Name:    mariadb_db_FETCH_attrib
 *
 *  Purpose: Function for fetching dbh attributes
 *
 *  Input:   dbh - database handle being queried
 *           imp_dbh - drivers private database handle data
 *           keysv - the attribute name
 *
 *  Returns: An SV*, if successful; NULL otherwise
 *
 *  Notes:   Do not forget to call sv_2mortal in the former case!
 *
 **************************************************************************/

SV* mariadb_db_FETCH_attrib(SV *dbh, imp_dbh_t *imp_dbh, SV *keysv)
{
  dTHX;
  STRLEN kl;
  char *key = SvPV(keysv, kl); /* needs to process get magic */
  SV* result = NULL;
  PERL_UNUSED_ARG(dbh);

  switch (*key) {
    case 'A':
      if (memEQs(key, kl, "AutoCommit"))
          return sv_2mortal(boolSV(DBIc_has(imp_dbh,DBIcf_AutoCommit)));
      break;
  }

  if (!strBEGINs(key, "mariadb_"))
  {
    if (!skip_attribute(key)) /* Not handled by this driver */
      error_unknown_attribute(dbh, key);
    return Nullsv;
  }
  else
  {
    if (memEQs(key, kl, "mariadb_auto_reconnect"))
      result = boolSV(imp_dbh->auto_reconnect);
    else if (memEQs(key, kl, "mariadb_bind_type_guessing"))
      result = boolSV(imp_dbh->bind_type_guessing);
    else if (memEQs(key, kl, "mariadb_bind_comment_placeholders"))
      result = boolSV(imp_dbh->bind_comment_placeholders);
    else if (memEQs(key, kl, "mariadb_clientinfo"))
    {
      const char* clientinfo = mysql_get_client_info();
      result = clientinfo ? sv_2mortal(newSVpv(clientinfo, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    }
    else if (memEQs(key, kl, "mariadb_clientversion"))
      result= sv_2mortal(newSVuv(mysql_get_client_version()));
    else if (memEQs(key, kl, "mariadb_errno"))
      result = imp_dbh->pmysql ? sv_2mortal(newSVuv(mysql_errno(imp_dbh->pmysql))) : &PL_sv_undef;
    else if (memEQs(key, kl, "mariadb_error"))
    {
      result = imp_dbh->pmysql ? sv_2mortal(newSVpv(mysql_error(imp_dbh->pmysql), 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    }
    else if (memEQs(key, kl, "mariadb_dbd_stats"))
    {
      HV* hv = newHV();
      result = sv_2mortal((newRV_noinc((SV*)hv)));
      (void)hv_stores(hv, "auto_reconnects_ok", newSViv(imp_dbh->stats.auto_reconnects_ok));
      (void)hv_stores(hv, "auto_reconnects_failed", newSViv(imp_dbh->stats.auto_reconnects_failed));
    }
    else if (memEQs(key, kl, "mariadb_hostinfo"))
    {
      const char *hostinfo = imp_dbh->pmysql ? mysql_get_host_info(imp_dbh->pmysql) : NULL;
      result = hostinfo ? sv_2mortal(newSVpv(hostinfo, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    }
    else if (memEQs(key, kl, "mariadb_info"))
    {
      const char *info = imp_dbh->pmysql ? mysql_info(imp_dbh->pmysql) : NULL;
      result = info ? sv_2mortal(newSVpv(info, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    }
    else if (memEQs(key, kl, "mariadb_insertid"))
    {
      /* We cannot return an IV, because the insertid is a long. */
      result = imp_dbh->pmysql ? sv_2mortal(my_ulonglong2sv(mysql_insert_id(imp_dbh->pmysql))) : &PL_sv_undef;
    }
    else if (memEQs(key, kl, "mariadb_max_allowed_packet"))
    {
      unsigned long packet_size;
#if (!defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50709 && MYSQL_VERSION_ID != 60000) || (defined(MARIADB_VERSION_ID) && MARIADB_VERSION_ID >= 100202)
      /* mysql_get_option() is not available in all versions */
      /* if we do not have mysql_get_option() we cannot retrieve max_allowed_packet */
      /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in mysql 5.7.9 */
      /* MYSQL_OPT_MAX_ALLOWED_PACKET was added in MariaDB 10.2.2 */
  #ifdef HAVE_GET_OPTION
      mysql_get_option(imp_dbh->pmysql, MYSQL_OPT_MAX_ALLOWED_PACKET, &packet_size);
  #else
      mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "Fetching mariadb_max_allowed_packet is not supported", "HY000");
      return Nullsv;
  #endif
#else
      /* before MySQL 5.7.9 and MariaDB 10.2.2 use max_allowed_packet macro */
      packet_size = max_allowed_packet;
#endif
      result = sv_2mortal(newSVuv(packet_size));
    }
    else if (memEQs(key, kl, "mariadb_no_autocommit_cmd"))
      result = boolSV(imp_dbh->no_autocommit_cmd);
    else if (memEQs(key, kl, "mariadb_protoinfo"))
      result = imp_dbh->pmysql ? sv_2mortal(newSViv(mysql_get_proto_info(imp_dbh->pmysql))) : &PL_sv_undef;
    else if (memEQs(key, kl, "mariadb_serverinfo"))
    {
      const char *serverinfo = imp_dbh->pmysql ? mysql_get_server_info(imp_dbh->pmysql) : NULL;
#ifndef MARIADB_BASE_VERSION
      /* serverinfo for MariaDB server from MySQL client is prefixed by string 5.5.5- */
      if (serverinfo && strBEGINs(serverinfo, "5.5.5-"))
          serverinfo += sizeof("5.5.5-")-1;
#endif
      result = serverinfo ? sv_2mortal(newSVpv(serverinfo, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    } 
    else if (memEQs(key, kl, "mariadb_ssl_cipher"))
    {
#if ((MYSQL_VERSION_ID >= 50023 && MYSQL_VERSION_ID < 50100) || MYSQL_VERSION_ID >= 50111)
      const char *ssl_cipher = imp_dbh->pmysql ? mysql_get_ssl_cipher(imp_dbh->pmysql) : NULL;
      result = ssl_cipher ? sv_2mortal(newSVpv(ssl_cipher, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
#else
      mariadb_dr_do_error(dbh, CR_UNKNOWN_ERROR, "Fetching mariadb_ssl_cipher is not supported", "HY000");
      return Nullsv;
#endif
    }
    else if (memEQs(key, kl, "mariadb_serverversion"))
    {
#ifndef MARIADB_BASE_VERSION
      unsigned int major, minor, patch;
      const char *serverinfo = imp_dbh->pmysql ? mysql_get_server_info(imp_dbh->pmysql) : NULL;
      /* serverinfo for MariaDB server from MySQL client is prefixed by string 5.5.5- */
      if (serverinfo && strBEGINs(serverinfo, "5.5.5-"))
      {
        /* And in this case mysql_get_server_version() returns just 50505 and not correct
         * MariaDB server version. So parse serverversion manually from serverinfo. */
        serverinfo += sizeof("5.5.5-")-1;
        if (sscanf(serverinfo, "%u.%u.%u", &major, &minor, &patch) == 3)
          result = sv_2mortal(newSVuv(10000UL * major + 100UL * minor + patch));
      }
#endif
      if (!result)
        result = imp_dbh->pmysql ? sv_2mortal(newSVuv(mysql_get_server_version(imp_dbh->pmysql))) : &PL_sv_undef;
    }
    else if (memEQs(key, kl, "mariadb_sock"))
      result = sv_2mortal(newSViv(PTR2IV(imp_dbh->pmysql)));
    else if (memEQs(key, kl, "mariadb_sockfd"))
      result = imp_dbh->pmysql ? sv_2mortal(newSViv(imp_dbh->pmysql->net.fd)) : &PL_sv_undef;
    else if (memEQs(key, kl, "mariadb_stat"))
    {
      const char *stats = imp_dbh->pmysql ? mysql_stat(imp_dbh->pmysql) : NULL;
      result = stats ? sv_2mortal(newSVpv(stats, 0)) : &PL_sv_undef;
      sv_utf8_decode(result);
    }
    else if (memEQs(key, kl, "mariadb_server_prepare"))
      result = boolSV(imp_dbh->use_server_side_prepare);
    else if (memEQs(key, kl, "mariadb_server_prepare_disable_fallback"))
      result = boolSV(imp_dbh->disable_fallback_for_server_prepare);
    else if (memEQs(key, kl, "mariadb_thread_id"))
      result = imp_dbh->pmysql ? sv_2mortal(newSViv(mysql_thread_id(imp_dbh->pmysql))) : &PL_sv_undef;
    else if (memEQs(key, kl, "mariadb_warning_count"))
      result = imp_dbh->pmysql ? sv_2mortal(newSViv(mysql_warning_count(imp_dbh->pmysql))) : &PL_sv_undef;
    else if (memEQs(key, kl, "mariadb_use_result"))
      result = boolSV(imp_dbh->use_mysql_use_result);
    else
    {
      error_unknown_attribute(dbh, key);
      return Nullsv;
    }
  }

  return result;
}

AV *mariadb_db_data_sources(SV *dbh, imp_dbh_t *imp_dbh, SV *attr)
{
  dTHX;
  SV *sv;
  AV *av;
  SSize_t i;
  MYSQL_RES *res;
  MYSQL_ROW row;
  MYSQL_FIELD* field;
  my_ulonglong num_rows;
  unsigned long *lengths;
  const char *prefix = "DBI:MariaDB:";
  const Size_t prefix_len = strlen(prefix);
  PERL_UNUSED_ARG(attr);

  ASYNC_CHECK_RETURN(dbh, NULL);

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(dbh, NULL))
  {
    mariadb_dr_do_error(dbh, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return NULL;
  }

  av = newAV();
  sv_2mortal((SV *)av);

  res = mysql_list_dbs(imp_dbh->pmysql, NULL);
  if (!res && mariadb_db_reconnect(dbh, NULL))
    res = mysql_list_dbs(imp_dbh->pmysql, NULL);
  if (!res)
  {
    mariadb_dr_do_error(dbh, mysql_errno(imp_dbh->pmysql),
                        mysql_error(imp_dbh->pmysql),
                        mysql_sqlstate(imp_dbh->pmysql));
    return NULL;
  }

  field = mysql_fetch_field(res);
  if (!field)
  {
    mariadb_dr_do_error(dbh, CR_NO_RESULT_SET, "No result list of databases", "HY000");
    return NULL;
  }

  num_rows = mysql_num_rows(res);
  if (num_rows == 0)
    return av;

  /* av_extend() extends array to size: arg+1 */
  --num_rows;

  /* Truncate list when is too big */
  if (num_rows > SSize_t_MAX)
    num_rows = SSize_t_MAX;

  av_extend(av, num_rows);

  i = 0;
  while ((row = mysql_fetch_row(res)))
  {
    if (!row[0])
      continue;

    lengths = mysql_fetch_lengths(res);

    /* newSV automatically adds extra byte for '\0' and does not set POK */
    sv = newSV(prefix_len + lengths[0]);
    av_store(av, i, sv);

    memcpy(SvPVX(sv), prefix, prefix_len);
    memcpy(SvPVX(sv)+prefix_len, row[0], lengths[0]);
    *(SvPVX(sv)+prefix_len+lengths[0]) = '\0';

    SvPOK_on(sv);
    SvCUR_set(sv, prefix_len + lengths[0]);

    if (mysql_charsetnr_is_utf8(field->charsetnr))
      sv_utf8_decode(sv);

    if ((my_ulonglong)i == num_rows+1)
      break;

    i++;
  }

  mysql_free_result(res);
  return av;
}

static int mariadb_st_free_result_sets (SV * sth, imp_sth_t * imp_sth);

/* 
 **************************************************************************
 *
 *  Name:    mariadb_st_prepare_sv
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *           statement - pointer to string with SQL statement
 *           attribs - statement attributes
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
mariadb_st_prepare_sv(
  SV *sth,
  imp_sth_t *imp_sth,
  SV *statement_sv,
  SV *attribs)
{
  int i;
  HV *hv;
  HE *he;
  SV **svp;
  HV *processed;
  char *statement;
  STRLEN statement_len;
  dTHX;
  int prepare_retval;
  MYSQL_BIND *bind, *bind_end;
  imp_sth_phb_t *fbind;
  unsigned long int num_params;
  D_imp_xxh(sth);
  D_imp_dbh_from_sth;

  if (imp_sth->statement)
  {
    mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "Statement is already prepared", "HY000");
    return 0;
  }

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(sth, NULL))
  {
    mariadb_dr_do_error(sth, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return 0;
  }

  statement = SvPVutf8_nomg(statement_sv, statement_len);
  imp_sth->statement = savepvn(statement, statement_len);
  imp_sth->statement_len = statement_len;

 /* Set default value of 'mariadb_server_prepare' attribute for sth from dbh */
  imp_sth->use_mysql_use_result = imp_dbh->use_mysql_use_result;
  imp_sth->use_server_side_prepare = imp_dbh->use_server_side_prepare;
  imp_sth->disable_fallback_for_server_prepare = imp_dbh->disable_fallback_for_server_prepare;

  imp_sth->fetch_done = FALSE;
  imp_sth->done_desc = FALSE;
  imp_sth->result = NULL;
  imp_sth->currow = 0;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                 "\t-> mariadb_st_prepare_sv MYSQL_VERSION_ID %d, SQL statement: %.1000s%s\n",
                  MYSQL_VERSION_ID, statement, statement_len > 1000 ? "..." : "");

  if (attribs)
  {
    processed = newHV();
    sv_2mortal(newRV_noinc((SV *)processed)); /* Automatically free HV processed */

    /* Automatically processed by DBI */
    (void)hv_stores(processed, "Slice", &PL_sv_yes);
    (void)hv_stores(processed, "Columns", &PL_sv_yes);
    (void)hv_stores(processed, "MaxRows", &PL_sv_yes);

    (void)hv_stores(processed, "mariadb_server_prepare", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_server_prepare");
    imp_sth->use_server_side_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->use_server_side_prepare;

    (void)hv_stores(processed, "mariadb_server_prepare_disable_fallback", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_server_prepare_disable_fallback");
    imp_sth->disable_fallback_for_server_prepare = (svp) ?
      SvTRUE(*svp) : imp_dbh->disable_fallback_for_server_prepare;

    (void)hv_stores(processed, "mariadb_async", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_async");
    if(svp && SvTRUE(*svp)) {
        imp_sth->is_async = TRUE;
        if (imp_sth->disable_fallback_for_server_prepare)
        {
          mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED,
                   "Async option not supported with server side prepare", "HY000");
          return 0;
        }
        imp_sth->use_server_side_prepare = FALSE;
    }

    /* Set default value of 'mariadb_use_result' attribute for sth from dbh */
    (void)hv_stores(processed, "mariadb_use_result", &PL_sv_yes);
    svp = MARIADB_DR_ATTRIB_GET_SVPS(attribs, "mariadb_use_result");
    imp_sth->use_mysql_use_result= svp ?
      SvTRUE(*svp) : imp_dbh->use_mysql_use_result;

    hv = (HV*) SvRV(attribs);
    hv_iterinit(hv);
    while ((he = hv_iternext(hv)) != NULL)
    {
      I32 len;
      const char *key;
      key = hv_iterkey(he, &len);
      if (hv_exists(processed, key, len))
        continue;
      error_unknown_attribute(sth, key);
      return 0;
    }
  }

  for (i= 0; i < AV_ATTRIB_LAST; i++)
    imp_sth->av_attr[i]= Nullav;

  /*
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  mariadb_st_free_result_sets(sth, imp_sth);

  if (imp_sth->use_server_side_prepare)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tuse_server_side_prepare set\n");

    imp_sth->stmt= mysql_stmt_init(imp_dbh->pmysql);

    if (! imp_sth->stmt)
    {
      mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
      return 0;
    }

    prepare_retval= mysql_stmt_prepare(imp_sth->stmt,
                                       statement,
                                       statement_len);

    if (prepare_retval && mariadb_db_reconnect(sth, imp_sth->stmt))
    {
        mysql_stmt_close(imp_sth->stmt);
        imp_sth->stmt = mysql_stmt_init(imp_dbh->pmysql);
        if (!imp_sth->stmt)
        {
          mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), mysql_sqlstate(imp_dbh->pmysql));
          return 0;
        }
        prepare_retval= mysql_stmt_prepare(imp_sth->stmt,
                                           statement,
                                           statement_len);
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tmysql_stmt_prepare returned %d\n",
                      prepare_retval);

    if (prepare_retval)
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tmysql_stmt_prepare %d %s\n",
                      mysql_stmt_errno(imp_sth->stmt),
                      mysql_stmt_error(imp_sth->stmt));

      /* For commands that are not supported by server side prepared statement
         mechanism lets try to pass them through regular API */
      if (!imp_sth->disable_fallback_for_server_prepare &&
          (mysql_stmt_errno(imp_sth->stmt) == ER_UNSUPPORTED_PS ||
          /* And also fallback when placeholder is used in unsupported
           * construction with old server versions (e.g. LIMIT ?) */
          (mysql_stmt_errno(imp_sth->stmt) == ER_PARSE_ERROR &&
           mysql_get_server_version(imp_dbh->pmysql) < 50007 &&
           strstr(mysql_stmt_error(imp_sth->stmt), "'?"))))
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tSETTING imp_sth->use_server_side_prepare to FALSE\n");
        mysql_stmt_close(imp_sth->stmt);
        imp_sth->stmt = NULL;
        imp_sth->use_server_side_prepare = FALSE;
      }
      else
      {
        mariadb_dr_do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt),
                mysql_sqlstate(imp_dbh->pmysql));
        mysql_stmt_close(imp_sth->stmt);
        imp_sth->stmt= NULL;
        return 0;
      }
    }
    else
    {
      num_params = mysql_stmt_param_count(imp_sth->stmt);
      if (num_params > INT_MAX)
      {
        mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "Prepared statement contains too many placeholders", "HY000");
        mysql_stmt_close(imp_sth->stmt);
        imp_sth->stmt = NULL;
        return 0;
      }
      DBIc_NUM_PARAMS(imp_sth) = num_params;
      if (DBIc_NUM_PARAMS(imp_sth) > 0)
      {
        /* Allocate memory for bind variables */
        imp_sth->bind=            alloc_bind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->fbind=           alloc_fbind(DBIc_NUM_PARAMS(imp_sth));
        imp_sth->has_been_bound = FALSE;

        /* Initialize ph variables with  NULL values */
        for (i= 0,
             bind=      imp_sth->bind,
             fbind=     imp_sth->fbind,
             bind_end=  bind+DBIc_NUM_PARAMS(imp_sth);
             bind < bind_end ;
             bind++, fbind++, i++ )
        {
          bind->buffer_type=  MYSQL_TYPE_STRING;
          bind->buffer=       NULL;
          bind->length=       &(fbind->length);
          bind->is_null=      &(fbind->is_null);
          fbind->is_null=     TRUE;
          fbind->length=      0;
        }
      }
    }
  }

  /* Count the number of parameters (driver, vs server-side) */
  if (!imp_sth->use_server_side_prepare)
  {
    num_params = count_params(imp_dbh, aTHX_ statement, statement_len,
                                            imp_dbh->bind_comment_placeholders);
    if (num_params > INT_MAX || num_params == ULONG_MAX)
    {
      mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "Prepared statement contains too many placeholders", "HY000");
      mysql_stmt_close(imp_sth->stmt);
      imp_sth->stmt = NULL;
      return 0;
    }
    DBIc_NUM_PARAMS(imp_sth) = num_params;
  }

  /* Allocate memory for parameters */
  if (DBIc_NUM_PARAMS(imp_sth) > 0)
    imp_sth->params = alloc_param(DBIc_NUM_PARAMS(imp_sth));
  DBIc_IMPSET_on(imp_sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_prepare_sv\n");
  return 1;
}

/***************************************************************************
 * Name: mariadb_st_free_result_sets
 *
 * Purpose: Clean-up single or multiple result sets (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: 1 ok
 *          0 error
 *************************************************************************/
static int mariadb_st_free_result_sets (SV * sth, imp_sth_t * imp_sth)
{
  dTHX;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
  int next_result_rc= -1;

  if (!imp_dbh->pmysql)
    return 0;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t>- mariadb_st_free_result_sets\n");

  do
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_free_result_sets RC %d\n", next_result_rc);

    if (next_result_rc == 0)
    {
      if (!(imp_sth->result = mysql_use_result(imp_dbh->pmysql)))
      {
        /* Check for possible error */
        if (mysql_field_count(imp_dbh->pmysql))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_free_result_sets ERROR: %s\n",
                                  mysql_error(imp_dbh->pmysql));

          mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
                   mysql_sqlstate(imp_dbh->pmysql));
          return 0;
        }
      }
    }
    if (imp_sth->result)
    {
      mysql_free_result(imp_sth->result);
      imp_sth->result=NULL;
    }
  } while ((next_result_rc=mysql_next_result(imp_dbh->pmysql))==0);

  if (next_result_rc > 0)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_free_result_sets: Error while processing multi-result set: %s\n",
                    mysql_error(imp_dbh->pmysql));

    mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
             mysql_sqlstate(imp_dbh->pmysql));
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_free_result_sets\n");

  return 1;
}


/***************************************************************************
 * Name: mariadb_st_more_results
 *
 * Purpose: Move onto the next result set (if any)
 *
 * Inputs: sth - Statement handle
 *         imp_sth - driver's private statement handle
 *
 * Returns: TRUE if there are more results sets
 *          FALSE if there are not
 *************************************************************************/
bool mariadb_st_more_results(SV* sth, imp_sth_t* imp_sth)
{
  dTHX;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);

  bool use_mysql_use_result = imp_sth->use_mysql_use_result;
  int next_result_return_code, i;

  if (!SvROK(sth) || SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(sth, NULL))
  {
    mariadb_dr_do_error(sth, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return FALSE;
  }

  if (!mysql_more_results(imp_dbh->pmysql))
  {
    /* No more pending result set(s)*/
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
		    "\n      <- mariadb_st_more_results no more results\n");
    return FALSE;
  }

  if (imp_sth->use_server_side_prepare)
  {
    mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED, "Processing of multiple result set is not possible with server side prepare", "HY000");
    return FALSE;
  }

  /*
   *  Free cached array attributes
   */
  for (i= 0; i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  /* Release previous MySQL result*/
  if (imp_sth->result)
  {
    mysql_free_result(imp_sth->result);
    imp_sth->result= NULL;
  }

  if (DBIc_ACTIVE(imp_sth))
    DBIc_ACTIVE_off(imp_sth);

  next_result_return_code= mysql_next_result(imp_dbh->pmysql);

  imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);

  /*
    mysql_next_result returns
      0 if there are more results
     -1 if there are no more results
     >0 if there was an error
   */
  if (next_result_return_code > 0)
  {
    mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql),
             mysql_sqlstate(imp_dbh->pmysql));

    return FALSE;
  }
  else if(next_result_return_code == -1)                                                                                                                  
  {                                                                                                                                                       
    return FALSE;
  }  
  else
  {
    /* Store the result from the Query */
    imp_sth->result = use_mysql_use_result ?
     mysql_use_result(imp_dbh->pmysql) : mysql_store_result(imp_dbh->pmysql);

    if (mysql_errno(imp_dbh->pmysql))
    {
      mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql), mysql_error(imp_dbh->pmysql), 
               mysql_sqlstate(imp_dbh->pmysql));
      return FALSE;
    }

    imp_sth->row_num= mysql_affected_rows(imp_dbh->pmysql);

    if (imp_sth->result == NULL)
    {
      /* No "real" rowset*/
      DBIS->set_attr_k(sth, sv_2mortal(newSVpvs("NUM_OF_FIELDS")), 0,
			               sv_2mortal(newSViv(0)));
      return TRUE;
    }
    else
    {
      /* We have a new rowset */
      imp_sth->currow=0;


      /* delete cached handle attributes */
      /* XXX should be driven by a list to ease maintenance */
      (void)hv_deletes((HV*)SvRV(sth), "NAME", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "NULLABLE", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "NUM_OF_FIELDS", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "PRECISION", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "SCALE", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "TYPE", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_insertid", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_is_auto_increment", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_is_blob", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_is_key", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_is_num", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_is_pri_key", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_length", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_max_length", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_table", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_type", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_type_name", G_DISCARD);
      (void)hv_deletes((HV*)SvRV(sth), "mariadb_warning_count", G_DISCARD);

      /* Adjust NUM_OF_FIELDS - which also adjusts the row buffer size */
      DBIc_DBISTATE(imp_sth)->set_attr_k(sth, sv_2mortal(newSVpvs("NUM_OF_FIELDS")), 0,
          sv_2mortal(newSVuv(mysql_num_fields(imp_sth->result)))
      );

      DBIc_ACTIVE_on(imp_sth);

      imp_sth->done_desc = FALSE;
    }
    imp_dbh->pmysql->net.last_errno= 0;
    return TRUE;
  }
}

/**************************************************************************
 *
 *  Name:    mariadb_st_internal_execute
 *
 *  Purpose: Internal version for executing a statement, called both from
 *           within the "do" and the "execute" method.
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           num_params - number of parameters being bound
 *           params - parameter array
 *           result - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/


my_ulonglong mariadb_st_internal_execute(
                                       SV *h, /* could be sth or dbh */
                                       char *sbuf,
                                       STRLEN slen,
                                       int num_params,
                                       imp_sth_ph_t *params,
                                       MYSQL_RES **result,
                                       MYSQL **svsock,
                                       bool use_mysql_use_result
                                      )
{
  dTHX;
  bool bind_type_guessing= FALSE;
  bool bind_comment_placeholders= TRUE;
  char *salloc;
  int htype;
  bool async = FALSE;
  my_ulonglong rows= 0;
  /* thank you DBI.c for this info! */
  D_imp_xxh(h);

  htype= DBIc_TYPE(imp_xxh);
  /*
    It is important to import imp_dbh properly according to the htype
    that it is! Also, one might ask why bind_type_guessing is assigned
    in each block. Well, it's because D_imp_ macros called in these
    blocks make it so imp_dbh is not "visible" or defined outside of the
    if/else (when compiled, it fails for imp_dbh not being defined).
  */
  /* h is a dbh */
  if (htype == DBIt_DB)
  {
    D_imp_dbh(h);
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
    {
      bind_type_guessing= imp_dbh->bind_type_guessing;
      bind_comment_placeholders= imp_dbh->bind_comment_placeholders;
    }
    async = imp_dbh->async_query_in_flight ? TRUE : FALSE;
  }
  /* h is a sth */
  else
  {
    D_imp_sth(h);
    D_imp_dbh_from_sth;
    /* if imp_dbh is not available, it causes segfault (proper) on OpenBSD */
    if (imp_dbh)
    {
      bind_type_guessing= imp_dbh->bind_type_guessing;
      bind_comment_placeholders= imp_dbh->bind_comment_placeholders;
    }
    async = imp_sth->is_async;
    imp_dbh->async_query_in_flight = async ? imp_sth : NULL;
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "mariadb_st_internal_execute MYSQL_VERSION_ID %d\n",
                  MYSQL_VERSION_ID );

  if (!*svsock && !mariadb_db_reconnect(h, NULL))
  {
    mariadb_dr_do_error(h, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return -1;
  }

  salloc= parse_params(imp_xxh,
                              aTHX_ *svsock,
                              sbuf,
                              &slen,
                              params,
                              num_params,
                              bind_type_guessing,
                              bind_comment_placeholders);

  if (salloc)
  {
    sbuf= salloc;
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "Binding parameters: %.1000s%s\n", sbuf, slen > 1000 ? "..." : "");
  }

  if(async) {
    if((mysql_send_query(*svsock, sbuf, slen)) &&
       (!mariadb_db_reconnect(h, NULL) ||
        (mysql_send_query(*svsock, sbuf, slen))))
    {
        rows = -1;
    } else {
        rows = 0;
    }
  } else {
#ifdef HAVE_BROKEN_INSERT_ID_AFTER_SELECT
      /*
       * mysql_insert_id() returns incorrect value after SELECT operation.
       * As a workaround prior to issuing mysql query we store value of
       * last insert id. If query returns result set then we know that it
       * was SELECT operation and so after that we restore previous value
       * of last insert id.
       */
      my_ulonglong insertid = mysql_insert_id(*svsock);
#endif

      if ((mysql_real_query(*svsock, sbuf, slen))  &&
          (!mariadb_db_reconnect(h, NULL) ||
           (mysql_real_query(*svsock, sbuf, slen))))
      {
        rows = -1;
      } else {
          /** Store the result from the Query */
          *result= use_mysql_use_result ?
            mysql_use_result(*svsock) : mysql_store_result(*svsock);

          if (mysql_errno(*svsock))
            rows = -1;
          else if (*result)
            rows = mysql_num_rows(*result);
          else {
            rows = mysql_affected_rows(*svsock);
          }

#ifdef HAVE_BROKEN_INSERT_ID_AFTER_SELECT
          if (*result)
            (*svsock)->insert_id = insertid;
#endif
      }
  }

  if (salloc)
    Safefree(salloc);

  if (rows == (my_ulonglong)-1)
    mariadb_dr_do_error(h, mysql_errno(*svsock), mysql_error(*svsock), 
             mysql_sqlstate(*svsock));

  return(rows);
}

 /**************************************************************************
 *
 *  Name:    mariadb_st_internal_execute41
 *
 *  Purpose: Internal version for executing a prepared statement, called both
 *           from within the "do" and the "execute" method.
 *           MYSQL 4.1 API
 *
 *
 *  Inputs:  h - object handle, for storing error messages
 *           statement - query being executed
 *           attribs - statement attributes, currently ignored
 *           has_params - non-zero parameters being bound
 *           params - parameter array
 *           result - where to store results, if any
 *           svsock - socket connected to the database
 *
 **************************************************************************/

my_ulonglong mariadb_st_internal_execute41(
                                         SV *h,
                                         char *sbuf,
                                         STRLEN slen,
                                         bool has_params,
                                         MYSQL_RES **result,
                                         MYSQL_STMT **stmt_ptr,
                                         MYSQL_BIND *bind,
                                         MYSQL **svsock,
                                         bool *has_been_bound
                                        )
{
  dTHX;
  int store_retval;
  int execute_retval;
  unsigned int i, num_fields;
  MYSQL_STMT *stmt = *stmt_ptr;
  my_ulonglong rows=0;
  bool reconnected = FALSE;
  D_imp_xxh(h);

#ifdef HAVE_BROKEN_INSERT_ID_AFTER_SELECT
  my_ulonglong insertid;
#endif

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t-> mariadb_st_internal_execute41\n");

  /* free result if exists */
  if (*result)
  {
    mysql_free_result(*result);
    *result = NULL;
  }

  if (!*svsock)
  {
    if (!mariadb_db_reconnect(h, NULL))
    {
      mariadb_dr_do_error(h, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
      return -1;
    }
    reconnected = TRUE;
  }

  /*
    If were performed any changes with ph variables
    we have to rebind them
  */

  if (!reconnected && has_params && !(*has_been_bound))
  {
    if (mysql_stmt_bind_param(stmt,bind) == 0)
    {
      *has_been_bound = TRUE;
    }
    else
    {
      if (!mariadb_db_reconnect(h, stmt))
        goto error;
      reconnected = TRUE;
    }
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmariadb_st_internal_execute41 calling mysql_execute\n");

#ifdef HAVE_BROKEN_INSERT_ID_AFTER_SELECT
  /*
   * mysql_insert_id() returns incorrect value after SELECT operation.
   * As a workaround prior to issuing mysql query we store value of
   * last insert id. If query returns result set then we know that it
   * was SELECT operation and so after that we restore previous value
   * of last insert id. Restoring needs to be done after call to the
   * mysql_stmt_store_result() function as it may clear insert id.
   */
  insertid = mysql_insert_id(*svsock);
#endif

  if (!reconnected)
  {
    execute_retval = mysql_stmt_execute(stmt);
    if (execute_retval && mariadb_db_reconnect(h, stmt))
      reconnected = TRUE;
  }
  if (reconnected)
  {
    *has_been_bound = FALSE;
    stmt = mysql_stmt_init(*svsock);
    if (!stmt)
    {
      mariadb_dr_do_error(h, mysql_errno(*svsock), mysql_error(*svsock), mysql_sqlstate(*svsock));
      return -1;
    }
    if (mysql_stmt_prepare(stmt, sbuf, slen))
    {
      mariadb_dr_do_error(h, mysql_stmt_errno(stmt), mysql_stmt_error(stmt), mysql_stmt_sqlstate(stmt));
      mysql_stmt_close(stmt);
      return -1;
    }
    mysql_stmt_close(*stmt_ptr);
    *stmt_ptr = stmt;
    if (has_params)
    {
      if (mysql_stmt_bind_param(stmt,bind))
        goto error;
      *has_been_bound = TRUE;
    }
    execute_retval= mysql_stmt_execute(stmt);
  }
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\tmysql_stmt_execute returned %d\n",
                  execute_retval);
  if (execute_retval)
    goto error;

  /*
   This statement does not return a result set (INSERT, UPDATE...)
  */
  if (!(*result= mysql_stmt_result_metadata(stmt)))
  {
    if (mysql_stmt_errno(stmt))
      goto error;

    rows= mysql_stmt_affected_rows(stmt);

    /* mysql_stmt_affected_rows(): -1 indicates that the query returned an error */
    if (rows == (my_ulonglong)-1)
      goto error;
  }
  /*
    This statement returns a result set (SELECT...)
  */
  else
  {
    num_fields = mysql_stmt_field_count(stmt);
    for (i = 0; i < num_fields; ++i)
    {
      MYSQL_FIELD *field = mysql_fetch_field_direct(*result, i);
      if (field && mysql_field_needs_allocated_buffer(field))
      {
        /* mysql_stmt_store_result to update MYSQL_FIELD->max_length */
        my_bool on = TRUE;
        mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &on);
        break;
      }
    }
    store_retval = mysql_stmt_store_result(stmt);
#ifdef HAVE_BROKEN_INSERT_ID_AFTER_SELECT
    (*svsock)->insert_id = insertid;
#endif
    if (store_retval)
      goto error;
    /* Get the total rows affected and return */
    rows = mysql_stmt_num_rows(stmt);
  }
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t<- mysql_internal_execute_41 returning %" SVf " rows\n",
                  SVfARG(sv_2mortal(my_ulonglong2sv(rows))));
  return(rows);

error:
  if (*result)
  {
    mysql_free_result(*result);
    *result = NULL;
  }
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "     errno %d err message %s\n",
                  mysql_stmt_errno(stmt),
                  mysql_stmt_error(stmt));
  mariadb_dr_do_error(h, mysql_stmt_errno(stmt), mysql_stmt_error(stmt),
           mysql_stmt_sqlstate(stmt));
  mysql_stmt_reset(stmt);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t<- mariadb_st_internal_execute41\n");
  return -1;

}

/***************************************************************************
 *
 *  Name:    mariadb_st_execute_iv
 *
 *  Purpose: Called for preparing an SQL statement; our part of the
 *           statement handle constructor
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: -2 for errors, -1 for unknown number of rows, otherwise number
 *           of rows; mariadb_dr_do_error will be called for errors
 *
 **************************************************************************/

IV mariadb_st_execute_iv(SV* sth, imp_sth_t* imp_sth)
{
  dTHX;
  int i;
  unsigned int num_fields;
  D_imp_dbh_from_sth;
  D_imp_xxh(sth);
  bool use_server_side_prepare = imp_sth->use_server_side_prepare;
  bool disable_fallback_for_server_prepare = imp_sth->disable_fallback_for_server_prepare;

  ASYNC_CHECK_RETURN(sth, -2);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
      " -> mariadb_st_execute_iv for %p\n", sth);

  if (!SvROK(sth)  ||  SvTYPE(SvRV(sth)) != SVt_PVHV)
    croak("Expected hash array");

  for (i = 0; i < DBIc_NUM_PARAMS(imp_sth); ++i)
  {
    if (!imp_sth->params[i].bound)
    {
      mariadb_dr_do_error(sth, ER_WRONG_ARGUMENTS, "Wrong number of bind parameters", "HY000");
      return -2;
    }
  }

  /* Free cached array attributes */
  for (i= 0;  i < AV_ATTRIB_LAST;  i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);

    imp_sth->av_attr[i]= Nullav;
  }

  /* 
     Clean-up previous result set(s) for sth to prevent
     'Commands out of sync' error 
  */
  mariadb_st_free_result_sets (sth, imp_sth);

  if (use_server_side_prepare)
  {
    if (imp_sth->use_mysql_use_result)
    {
      if (disable_fallback_for_server_prepare)
      {
        mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED,
                 "\"mariadb_use_result\" not supported with server side prepare",
                 "HY000");
        return -2;
      }
      use_server_side_prepare = FALSE;
    }

    if (use_server_side_prepare)
    {
      imp_sth->row_num= mariadb_st_internal_execute41(
                                                    sth,
                                                    imp_sth->statement,
                                                    imp_sth->statement_len,
                                                    !!(DBIc_NUM_PARAMS(imp_sth) > 0),
                                                    &imp_sth->result,
                                                    &imp_sth->stmt,
                                                    imp_sth->bind,
                                                    &imp_dbh->pmysql,
                                                    &imp_sth->has_been_bound
                                                   );
      if (imp_sth->row_num == (my_ulonglong)-1) /* -1 means error */
      {
        SV *err = DBIc_ERR(imp_xxh);
        if (!disable_fallback_for_server_prepare && SvIV(err) == ER_UNSUPPORTED_PS)
        {
          use_server_side_prepare = FALSE;
        }
      }
    }
  }

  if (!use_server_side_prepare)
  {
    imp_sth->row_num= mariadb_st_internal_execute(
                                                sth,
                                                imp_sth->statement,
                                                imp_sth->statement_len,
                                                DBIc_NUM_PARAMS(imp_sth),
                                                imp_sth->params,
                                                &imp_sth->result,
                                                &imp_dbh->pmysql,
                                                imp_sth->use_mysql_use_result
                                               );
    if(imp_dbh->async_query_in_flight) {
        DBIc_ACTIVE_on(imp_sth);
        return 0;
    }
  }

  if (imp_sth->row_num != (my_ulonglong)-1)
  {
    if (!imp_sth->result)
    {
      imp_sth->insertid= mysql_insert_id(imp_dbh->pmysql);
      if (mysql_more_results(imp_dbh->pmysql))
        DBIc_ACTIVE_on(imp_sth);
    }
    else
    {
      /** Store the result in the current statement handle */
      num_fields = mysql_num_fields(imp_sth->result);
      DBIc_NUM_FIELDS(imp_sth) = (num_fields <= INT_MAX) ? num_fields : INT_MAX;
      DBIc_ACTIVE_on(imp_sth);
      if (!use_server_side_prepare)
        imp_sth->done_desc = FALSE;
      imp_sth->fetch_done = FALSE;
    }
  }

  imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  " <- mariadb_st_execute_iv returning imp_sth->row_num %" SVf "\n",
                  SVfARG(sv_2mortal(my_ulonglong2sv(imp_sth->row_num))));
  }

  if (imp_sth->row_num == (my_ulonglong)-1)
    return -2; /* -2 is error */
  else if (imp_sth->row_num <= IV_MAX)
    return imp_sth->row_num;
  else         /* overflow */
    return -1; /* -1 is unknown number of rows */
}

 /**************************************************************************
 *
 *  Name:    mariadb_st_describe
 *
 *  Purpose: Called from within the fetch method to describe the result
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - our part of the statement handle, there's no
 *               need for supplying both; Tim just doesn't remove it
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error will
 *           be called in the latter case
 *
 **************************************************************************/

static int mariadb_st_describe(SV* sth, imp_sth_t* imp_sth)
{
  dTHX;
  D_imp_xxh(sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t--> mariadb_st_describe\n");

  if (imp_sth->use_server_side_prepare)
  {
    int i;
    int num_fields= DBIc_NUM_FIELDS(imp_sth);
    imp_sth_fbh_t *fbh;
    MYSQL_BIND *buffer;
    MYSQL_FIELD *fields;

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmariadb_st_describe() num_fields %d\n",
                    num_fields);

    if (imp_sth->done_desc)
      return 1;

    if (num_fields <= 0 || !imp_sth->result)
    {
      /* no metadata */
      mariadb_dr_do_error(sth, CR_NO_STMT_METADATA, "Prepared statement contains no metadata", "HY000");
      return 0;
    }

    /* allocate fields buffers  */
    if (  !(imp_sth->fbh= alloc_fbuffer(num_fields))
          || !(imp_sth->buffer= alloc_bind(num_fields)) )
    {
      /* Out of memory */
      mariadb_dr_do_error(sth, CR_OUT_OF_MEMORY, "Out of memory in mariadb_st_describe()", "HY000");
      return 0;
    }

    fields= mysql_fetch_fields(imp_sth->result);

    for (
         fbh= imp_sth->fbh, buffer= (MYSQL_BIND*)imp_sth->buffer, i= 0;
         i < num_fields;
         i++, fbh++, buffer++
        )
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      {
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\ti %d fbh->length %lu\n",
                      i, fbh->length);
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "\t\tfields[i].length %lu fields[i].max_length %lu fields[i].type %d fields[i].flags %d fields[i].charsetnr %d\n",
                      fields[i].length, fields[i].max_length, fields[i].type, fields[i].flags, fields[i].charsetnr);
      }

      fbh->is_utf8 = mysql_charsetnr_is_utf8(fields[i].charsetnr);

      buffer->buffer_type= fields[i].type;
      buffer->is_unsigned= (fields[i].flags & UNSIGNED_FLAG) ? TRUE : FALSE;
      buffer->length= &(fbh->length);
      buffer->is_null= &(fbh->is_null);
#if MYSQL_VERSION_ID >= NEW_DATATYPE_VERSION
      buffer->error= &(fbh->error);
#endif

      if (mysql_field_needs_string_type(&fields[i]))
        buffer->buffer_type = MYSQL_TYPE_STRING;

      switch (buffer->buffer_type) {
      case MYSQL_TYPE_NULL:
        buffer->buffer_length= 0;
        buffer->buffer= NULL;

      case MYSQL_TYPE_TINY:
        buffer->buffer_length= sizeof(fbh->numeric_val.tval);
        buffer->buffer= (char*) &fbh->numeric_val.tval;
        break;

      case MYSQL_TYPE_SHORT:
        buffer->buffer_length= sizeof(fbh->numeric_val.sval);
        buffer->buffer= (char*) &fbh->numeric_val.sval;
        break;

      case MYSQL_TYPE_LONG:
        buffer->buffer_length= sizeof(fbh->numeric_val.lval);
        buffer->buffer= (char*) &fbh->numeric_val.lval;
        break;

      case MYSQL_TYPE_LONGLONG:
        buffer->buffer_length= sizeof(fbh->numeric_val.llval);
        buffer->buffer= (char*) &fbh->numeric_val.llval;
        break;

      case MYSQL_TYPE_FLOAT:
        buffer->buffer_length= sizeof(fbh->numeric_val.fval);
        buffer->buffer= (char*) &fbh->numeric_val.fval;
        break;

      case MYSQL_TYPE_DOUBLE:
        buffer->buffer_length= sizeof(fbh->numeric_val.dval);
        buffer->buffer= (char*) &fbh->numeric_val.dval;
        break;

      /* TODO: datetime structures */
#if 0
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        break;
#endif

      default:
        if (buffer->buffer_type != MYSQL_TYPE_BLOB)
          buffer->buffer_type= MYSQL_TYPE_STRING;
        buffer->buffer_length= fields[i].max_length ? fields[i].max_length : 1;
        Newz(908, fbh->data, buffer->buffer_length, char);
        buffer->buffer= (char *) fbh->data;
        break;
      }
    }

    if (mysql_stmt_bind_result(imp_sth->stmt, imp_sth->buffer))
    {
      mariadb_dr_do_error(sth, mysql_stmt_errno(imp_sth->stmt),
               mysql_stmt_error(imp_sth->stmt),
               mysql_stmt_sqlstate(imp_sth->stmt));
      return 0;
    }
  }

  imp_sth->done_desc = TRUE;
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_describe\n");
  return 1;
}

/**************************************************************************
 *
 *  Name:    mariadb_st_fetch
 *
 *  Purpose: Called for fetching a result row
 *
 *  Input:   sth - statement handle being initialized
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: array of columns; the array is allocated by DBI via
 *           DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth), even the values
 *           of the array are prepared, we just need to modify them
 *           appropriately
 *
 **************************************************************************/

AV*
mariadb_st_fetch(SV *sth, imp_sth_t* imp_sth)
{
  dTHX;
  bool ChopBlanks;
  int rc;
  unsigned int i, num_fields;
  unsigned long *lengths;
  AV *av;
  unsigned int av_length;
  bool av_readonly;
  MYSQL_ROW cols;
  D_imp_dbh_from_sth;
  imp_sth_fbh_t *fbh;
  D_imp_xxh(sth);
  MYSQL_BIND *buffer;
  IV int_val;
  const char *int_type;
  MYSQL_FIELD *fields;
  bool rebind_result;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t-> mariadb_st_fetch\n");

  if (!imp_dbh->pmysql)
    return Nullav;

  if(imp_dbh->async_query_in_flight) {
      if (mariadb_db_async_result(sth, &imp_sth->result) == (my_ulonglong)-1)
        return Nullav;
  }

  if (imp_sth->use_server_side_prepare)
  {
    if (!DBIc_ACTIVE(imp_sth) )
    {
      mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "no statement executing", "HY000");
      return Nullav;
    }

    if (imp_sth->fetch_done)
    {
      mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "fetch() but fetch already done", "HY000");
      return Nullav;
    }

    if (!imp_sth->done_desc)
    {
      if (!mariadb_st_describe(sth, imp_sth))
        return Nullav;
    }
  }

  ChopBlanks = DBIc_is(imp_sth, DBIcf_ChopBlanks) ? TRUE : FALSE;

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\tmariadb_st_fetch for %p, chopblanks %d\n",
                  sth, ChopBlanks ? 1 : 0);

  if (!imp_sth->result)
  {
    mariadb_dr_do_error(sth, CR_UNKNOWN_ERROR, "fetch() without execute()", "HY000");
    return Nullav;
  }

  /* fix from 2.9008 */
  imp_dbh->pmysql->net.last_errno = 0;

  if (imp_sth->use_server_side_prepare)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmariadb_st_fetch calling mysql_fetch\n");

    if ((rc= mysql_stmt_fetch(imp_sth->stmt)))
    {
#if MYSQL_VERSION_ID >= 50003
      if (rc == MYSQL_DATA_TRUNCATED) {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmariadb_st_fetch data truncated\n");
        goto process;
      }
#endif

      if (rc == MYSQL_NO_DATA)
      {
        /* Update row_num to affected_rows value */
        imp_sth->row_num= mysql_stmt_affected_rows(imp_sth->stmt);
        imp_sth->fetch_done = TRUE;
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tmariadb_st_fetch no data\n");
      }
      else if (rc == 1)
      {
        mariadb_dr_do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                 mysql_stmt_error(imp_sth->stmt),
                 mysql_stmt_sqlstate(imp_sth->stmt));
      }

      return Nullav;
    }

process:
    imp_sth->currow++;

    av= DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth);
    num_fields=mysql_stmt_field_count(imp_sth->stmt);
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                    "\t\tmariadb_st_fetch called mysql_fetch, rc %d num_fields %u\n",
                    rc, num_fields);

    rebind_result = FALSE;
    for (
         buffer= imp_sth->buffer,
         fbh= imp_sth->fbh,
         i= 0;
         i < num_fields;
         i++,
         fbh++,
         buffer++
        )
    {
        /* In case of BLOB/TEXT fields we allocate only few bytes
           in mariadb_st_describe() for data. Here we know real size of field
           so we should increase buffer size and refetch column value
        */
        if (!fbh->is_null && mysql_type_needs_allocated_buffer(buffer->buffer_type) && (fbh->length > buffer->buffer_length || fbh->error))
        {
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
              "\t\tRefetch BLOB/TEXT column: %d, length: %lu, error: %d\n",
              i, fbh->length, fbh->error ? 1 : 0);

          Renew(fbh->data, fbh->length, char);
          buffer->buffer_length= fbh->length;
          buffer->buffer= (char *) fbh->data;

          /* We invalidated fbh->data, therefore we must call mysql_stmt_bind_result()
           * prior exiting this function. But we cannot call mysql_stmt_bind_result()
           * before all remaining mysql_stmt_fetch_column() calls because we would get
           * again truncated data. */
          rebind_result = TRUE;

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2) {
            char *ptr = (char*)buffer->buffer;
            unsigned long int j, m;
            m = buffer->buffer_length;
            if (m > *buffer->length)
              m = *buffer->length;
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\tbefore buffer->buffer: ");
            for (j = 0; j < m; j++) {
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%c", *ptr++);
            }
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\n");
          }

          /*TODO: Use offset instead of 0 to fetch only remain part of data*/
          if (mysql_stmt_fetch_column(imp_sth->stmt, buffer , i, 0))
          {
            mariadb_dr_do_error(sth, mysql_stmt_errno(imp_sth->stmt),
                     mysql_stmt_error(imp_sth->stmt),
                     mysql_stmt_sqlstate(imp_sth->stmt));
            mysql_stmt_bind_result(imp_sth->stmt, imp_sth->buffer);
            return Nullav;
          }

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2) {
            char *ptr = (char*)buffer->buffer;
            unsigned long int j, m;
            m = buffer->buffer_length;
            if (m > *buffer->length)
              m = *buffer->length;
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\t\tafter buffer->buffer: ");
            for (j = 0; j < m; j++) {
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "%c", *ptr++);
            }
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),"\n");
          }
        }
    }

    if (rebind_result)
    {
      if (mysql_stmt_bind_result(imp_sth->stmt, imp_sth->buffer))
      {
        mariadb_dr_do_error(sth, mysql_stmt_errno(imp_sth->stmt), mysql_stmt_error(imp_sth->stmt), mysql_stmt_sqlstate(imp_sth->stmt));
        return Nullav;
      }
    }

    for (
         buffer= imp_sth->buffer,
         fbh= imp_sth->fbh,
         i= 0;
         i < num_fields;
         i++,
         fbh++,
         buffer++
        )
    {
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/
      STRLEN len;

      if (fbh->is_null)
        (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
      else
      {
        switch (buffer->buffer_type) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_LONG:
#if IVSIZE >= 8
        case MYSQL_TYPE_LONGLONG:
#endif
          switch (buffer->buffer_type) {
          case MYSQL_TYPE_TINY:
            if (buffer->is_unsigned)
              int_val= (unsigned char)fbh->numeric_val.tval;
            else
              int_val= (signed char)fbh->numeric_val.tval;
            int_type= "TINY INT";
            break;

          case MYSQL_TYPE_SHORT:
            if (buffer->is_unsigned)
              int_val= (unsigned short)fbh->numeric_val.sval;
            else
              int_val= (signed short)fbh->numeric_val.sval;
            int_type= "SHORT INT";
            break;

          case MYSQL_TYPE_LONG:
            if (buffer->is_unsigned)
              int_val= (uint32_t)fbh->numeric_val.lval;
            else
              int_val= (int32_t)fbh->numeric_val.lval;
            int_type= "LONG INT";
            break;

#if IVSIZE >= 8
          case MYSQL_TYPE_LONGLONG:
            if (buffer->is_unsigned)
              int_val= fbh->numeric_val.llval;
            else
              int_val= fbh->numeric_val.llval;
            int_type= "LONGLONG INT";
            break;
#endif
          default:
            NOT_REACHED;
            int_val = 0;
            int_type = "";
            break;
          }

          if (buffer->is_unsigned)
            sv_setuv(sv, (UV)int_val);
          else
            sv_setiv(sv, (IV)int_val);

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          {
            if (buffer->is_unsigned)
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch AN UNSIGNED %s NUMBER %"UVuf"\n",
                            int_type, (UV)int_val);
            else
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch A SIGNED %s NUMBER %"IVdf"\n",
                            int_type, (IV)int_val);
          }
          break;

#if IVSIZE < 8
        case MYSQL_TYPE_LONGLONG:
          {
            char buf[64];
            STRLEN len = sizeof(buf);
            char *ptr;

            if (buffer->is_unsigned)
              ptr = my_ulonglong2str(fbh->numeric_val.llval, buf, &len);
            else
              ptr = signed_my_ulonglong2str(fbh->numeric_val.llval, buf, &len);

            SvUTF8_off(sv);
            sv_setpvn(sv, ptr, len);

            if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
              PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch %s LONGLONG INT NUMBER %s\n",
                            (buffer->is_unsigned ? "AN UNSIGNED" : "A SIGNED"), ptr);
          }
          break;
#endif

        case MYSQL_TYPE_FLOAT:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch A FLOAT NUMBER %f\n", fbh->numeric_val.fval);
          sv_setnv(sv, fbh->numeric_val.fval);
          break;

        case MYSQL_TYPE_DOUBLE:
          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tst_fetch A DOUBLE NUMBER %f\n", fbh->numeric_val.dval);
          sv_setnv(sv, fbh->numeric_val.dval);
          break;

        /* TODO: datetime structures */
  #if 0
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
          break;
  #endif

        case MYSQL_TYPE_NULL:
          (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
          break;

        default:
          /* TEXT columns can be returned as MYSQL_TYPE_BLOB, so always check for charset */
          len= fbh->length;
	  /* ChopBlanks server-side prepared statement */
          if (ChopBlanks)
          {
            if (fbh->is_utf8)
              while (len && fbh->data[len-1] == ' ') { --len; }
          }
	  /* END OF ChopBlanks */

          SvUTF8_off(sv);
          sv_setpvn(sv, fbh->data, len);
          if (fbh->is_utf8)
            sv_utf8_decode(sv);
          break;
        }
      }
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_fetch, %u cols\n", num_fields);

    return av;
  }
  else
  {
    imp_sth->currow++;

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    {
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmariadb_st_fetch result set details\n");
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\timp_sth->result=%p\n", imp_sth->result);
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_num_fields=%u\n",
                    mysql_num_fields(imp_sth->result));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_num_rows=%" SVf "\n",
                    SVfARG(sv_2mortal(my_ulonglong2sv(mysql_num_rows(imp_sth->result)))));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmysql_affected_rows=%" SVf "\n",
                    SVfARG(sv_2mortal(my_ulonglong2sv(mysql_affected_rows(imp_dbh->pmysql)))));
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmariadb_st_fetch for %p, currow= %d\n",
                    sth,imp_sth->currow);
    }

    if (!(cols= mysql_fetch_row(imp_sth->result)))
    {
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      {
        PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tmariadb_st_fetch, no more rows to fetch");
      }
      if (mysql_errno(imp_dbh->pmysql))
        mariadb_dr_do_error(sth, mysql_errno(imp_dbh->pmysql),
                 mysql_error(imp_dbh->pmysql),
                 mysql_sqlstate(imp_dbh->pmysql));
      return Nullav;
    }

    num_fields= mysql_num_fields(imp_sth->result);
    fields= mysql_fetch_fields(imp_sth->result);
    lengths= mysql_fetch_lengths(imp_sth->result);

    if ((av= DBIc_FIELDS_AV(imp_sth)) != Nullav)
    {
      av_length= av_len(av)+1;

      if (av_length != num_fields)              /* Resize array if necessary */
      {
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_fetch, size of results array(%u) != num_fields(%u)\n",
                                   av_length, num_fields);

        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_fetch, result fields(%d)\n",
                                   DBIc_NUM_FIELDS(imp_sth));

        av_readonly = SvREADONLY(av) ? TRUE : FALSE;

        if (av_readonly)
          SvREADONLY_off( av );              /* DBI sets this readonly */

        while (av_length < num_fields)
        {
          av_store(av, av_length++, newSV(0));
        }

        while (av_length > num_fields)
        {
          SvREFCNT_dec(av_pop(av));
          av_length--;
        }
        if (av_readonly)
          SvREADONLY_on(av);
      }
    }

    av= DBIc_DBISTATE(imp_sth)->get_fbav(imp_sth);

    for (i= 0;  i < num_fields; ++i)
    {
      char *col= cols[i];
      SV *sv= AvARRAY(av)[i]; /* Note: we (re)use the SV in the AV	*/

      if (col)
      {
        STRLEN len= lengths[i];
        if (ChopBlanks)
        {
          if (mysql_charsetnr_is_utf8(fields[i].charsetnr))
          while (len && col[len-1] == ' ')
          {	--len; }
        }

        /* Set string value returned from mysql server */
        SvUTF8_off(sv);
        sv_setpvn(sv, col, len);

        switch (mysql_to_perl_type(fields[i].type)) {
        case PERL_TYPE_NUMERIC:
          if (!mysql_field_needs_string_type(&fields[i]))
          {
            /* Coerce to dobule and set scalar as NV */
            sv_setnv(sv, SvNV(sv));
          }
          break;

        case PERL_TYPE_INTEGER:
          if (!mysql_field_needs_string_type(&fields[i]))
          {
            /* Coerce to integer and set scalar as UV resp. IV */
            if (fields[i].flags & UNSIGNED_FLAG)
              sv_setuv(sv, SvUV_nomg(sv));
            else
              sv_setiv(sv, SvIV_nomg(sv));
          }
          break;

        case PERL_TYPE_UNDEF:
          /* Field is NULL, return undef */
          (void) SvOK_off(sv);
          break;

        default:
          /* TEXT columns can be returned as MYSQL_TYPE_BLOB, so always check for charset */
          if (mysql_charsetnr_is_utf8(fields[i].charsetnr))
            sv_utf8_decode(sv);
          break;
        }
      }
      else
        (void) SvOK_off(sv);  /*  Field is NULL, return undef  */
    }

    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t<- mariadb_st_fetch, %u cols\n", num_fields);
    return av;
  }

}

/***************************************************************************
 *
 *  Name:    mariadb_st_finish
 *
 *  Purpose: Called for freeing a mysql result
 *
 *  Input:   sth - statement handle being finished
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error() will
 *           be called in the latter case
 *
 **************************************************************************/

int mariadb_st_finish(SV* sth, imp_sth_t* imp_sth) {
  dTHX;
  D_imp_xxh(sth);
  D_imp_dbh_from_sth;

  if(imp_dbh->async_query_in_flight) {
    mariadb_db_async_result(sth, &imp_sth->result);
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\n--> mariadb_st_finish\n");
  }

  if (imp_sth->use_server_side_prepare && imp_sth->stmt)
  {
    /*
      We have to fetch all data from stmt
      There is may be useful for 2 cases:
      1. st_finish when we have undef statement
      2. call st_execute again when we have some unfetched data in stmt
     */
    if (DBIc_ACTIVE(imp_sth) && mariadb_st_describe(sth, imp_sth) && !imp_sth->fetch_done)
      mysql_stmt_free_result(imp_sth->stmt);
  }

  /*
    Cancel further fetches from this cursor.
    We don't close the cursor till DESTROY.
    The application may re execute it.
  */
  if (imp_sth && DBIc_ACTIVE(imp_sth))
  {
    /*
      Clean-up previous result set(s) for sth to prevent
      'Commands out of sync' error
    */
    mariadb_st_free_result_sets(sth, imp_sth);
  }
  DBIc_ACTIVE_off(imp_sth);
  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
  {
    PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\n<-- mariadb_st_finish\n");
  }
  return 1;
}


/**************************************************************************
 *
 *  Name:    mariadb_st_destroy
 *
 *  Purpose: Our part of the statement handles destructor
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *
 *  Returns: Nothing
 *
 **************************************************************************/

void mariadb_st_destroy(SV *sth, imp_sth_t *imp_sth) {
  dTHX;
  D_imp_xxh(sth);

  int i;

  imp_sth_fbh_t *fbh;
  int num_params;
  int num_fields;

  if (imp_sth->statement)
    Safefree(imp_sth->statement);

  num_params = DBIc_NUM_PARAMS(imp_sth);
  if (num_params > 0)
  {
    if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
      PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\tFreeing %d parameters, bind %p fbind %p\n",
          num_params, imp_sth->bind, imp_sth->fbind);

    free_bind(imp_sth->bind);
    free_fbind(imp_sth->fbind);
  }

  fbh= imp_sth->fbh;
  if (fbh)
  {
    num_fields = DBIc_NUM_FIELDS(imp_sth);
    i = 0;
    while (i < num_fields)
    {
      if (fbh[i].data) Safefree(fbh[i].data);
      ++i;
    }

    free_fbuffer(fbh);
    if (imp_sth->buffer)
      free_bind(imp_sth->buffer);
  }

  if (imp_sth->stmt)
  {
    mysql_stmt_close(imp_sth->stmt);
    imp_sth->stmt= NULL;
  }

  /* mariadb_st_finish has already been called by .xs code if needed.	*/

  /* Free values allocated by mariadb_st_bind_ph */
  if (imp_sth->params)
  {
    free_param(aTHX_ imp_sth->params, num_params);
    imp_sth->params= NULL;
  }

  /* Free cached array attributes */
  for (i= 0; i < AV_ATTRIB_LAST; i++)
  {
    if (imp_sth->av_attr[i])
      SvREFCNT_dec(imp_sth->av_attr[i]);
    imp_sth->av_attr[i]= Nullav;
  }
  /* let DBI know we've done it   */
  DBIc_IMPSET_off(imp_sth);
}


/*
 **************************************************************************
 *
 *  Name:    mariadb_st_STORE_attrib
 *
 *  Purpose: Modifies a statement handles attributes; we currently
 *           support just nothing
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *           valuesv - attribute value
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error will
 *           be called in the latter case
 *
 **************************************************************************/
int
mariadb_st_STORE_attrib(
                    SV *sth,
                    imp_sth_t *imp_sth,
                    SV *keysv,
                    SV *valuesv
                   )
{
  dTHX;
  STRLEN(kl);
  char *key= SvPV(keysv, kl); /* needs to process get magic */
  int retval = 0;
  D_imp_xxh(sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\t-> mariadb_st_STORE_attrib for %p, key %s\n",
                  sth, key);

  if (memEQs(key, kl, "mariadb_use_result"))
  {
    imp_sth->use_mysql_use_result= SvTRUE_nomg(valuesv);
    retval = 1;
  }
  else
  {
    if (!skip_attribute(key)) /* Not handled by this driver */
      error_unknown_attribute(sth, key);
  }

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "\t\t<- mariadb_st_STORE_attrib for %p, result %d\n",
                  sth, retval);

  return retval;
}


/*
 **************************************************************************
 *
 *  Name:    mariadb_st_fetch_internal
 *
 *  Purpose: Retrieves a statement handles array attributes; we use
 *           a separate function, because creating the array
 *           attributes shares much code and it aids in supporting
 *           enhanced features like caching.
 *
 *  Input:   sth - statement handle; may even be a database handle,
 *               in which case this will be used for storing error
 *               messages only. This is only valid, if cacheit (the
 *               last argument) is set to TRUE.
 *           what - internal attribute number
 *           res - pointer to a DBMS result
 *           cacheit - TRUE, if results may be cached in the sth.
 *
 *  Returns: RV pointing to result array in case of success, NULL
 *           otherwise; mariadb_dr_do_error has already been called in the latter
 *           case.
 *
 **************************************************************************/

#define IS_KEY(A) (((A) & (PRI_KEY_FLAG | UNIQUE_KEY_FLAG | MULTIPLE_KEY_FLAG)) != 0)
#define IS_AUTO_INCREMENT(A) (((A) & AUTO_INCREMENT_FLAG) != 0)

static SV* mariadb_st_fetch_internal(
  SV *sth,
  int what,
  MYSQL_RES *res,
  bool cacheit
)
{
  dTHX;
  D_imp_sth(sth);
  AV *av= Nullav;
  MYSQL_FIELD *curField;
  unsigned int length;

  /* Are we asking for a legal value? */
  if (what < 0 ||  what >= AV_ATTRIB_LAST)
    mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED, "Not implemented", "HY000");

  /* Return cached value, if possible */
  else if (cacheit  &&  imp_sth->av_attr[what])
    av= imp_sth->av_attr[what];

  /* Does this sth really have a result? */
  else if (!res)
    mariadb_dr_do_error(sth, CR_NO_RESULT_SET, "No result set associated with the statement", "HY000");
  /* Do the real work. */
  else
  {
    av= newAV();
    mysql_field_seek(res, 0);
    while ((curField= mysql_fetch_field(res)))
    {
      SV *sv;

      switch(what) {
      case AV_ATTRIB_NAME:
        length = curField->name_length;
#if MYSQL_VERSION_ID < 50500 || (defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100204) || defined(MARIADB_PACKAGE_VERSION)
        /* MySQL clients prior to 5.5.0, MariaDB clients 10.2.4+ and all MariaDB Connector/C clients
         * fill uninitialized value for length in prepared statements, so calculate length it manually */
        if (imp_sth->stmt)
          length = strlen(curField->name);
#endif
        sv= newSVpvn(curField->name, length);
        if (mysql_charsetnr_is_utf8(curField->charsetnr))
          sv_utf8_decode(sv);
        break;

      case AV_ATTRIB_TABLE:
        length = curField->table_length;
#if MYSQL_VERSION_ID < 50500 || (defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 100204) || defined(MARIADB_PACKAGE_VERSION)
        /* MySQL clients prior to 5.5.0, MariaDB clients 10.2.4+ and all MariaDB Connector/C clients
         * fill uninitialized value for length in prepared statements, so calculate length it manually */
        if (imp_sth->stmt)
          length = strlen(curField->table);
#endif
        sv= newSVpvn(curField->table, length);
        if (mysql_charsetnr_is_utf8(curField->charsetnr))
          sv_utf8_decode(sv);
        break;

      case AV_ATTRIB_TYPE:
        sv= newSVuv(curField->type);
        break;

      case AV_ATTRIB_SQL_TYPE:
        sv= newSVuv(native2sql(curField->type)->data_type);
        break;
      case AV_ATTRIB_IS_PRI_KEY:
        sv= boolSV(IS_PRI_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_NOT_NULL:
        sv= boolSV(IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_NULLABLE:
        sv= boolSV(!IS_NOT_NULL(curField->flags));
        break;

      case AV_ATTRIB_LENGTH:
        sv= newSVuv(curField->length);
        break;

      case AV_ATTRIB_IS_NUM:
        sv= boolSV(native2sql(curField->type)->is_num);
        break;

      case AV_ATTRIB_TYPE_NAME:
        sv= newSVpv(native2sql(curField->type)->type_name, 0);
        break;

      case AV_ATTRIB_MAX_LENGTH:
        sv= newSVuv(curField->max_length);
        break;

      case AV_ATTRIB_IS_AUTO_INCREMENT:
        sv= boolSV(IS_AUTO_INCREMENT(curField->flags));
        break;

      case AV_ATTRIB_IS_KEY:
        sv= boolSV(IS_KEY(curField->flags));
        break;

      case AV_ATTRIB_IS_BLOB:
        sv= boolSV(IS_BLOB(curField->flags));
        break;

      case AV_ATTRIB_SCALE:
        sv= newSVuv(curField->decimals);
        break;

      case AV_ATTRIB_PRECISION:
        sv= newSVuv((curField->length > curField->max_length) ?
                     curField->length : curField->max_length);
        break;

      default:
        sv= &PL_sv_undef;
        break;
      }
      av_push(av, sv);
    }

    /* Ensure that this value is kept, decremented in
     *  mariadb_st_destroy and mariadb_st_execute_iv.  */
    if (!cacheit)
      return sv_2mortal(newRV_noinc((SV*)av));
    imp_sth->av_attr[what]= av;
  }

  if (av == Nullav)
    return &PL_sv_undef;

  return sv_2mortal(newRV_inc((SV*)av));
}


/*
 **************************************************************************
 *
 *  Name:    mariadb_st_FETCH_attrib
 *
 *  Purpose: Retrieves a statement handles attributes
 *
 *  Input:   sth - statement handle being destroyed
 *           imp_sth - drivers private statement handle data
 *           keysv - attribute name
 *
 *  Returns: NULL for an unknown attribute, "undef" for error,
 *           attribute value otherwise.
 *
 **************************************************************************/

#define ST_FETCH_AV(what) \
    mariadb_st_fetch_internal(sth, (what), imp_sth->result, TRUE)

SV* mariadb_st_FETCH_attrib(
                          SV *sth,
                          imp_sth_t *imp_sth,
                          SV *keysv
                         )
{
  dTHX;
  STRLEN(kl);
  char *key= SvPV(keysv, kl); /* needs to process get magic */
  SV *retsv= Nullsv;
  D_imp_xxh(sth);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "    -> mariadb_st_FETCH_attrib for %p, key %s\n",
                  sth, key);

  switch (*key) {
  case 'N':
    if (memEQs(key, kl, "NAME"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NAME);
    else if (memEQs(key, kl, "NULLABLE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_NULLABLE);
    break;
  case 'P':
    if (memEQs(key, kl, "PRECISION"))
      retsv= ST_FETCH_AV(AV_ATTRIB_PRECISION);
    else if (memEQs(key, kl, "ParamValues"))
    {
        HV *pvhv= newHV();
        if (DBIc_NUM_PARAMS(imp_sth) > 0)
        {
            int i;
            char key[100];
            I32 keylen;
            SV *sv;
            for (i = 0; i < DBIc_NUM_PARAMS(imp_sth); i++)
            {
                keylen = sprintf(key, "%d", i);
                sv = newSVpvn(imp_sth->params[i].value, imp_sth->params[i].len);
                if (!sql_type_is_binary(imp_sth->params[i].type))
                  sv_utf8_decode(sv);
                (void)hv_store(pvhv, key, keylen, sv, 0);
            }
        }
        retsv= sv_2mortal(newRV_noinc((SV*)pvhv));
    }
    break;
  case 'S':
    if (memEQs(key, kl, "SCALE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SCALE);
    break;
  case 'T':
    if (memEQs(key, kl, "TYPE"))
      retsv= ST_FETCH_AV(AV_ATTRIB_SQL_TYPE);
    break;
  case 'm':
      if (memEQs(key, kl, "mariadb_type"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TYPE);
      else if (memEQs(key, kl, "mariadb_sock"))
        retsv= (imp_sth->stmt) ? sv_2mortal(newSViv(PTR2IV(imp_sth->stmt->mysql))) : boolSV(0);
      else if (memEQs(key, kl, "mariadb_table"))
        retsv= ST_FETCH_AV(AV_ATTRIB_TABLE);
      else if (memEQs(key, kl, "mariadb_is_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_KEY);
      else if (memEQs(key, kl, "mariadb_is_num"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_NUM);
      else if (memEQs(key, kl, "mariadb_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_LENGTH);
      else if (memEQs(key, kl, "mariadb_result"))
        retsv= sv_2mortal(newSViv(PTR2IV(imp_sth->result)));
      else if (memEQs(key, kl, "mariadb_is_blob"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_BLOB);
      else if (memEQs(key, kl, "mariadb_insertid"))
      {
        /* We cannot return an IV, because the insertid is a long.  */
        retsv= sv_2mortal(my_ulonglong2sv(imp_sth->insertid));
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "INSERT ID %" SVf "\n", SVfARG(retsv));
      }
      else if (memEQs(key, kl, "mariadb_type_name"))
        retsv = ST_FETCH_AV(AV_ATTRIB_TYPE_NAME);
      else if (memEQs(key, kl, "mariadb_is_pri_key"))
        retsv= ST_FETCH_AV(AV_ATTRIB_IS_PRI_KEY);
      else if (memEQs(key, kl, "mariadb_max_length"))
        retsv= ST_FETCH_AV(AV_ATTRIB_MAX_LENGTH);
      else if (memEQs(key, kl, "mariadb_use_result"))
        retsv= boolSV(imp_sth->use_mysql_use_result);
      else if (memEQs(key, kl, "mariadb_warning_count"))
        retsv= sv_2mortal(newSVuv(imp_sth->warning_count));
      else if (memEQs(key, kl, "mariadb_server_prepare"))
        retsv= boolSV(imp_sth->use_server_side_prepare);
      else if (memEQs(key, kl, "mariadb_is_auto_increment"))
        retsv = ST_FETCH_AV(AV_ATTRIB_IS_AUTO_INCREMENT);
      else if (memEQs(key, kl, "mariadb_server_prepare_disable_fallback"))
        retsv= boolSV(imp_sth->disable_fallback_for_server_prepare);
    break;
  }
  if (retsv == Nullsv)
  {
    if (!skip_attribute(key)) /* Not handled by this driver */
      error_unknown_attribute(sth, key);
  }
  return retsv;
}


SV *mariadb_st_last_insert_id(SV *sth, imp_sth_t *imp_sth,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr)
{
  dTHX;
  PERL_UNUSED_ARG(sth);
  PERL_UNUSED_ARG(catalog);
  PERL_UNUSED_ARG(schema);
  PERL_UNUSED_ARG(table);
  PERL_UNUSED_ARG(field);
  PERL_UNUSED_ARG(attr);

  return sv_2mortal(my_ulonglong2sv(imp_sth->insertid));
}


/***************************************************************************
 *
 *  Name:    mariadb_st_blob_read
 *
 *  Purpose: Used for blob reads if the statement handles "LongTruncOk"
 *           attribute (currently not supported by DBD::MariaDB)
 *
 *  Input:   SV* - statement handle from which a blob will be fetched
 *           imp_sth - drivers private statement handle data
 *           field - field number of the blob (note, that a row may
 *               contain more than one blob)
 *           offset - the offset of the field, where to start reading
 *           len - maximum number of bytes to read
 *           destrv - RV* that tells us where to store
 *           destoffset - destination offset
 *
 *  Returns: 1 for success, 0 otherwise; mariadb_dr_do_error will
 *           be called in the latter case
 *
 **************************************************************************/

int mariadb_st_blob_read (
  SV *sth,
  imp_sth_t *imp_sth,
  int field,
  long offset,
  long len,
  SV *destrv,
  long destoffset)
{
  PERL_UNUSED_ARG(sth);
  PERL_UNUSED_ARG(imp_sth);
  PERL_UNUSED_ARG(field);
  PERL_UNUSED_ARG(offset);
  PERL_UNUSED_ARG(len);
  PERL_UNUSED_ARG(destrv);
  PERL_UNUSED_ARG(destoffset);
  mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED, "blob_read not implemented", "HY000");
  return 0;
}


/***************************************************************************
 *
 *  Name:    mariadb_st_bind_ph
 *
 *  Purpose: Binds a statement value to a parameter
 *
 *  Input:   sth - statement handle
 *           imp_sth - drivers private statement handle data
 *           param - parameter number, counting starts with 1
 *           value - value being inserted for parameter "param"
 *           sql_type - SQL type of the value
 *           attribs - bind parameter attributes, currently this must be
 *               one of the values SQL_CHAR, ...
 *           inout - TRUE, if parameter is an output variable (currently
 *               this is not supported)
 *           maxlen - ???
 *
 *  Returns: 1 for success, 0 otherwise
 *
 **************************************************************************/

int mariadb_st_bind_ph(SV *sth, imp_sth_t *imp_sth, SV *param, SV *value,
		 IV sql_type, SV *attribs, int is_inout, IV maxlen) {
  dTHX;
  IV param_num = SvIV(param); /* needs to process get magic */
  int idx;
  char *err_msg;
  D_imp_xxh(sth);
  D_imp_dbh_from_sth;
  PERL_UNUSED_ARG(attribs);
  PERL_UNUSED_ARG(maxlen);

  char *buffer= NULL;
  my_bool buffer_is_null = FALSE;
  my_bool buffer_is_unsigned = FALSE;
  unsigned long int buffer_length = 0;
  unsigned int buffer_type= 0;
  IV int_val= 0;
  const char *int_type = "";

  ASYNC_CHECK_RETURN(sth, FALSE);

  if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
    PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                  "   Called: mariadb_st_bind_ph\n");

  if (param_num <= 0  ||  param_num > DBIc_NUM_PARAMS(imp_sth))
  {
    mariadb_dr_do_error(sth, CR_INVALID_PARAMETER_NO, "Illegal parameter number", "HY000");
    return 0;
  }

  idx = param_num - 1;

  /*
     This fixes the bug whereby no warning was issued upon binding a
     defined non-numeric as numeric
   */
  if (SvOK(value) && sql_type_is_numeric(sql_type))
  {
    if (! looks_like_number(value))
    {
      err_msg = SvPVX(sv_2mortal(newSVpvf(
              "Binding non-numeric field %" IVdf ", value %s as a numeric!",
              param_num, neatsvpv(value,0))));
      mariadb_dr_do_error(sth, CR_INVALID_PARAMETER_NO, err_msg, "HY000");
      return 0;
    }
  }

  if (is_inout)
  {
    mariadb_dr_do_error(sth, CR_NOT_IMPLEMENTED, "Output parameters not implemented", "HY000");
    return 0;
  }

  bind_param(&imp_sth->params[idx], value, sql_type);

  if (imp_sth->use_server_side_prepare)
  {
    buffer_is_null = !imp_sth->params[idx].value;
    if (!buffer_is_null) {
      buffer_type= sql_to_mysql_type(sql_type);
      switch (buffer_type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
#if IVSIZE >= 8
      case MYSQL_TYPE_LONGLONG:
#endif
        if (!SvIOK(value) && DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND AN INT NUMBER\n");
        int_val= SvIV_nomg(value);
        /* SvIV and SvUV may modify SvIsUV flag, on overflow maximal value is returned */
        if (SvIsUV(value) || int_val == IV_MAX)
        {
          int_val = SvUV_nomg(value);
          buffer_is_unsigned = TRUE;
        }

        switch (buffer_type) {
        case MYSQL_TYPE_TINY:
          buffer_length= sizeof(imp_sth->fbind[idx].numeric_val.tval);
          if (int_val > SCHAR_MAX)
            buffer_is_unsigned = TRUE;
          if (buffer_is_unsigned)
            imp_sth->fbind[idx].numeric_val.tval= (unsigned char)((UV)int_val);
          else
            imp_sth->fbind[idx].numeric_val.tval= (signed char)((IV)int_val);
          buffer= (void*)&(imp_sth->fbind[idx].numeric_val.tval);
          int_val= imp_sth->fbind[idx].numeric_val.tval;
          int_type= "TINY INT";
          break;

        case MYSQL_TYPE_SHORT:
          buffer_length= sizeof(imp_sth->fbind[idx].numeric_val.sval);
          if (int_val > SHRT_MAX)
            buffer_is_unsigned = TRUE;
          if (buffer_is_unsigned)
            imp_sth->fbind[idx].numeric_val.sval= (unsigned short)((UV)int_val);
          else
            imp_sth->fbind[idx].numeric_val.sval= (signed short)((IV)int_val);
          buffer= (void*)&(imp_sth->fbind[idx].numeric_val.sval);
          int_val= imp_sth->fbind[idx].numeric_val.sval;
          int_type= "SHORT INT";
          break;

        case MYSQL_TYPE_LONG:
          buffer_length= sizeof(imp_sth->fbind[idx].numeric_val.lval);
          if (int_val > INT32_MAX)
            buffer_is_unsigned = TRUE;
          if (buffer_is_unsigned)
            imp_sth->fbind[idx].numeric_val.lval= (uint32_t)((UV)int_val);
          else
            imp_sth->fbind[idx].numeric_val.lval= (int32_t)((IV)int_val);
          buffer= (void*)&(imp_sth->fbind[idx].numeric_val.lval);
          int_val= imp_sth->fbind[idx].numeric_val.lval;
          int_type= "LONG INT";
          break;

#if IVSIZE >= 8
        case MYSQL_TYPE_LONGLONG:
          buffer_length= sizeof(imp_sth->fbind[idx].numeric_val.llval);
          if (int_val >= 0 && (UV)int_val > (~(my_ulonglong)0 >> 1))
            buffer_is_unsigned = TRUE;
          if (buffer_is_unsigned)
            imp_sth->fbind[idx].numeric_val.llval= (UV)int_val;
          else
            imp_sth->fbind[idx].numeric_val.llval= (IV)int_val;
          int_val= imp_sth->fbind[idx].numeric_val.llval;
          int_type= "LONGLONG INT";
          buffer= (void*)&(imp_sth->fbind[idx].numeric_val.llval);
          break;
#endif
        }

        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        {
          if (buffer_is_unsigned)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR sql_type %"IVdf" ->%"UVuf"<- IS AN UNSIGNED %s NUMBER\n",
                          sql_type, (UV)int_val, int_type);
          else
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR sql_type %"IVdf" ->%"IVdf"<- IS A SIGNED %s NUMBER\n",
                          sql_type, (IV)int_val, int_type);
        }
        break;

#if IVSIZE < 8
      case MYSQL_TYPE_LONGLONG:
        {
          char *buf;
          my_ulonglong val;

          buffer_length= sizeof(imp_sth->fbind[idx].numeric_val.llval);

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
            PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND AN LONGLONG INT NUMBER FROM STRING\n");

          buf= SvPV_nomg_nolen(value);
          val= strtoll(buf, NULL, 10);
          if (val == (~(my_ulonglong)0 >> 1))
          {
            val= strtoull(buf, NULL, 10);
            buffer_is_unsigned = TRUE;
          }

          imp_sth->fbind[idx].numeric_val.llval= val;
          buffer= (void*)&(imp_sth->fbind[idx].numeric_val.llval);

          if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          {
            PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                          "   SCALAR sql_type %" IVdf " ->%s<- IS %s LONGLONG INT NUMBER\n",
                          sql_type, buf, buffer_is_unsigned ? "AN UNSIGNED" : "A SIGNED");
          }
        }
        break;
#endif

      case MYSQL_TYPE_FLOAT:
        if (!SvNOK(value) && DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND A FLOAT NUMBER\n");
        buffer_length = sizeof(imp_sth->fbind[idx].numeric_val.fval);
        imp_sth->fbind[idx].numeric_val.fval= SvNV_nomg(value);
        buffer=(char*)&(imp_sth->fbind[idx].numeric_val.fval);
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "   SCALAR sql_type %"IVdf" ->%f<- IS A FLOAT NUMBER\n",
                        sql_type, *(float *)buffer);
        break;

      case MYSQL_TYPE_DOUBLE:
        if (!SvNOK(value) && DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh), "\t\tTRY TO BIND A DOUBLE NUMBER\n");
        buffer_length = sizeof(imp_sth->fbind[idx].numeric_val.dval);
#if NVSIZE >= 8
        imp_sth->fbind[idx].numeric_val.dval= SvNV_nomg(value);
#else
        imp_sth->fbind[idx].numeric_val.dval= atof(SvPV_nomg_nolen(value));
#endif
        buffer=(char*)&(imp_sth->fbind[idx].numeric_val.dval);
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "   SCALAR sql_type %"IVdf" ->%f<- IS A DOUBLE NUMBER\n",
                        sql_type, *(double *)buffer);
        break;

      /* TODO: datetime structures */
#if 0
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        break;
#endif

      case MYSQL_TYPE_BLOB:
        buffer= imp_sth->params[idx].value;
        buffer_length= imp_sth->params[idx].len;
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "   SCALAR sql_type %" IVdf " ->length %lu<- IS A BLOB\n", sql_type, buffer_length);
        break;

      default:
        buffer_type= MYSQL_TYPE_STRING;
        buffer= imp_sth->params[idx].value;
        buffer_length= imp_sth->params[idx].len;
        if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
          PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                        "   SCALAR sql_type %"IVdf" ->%.1000s%s<- IS A STRING\n", sql_type, buffer, buffer_length > 1000 ? "..." : "");
        break;
      }
    }
    else
    {
      buffer= NULL;
      buffer_type= MYSQL_TYPE_NULL;
      buffer_length= 0;
      if (DBIc_TRACE_LEVEL(imp_xxh) >= 2)
        PerlIO_printf(DBIc_LOGPIO(imp_xxh),
                      "   SCALAR sql_type %"IVdf" IS A NULL VALUE", sql_type);
    }

    /* Column was changed, force rebind */
    imp_sth->has_been_bound = FALSE;

    imp_sth->bind[idx].buffer_type= buffer_type;
    imp_sth->bind[idx].buffer= buffer;
    imp_sth->bind[idx].buffer_length= buffer_length;
    imp_sth->bind[idx].is_unsigned= buffer_is_unsigned;

    imp_sth->fbind[idx].length= buffer_length;
    imp_sth->fbind[idx].is_null= buffer_is_null;
  }

  return 1;
}


/***************************************************************************
 *
 *  Name:    mariadb_db_reconnect
 *
 *  Purpose: If the server has disconnected, try to reconnect.
 *
 *  Input:   h - database or statement handle
 *           stmt - MYSQL statement pointer (or NULL)
 *
 *  Returns: TRUE for success, FALSE otherwise
 *
 **************************************************************************/

bool mariadb_db_reconnect(SV *h, MYSQL_STMT *stmt)
{
  dTHX;
  SV *dbh;
  D_imp_xxh(h);
  imp_dbh_t* imp_dbh;

  if (DBIc_TYPE(imp_xxh) == DBIt_ST)
  {
    dbh = DBIc_PARENT_H(imp_xxh);
    imp_dbh = (imp_dbh_t*) DBIc_PARENT_COM(imp_xxh);
  }
  else
  {
    dbh = h;
    imp_dbh= (imp_dbh_t*) imp_xxh;
  }

  if (imp_dbh->pmysql &&
      mysql_errno(imp_dbh->pmysql) != CR_SERVER_GONE_ERROR &&
      mysql_errno(imp_dbh->pmysql) != CR_SERVER_LOST &&
      (!stmt || (mysql_stmt_errno(stmt) != CR_SERVER_GONE_ERROR &&
                 mysql_stmt_errno(stmt) != CR_SERVER_LOST &&
                 mysql_stmt_errno(stmt) != CR_STMT_CLOSED)))
  {
    /* Other error */
    return FALSE;
  }

  if (!DBIc_has(imp_dbh, DBIcf_AutoCommit) || !imp_dbh->auto_reconnect)
  {
    /* We never reconnect if AutoCommit is turned off.
     * Otherwise we might get an inconsistent transaction
     * state.
     */
    return FALSE;
  }

  /* we should disconnect the db handle before reconnecting, this will
   * prevent mariadb_db_my_login from thinking it's adopting an active child which
   * would prevent the handle from actually reconnecting
   */
  mariadb_db_disconnect(dbh, imp_dbh);
  if (!mariadb_db_my_login(aTHX_ dbh, imp_dbh))
  {
    ++imp_dbh->stats.auto_reconnects_failed;
    return FALSE;
  }

  /*
   *  Tell DBI, that dbh->disconnect should be called for this handle
   */
  DBIc_ACTIVE_on(imp_dbh);

  ++imp_dbh->stats.auto_reconnects_ok;
  return TRUE;
}


/**************************************************************************
 *
 *  Name:    mariadb_db_type_info_all
 *
 *  Purpose: Implements $dbh->type_info_all
 *
 *  Returns: RV to AV of types
 *
 **************************************************************************/

#define PV_PUSH(c)                              \
    if (c) {                                    \
	sv= newSVpv((char*) (c), 0);           \
	SvREADONLY_on(sv);                      \
    } else {                                    \
        sv= &PL_sv_undef;                         \
    }                                           \
    av_push(row, sv);

#define IV_PUSH(i) sv= newSViv((i)); SvREADONLY_on(sv); av_push(row, sv);

AV *mariadb_db_type_info_all(void)
{
  dTHX;
  AV *av= newAV();
  AV *row;
  HV *hv;
  SV *sv;
  size_t i;
  const char *cols[] = {
    "TYPE_NAME",
    "DATA_TYPE",
    "COLUMN_SIZE",
    "LITERAL_PREFIX",
    "LITERAL_SUFFIX",
    "CREATE_PARAMS",
    "NULLABLE",
    "CASE_SENSITIVE",
    "SEARCHABLE",
    "UNSIGNED_ATTRIBUTE",
    "FIXED_PREC_SCALE",
    "AUTO_UNIQUE_VALUE",
    "LOCAL_TYPE_NAME",
    "MINIMUM_SCALE",
    "MAXIMUM_SCALE",
    "NUM_PREC_RADIX",
    "SQL_DATATYPE",
    "SQL_DATETIME_SUB",
    "INTERVAL_PRECISION",
    "mariadb_native_type",
    "mariadb_is_num"
  };
 
  hv= newHV();
  av_push(av, newRV_noinc((SV*) hv));
  for (i = 0; i < sizeof(cols) / sizeof(const char*); i++)
  {
    if (!hv_store(hv, (char*) cols[i], strlen(cols[i]), newSVuv(i), 0))
    {
      SvREFCNT_dec((SV*) av);
      return Nullav;
    }
  }
  for (i = 0; i < SQL_GET_TYPE_INFO_num; i++)
  {
    const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];

    row= newAV();
    av_push(av, newRV_noinc((SV*) row));
    PV_PUSH(t->type_name);
    IV_PUSH(t->data_type);
    IV_PUSH(t->column_size);
    PV_PUSH(t->literal_prefix);
    PV_PUSH(t->literal_suffix);
    PV_PUSH(t->create_params);
    IV_PUSH(t->nullable);
    IV_PUSH(t->case_sensitive);
    IV_PUSH(t->searchable);
    IV_PUSH(t->unsigned_attribute);
    IV_PUSH(t->fixed_prec_scale);
    IV_PUSH(t->auto_unique_value);
    PV_PUSH(t->local_type_name);
    IV_PUSH(t->minimum_scale);
    IV_PUSH(t->maximum_scale);

    if (t->num_prec_radix)
    {
      IV_PUSH(t->num_prec_radix);
    }
    else
      av_push(row, &PL_sv_undef);

    IV_PUSH(t->sql_datatype); /* SQL_DATATYPE*/
    IV_PUSH(t->sql_datetime_sub); /* SQL_DATETIME_SUB*/
    IV_PUSH(t->interval_precision); /* INTERVAL_PRECISION */
    IV_PUSH(t->native_type);
    av_push(row, boolSV(t->is_num));
  }
  return av;
}

/*
  mariadb_db_quote

  Properly quotes a value 
*/
SV* mariadb_db_quote(SV *dbh, SV *str, SV *type)
{
  dTHX;
  SV *result;

  if (SvGMAGICAL(str))
    mg_get(str);

  if (!SvOK(str))
    result= newSVpvs("NULL");
  else
  {
    char *ptr, *sptr;
    STRLEN len;
    bool is_binary = FALSE;

    D_imp_dbh(dbh);

    if (type && SvGMAGICAL(type))
      mg_get(type);

    if (type  &&  SvOK(type))
    {
      size_t i;
      IV tp = SvIV_nomg(type);
      is_binary = sql_type_is_binary(tp);
      for (i = 0; i < SQL_GET_TYPE_INFO_num; i++)
      {
        const sql_type_info_t *t= &SQL_GET_TYPE_INFO_values[i];
        if (t->data_type == tp)
        {
          if (!t->literal_prefix)
            return Nullsv;
          break;
        }
      }
    }

    if (is_binary)
    {
      ptr = SvPVbyte_nomg(str, len);
      result = newSV(len*2+4);
      sptr = SvPVX(result);

      *sptr++ = 'X';
      *sptr++ = '\'';
      sptr += mysql_hex_string(sptr, ptr, len);
      *sptr++ = '\'';
    }
    else
    {
      if (!imp_dbh->pmysql && !mariadb_db_reconnect(dbh, NULL))
      {
        mariadb_dr_do_error(dbh, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
        return Nullsv;
      }

      ptr = SvPVutf8_nomg(str, len);
      result = newSV(len*2+4);
      sptr = SvPVX(result);

#if MYSQL_VERSION_ID < 50001
      if (imp_dbh->pmysql->server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES)
      {
        *sptr++ = 'X';
        *sptr++ = '\'';
        sptr += mysql_hex_string(sptr, ptr, len);
        *sptr++ = '\'';
      }
      else
#endif
      {
      *sptr++ = '\'';
#if !defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50706 && MYSQL_VERSION_ID != 60000
      sptr += mysql_real_escape_string_quote(imp_dbh->pmysql, sptr, ptr, len, '\'');
#else
      sptr += mysql_real_escape_string(imp_dbh->pmysql, sptr, ptr, len);
#endif
      *sptr++ = '\'';
      }
    }

    SvPOK_on(result);
    SvCUR_set(result, sptr - SvPVX(result));
    /* Never hurts NUL terminating a Per string */
    *sptr++= '\0';

    if (!is_binary)
      sv_utf8_decode(result);
  }
  return result;
}

SV *mariadb_db_last_insert_id(SV *dbh, imp_dbh_t *imp_dbh,
        SV *catalog, SV *schema, SV *table, SV *field, SV *attr)
{
  dTHX;
  PERL_UNUSED_ARG(dbh);
  PERL_UNUSED_ARG(catalog);
  PERL_UNUSED_ARG(schema);
  PERL_UNUSED_ARG(table);
  PERL_UNUSED_ARG(field);
  PERL_UNUSED_ARG(attr);

  if (!imp_dbh->pmysql && !mariadb_db_reconnect(dbh, NULL))
  {
    mariadb_dr_do_error(dbh, CR_SERVER_GONE_ERROR, "MySQL server has gone away", "HY000");
    return Nullsv;
  }

  return sv_2mortal(my_ulonglong2sv(mysql_insert_id(imp_dbh->pmysql)));
}

my_ulonglong mariadb_db_async_result(SV* h, MYSQL_RES** resp)
{
  dTHX;
  D_imp_xxh(h);
  imp_dbh_t* dbh;
  MYSQL* svsock = NULL;
  MYSQL_RES* _res;
  my_ulonglong retval = 0;
  unsigned int num_fields;
  int htype;
  bool async_sth = FALSE;

  if(! resp) {
      resp = &_res;
  }
  htype = DBIc_TYPE(imp_xxh);


  if(htype == DBIt_DB) {
      D_imp_dbh(h);
      dbh = imp_dbh;
  } else {
      D_imp_sth(h);
      D_imp_dbh_from_sth;
      dbh = imp_dbh;
      async_sth = imp_sth->is_async;
      retval = imp_sth->row_num;
  }

  if(! dbh->async_query_in_flight) {
      if (async_sth)
          return retval;
      mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Gathering asynchronous results for a synchronous handle", "HY000");
      return -1;
  }
  if(dbh->async_query_in_flight != imp_xxh) {
      mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Gathering async_query_in_flight results for the wrong handle", "HY000");
      return -1;
  }
  dbh->async_query_in_flight = NULL;

  svsock= dbh->pmysql;
  if (!svsock)
    return -1;
  if (!mysql_read_query_result(svsock))
  {
    *resp= mysql_store_result(svsock);

    if (mysql_errno(svsock))
    {
      mariadb_dr_do_error(h, mysql_errno(svsock), mysql_error(svsock), mysql_sqlstate(svsock));
      return -1;
    }
    if (!*resp)
      retval= mysql_affected_rows(svsock);
    else {
      retval= mysql_num_rows(*resp);
      if(resp == &_res) {
        mysql_free_result(*resp);
        *resp= NULL;
      }
    }
    if(htype == DBIt_ST) {
      D_imp_sth(h);
      D_imp_dbh_from_sth;

      if (retval != (my_ulonglong)-1) {
        if(! *resp) {
          imp_sth->insertid= mysql_insert_id(svsock);
          if(! mysql_more_results(svsock))
            DBIc_ACTIVE_off(imp_sth);
        } else {
          num_fields = mysql_num_fields(imp_sth->result);
          DBIc_NUM_FIELDS(imp_sth) = (num_fields <= INT_MAX) ? num_fields : INT_MAX;
          imp_sth->done_desc = FALSE;
          imp_sth->fetch_done = FALSE;
        }
      }
      imp_sth->warning_count = mysql_warning_count(imp_dbh->pmysql);
    }
  } else {
     mariadb_dr_do_error(h, mysql_errno(svsock), mysql_error(svsock),
              mysql_sqlstate(svsock));
     return (my_ulonglong)-1;
  }
 return retval;
}

int mariadb_db_async_ready(SV* h)
{
  dTHX;
  D_imp_xxh(h);
  imp_dbh_t* dbh;
  int htype;
  bool async_sth = FALSE;
  bool async_active = FALSE;

  htype = DBIc_TYPE(imp_xxh);
  
  if(htype == DBIt_DB) {
      D_imp_dbh(h);
      dbh = imp_dbh;
  } else {
      D_imp_sth(h);
      D_imp_dbh_from_sth;
      dbh = imp_dbh;
      async_sth = imp_sth->is_async;
      async_active = !!DBIc_ACTIVE(imp_sth);
  }

  if (!dbh->pmysql)
    return -1;

  if(dbh->async_query_in_flight) {
      if (dbh->async_query_in_flight == imp_xxh) {
          int retval = mariadb_dr_socket_ready(dbh->pmysql->net.fd);
          if(retval < 0) {
              mariadb_dr_do_error(h, -retval, strerror(-retval), "HY000");
          }
          return retval;
      } else {
          mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Calling mariadb_async_ready on the wrong handle", "HY000");
          return -1;
      }
  } else {
      if (async_sth) {
          if (async_active)
              return 1;
          mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Asynchronous handle was not executed yet", "HY000");
          return -1;
      }
      mariadb_dr_do_error(h, CR_UNKNOWN_ERROR, "Handle is not in asynchronous mode", "HY000");
      return -1;
  }
}

static bool is_mysql_number(char *string, STRLEN len)
{
    char *cp = string;
    bool number_found = FALSE;

    /* Skip leading MySQL utf8mb4 whitespaces */
    while (*cp == ' ' || (*cp >= 9 && *cp <= 13))
      cp++;

    /* Optional '+' or '-' */
    if (*cp == '-' || *cp == '+')
      cp++;

    /* Number before '.' */
    while (*cp >= '0' && *cp <= '9')
    {
      cp++;
      number_found = TRUE;
    }

    /* Optional '.' */
    if (*cp == '.')
    {
      cp++;

      /* Number after '.' */
      while (*cp >= '0' && *cp <= '9')
      {
        cp++;
        number_found = TRUE;
      }
    }

    /* No number found - error */
    if (!number_found)
        return FALSE;

    /* Optional exponent */
    if (*cp == 'e' || *cp == 'E')
    {
      cp++;
      /* Search for number also in exponent */
      number_found = FALSE;

      /* Optional '+' or '-' in exponent */
      if (*cp == '-' || *cp == '+')
        cp++;

      /* Exponent number */
      while (*cp >= '0' && *cp <= '9')
      {
        cp++;
        number_found = TRUE;
      }
    }

    /* Skip trailing MySQL utf8mb4 whitespaces */
    while (*cp == ' ' || (*cp >= 9 && *cp <= 13))
      cp++;

    /* Check that we processed all characters */
    if (string + len != cp)
        return FALSE;

    return number_found;
}
