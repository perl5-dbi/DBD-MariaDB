#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include <mysql.h>

static double mariadb_constant(char* name, char* arg) {
  errno = 0;
  arg= arg;
  switch (*name) {
  case 'B':
    if (strEQ(name, "BLOB_FLAG"))
      return BLOB_FLAG;
    break;
  case 'F':
    if (strnEQ(name, "FIELD_TYPE_", 11)) {
      char* n = name+11;
      switch(*n) {
      case 'B':
	if (strEQ(n, "BLOB"))
	  return MYSQL_TYPE_BLOB;
	break;
      case 'D':
	if (strEQ(n, "DECIMAL"))
	  return MYSQL_TYPE_DECIMAL;
	if (strEQ(n, "DATE"))
	  return MYSQL_TYPE_DATE;
	if (strEQ(n, "DATETIME"))
	  return MYSQL_TYPE_DATETIME;
	if (strEQ(n, "DOUBLE"))
	  return MYSQL_TYPE_DOUBLE;
	break;
      case 'F':
	if (strEQ(n, "FLOAT"))
	  return MYSQL_TYPE_FLOAT;
	break;
      case 'I':
	if (strEQ(n, "INT24"))
	  return MYSQL_TYPE_INT24;
	break;
      case 'L':
	if (strEQ(n, "LONGLONG"))
	  return MYSQL_TYPE_LONGLONG;
	if (strEQ(n, "LONG_BLOB"))
	  return MYSQL_TYPE_LONG_BLOB;
	if (strEQ(n, "LONG"))
	  return MYSQL_TYPE_LONG;
	break;
      case 'M':
	if (strEQ(n, "MEDIUM_BLOB"))
	  return MYSQL_TYPE_MEDIUM_BLOB;
	break;
      case 'N':
	if (strEQ(n, "NULL"))
	  return MYSQL_TYPE_NULL;
	break;
      case 'S':
	if (strEQ(n, "SHORT"))
	  return MYSQL_TYPE_SHORT;
	if (strEQ(n, "STRING"))
	  return MYSQL_TYPE_STRING;
	break;
      case 'T':
	if (strEQ(n, "TINY"))
	  return MYSQL_TYPE_TINY;
	if (strEQ(n, "TINY_BLOB"))
	  return MYSQL_TYPE_TINY_BLOB;
	if (strEQ(n, "TIMESTAMP"))
	  return MYSQL_TYPE_TIMESTAMP;
	if (strEQ(n, "TIME"))
	  return MYSQL_TYPE_TIME;
	break;
      case 'V':
	if (strEQ(n, "VAR_STRING"))
	  return MYSQL_TYPE_VAR_STRING;
	break;
      }
    }
    break;
  case 'N':
    if (strEQ(name, "NOT_NULL_FLAG"))
      return NOT_NULL_FLAG;
    break;
  case 'P':
    if (strEQ(name, "PRI_KEY_FLAG"))
      return PRI_KEY_FLAG;
    break;
  }
  errno = EINVAL;
  return 0;
}

