#!/usr/bin/perl

use strict;
use warnings;
require 5.008_001; # just as DBI

package DBD::MariaDB;

use DBI;
use DynaLoader();
our @ISA = qw(DynaLoader);

our $VERSION = '1.21';

bootstrap DBD::MariaDB $VERSION;


our $err = 0;	    # holds error code for DBI::err
our $errstr = "";	# holds error string for DBI::errstr
our $sqlstate = "";	# holds five character SQLSTATE code
our $drh = undef;	# holds driver handle once initialised

my $methods_are_installed = 0;
sub driver{
    return $drh if $drh;
    my($class, $attr) = @_;

    $class .= "::dr";

    # not a 'my' since we use it above to prevent multiple drivers
    $drh = DBI::_new_drh($class, { 'Name' => 'MariaDB',
				   'Version' => $VERSION,
				   'Err'    => \$DBD::MariaDB::err,
				   'Errstr' => \$DBD::MariaDB::errstr,
				   'State'  => \$DBD::MariaDB::sqlstate,
				   'Attribution' => "DBD::MariaDB $VERSION by Pali and others",
				 });

    if (!$methods_are_installed) {
	# for older DBI versions disable warning: method name prefix 'mariadb_' is not associated with a registered driver
	local $SIG{__WARN__} = sub {} unless eval { DBI->VERSION(1.640) };
	DBD::MariaDB::db->install_method('mariadb_sockfd');
	DBD::MariaDB::db->install_method('mariadb_async_result');
	DBD::MariaDB::db->install_method('mariadb_async_ready');
	DBD::MariaDB::st->install_method('mariadb_async_result');
	DBD::MariaDB::st->install_method('mariadb_async_ready');

        # for older DBI versions register our last_insert_id statement method
        if (not eval { DBI->VERSION(1.642) }) {
            # disable warning: method name prefix 'last_' is not associated with a registered driver
            local $SIG{__WARN__} = sub {};
            DBD::MariaDB::st->install_method('last_insert_id');
        }

	$methods_are_installed++;
    }

    $drh;
}

sub CLONE {
  undef $drh;
}

sub parse_dsn {
    my ($class, $dsn) = @_;
    my $hash = {};
    my($var, $val);
    if (!defined($dsn)) {
        return $hash;
    }
    while (length($dsn)) {
	if ($dsn =~ /([^:;]*\[.*]|[^:;]*)[:;](.*)/) {
	    $val = $1;
	    $dsn = $2;
	    $val =~ s/\[|]//g; # Remove [] if present, the rest of the code prefers plain IPv6 addresses
	} else {
	    $val = $dsn;
	    $dsn = '';
	}
	if ($val =~ /([^=]*)=(.*)/) {
	    $var = $1;
	    $val = $2;
	    if ($var eq 'hostname'  ||  $var eq 'host') {
		$hash->{'host'} = $val;
	    } elsif ($var eq 'db'  ||  $var eq 'dbname') {
		$hash->{'database'} = $val;
	    } else {
		$hash->{$var} = $val;
	    }
	} else {
	    foreach $var (qw(database host port)) {
		if (!defined($hash->{$var})) {
		    $hash->{$var} = $val;
		    last;
		}
	    }
	}
    }
    return $hash;
}


# ====== DRIVER ======
package # hide from PAUSE
    DBD::MariaDB::dr;

use strict;
use DBI qw(:sql_types);

sub connect {
    my($drh, $dsn, $username, $password, $attrhash) = @_;
    my($port);
    my($cWarn);
    my $connect_ref= { 'Name' => $dsn };
    $attrhash = {} unless defined $attrhash;

    # create a 'blank' dbh
    my $attr_dsn = DBD::MariaDB->parse_dsn($dsn);
    my($this, $privateAttrHash) = (undef, $attrhash);
    $privateAttrHash = {
	%$attr_dsn,
	%$privateAttrHash,
	'Name' => $dsn,
	'user' => $username,
	'password' => $password
    };

    if (exists $attrhash->{dbi_imp_data}) {
      $connect_ref->{'dbi_imp_data'} = $attrhash->{dbi_imp_data};
    }

    if (!defined($this = DBI::_new_dbh($drh,
            $connect_ref,
            $privateAttrHash)))
    {
      return undef;
    }

    DBD::MariaDB::db::_login($this, $dsn, $username, $password)
	  or $this = undef;

    $this;
}

sub data_sources {
    my ($self, $attributes) = @_;

    my ($host, $port, $username, $password);
    if (defined $attributes)
    {
        %{$attributes} = %{$attributes};
        $host = delete $attributes->{host};
        $port = delete $attributes->{port};
        $username = delete $attributes->{user};
        $password = delete $attributes->{password};
    }

    my $dsn = '';
    $dsn .= ";host=$host" if defined $host;
    $dsn .= ";port=$port" if defined $port;

    my $dbh = $self->connect($dsn, $username, $password, $attributes);
    return unless defined $dbh;

    return $dbh->data_sources();
}


# ====== DATABASE ======
package # hide from PAUSE
    DBD::MariaDB::db;

use strict;
use DBI qw(:sql_types);

sub prepare {
    my($dbh, $statement, $attribs)= @_;

    return unless $dbh->func('_async_check');

    # create a 'blank' dbh
    my $sth = DBI::_new_sth($dbh, {'Statement' => $statement});

    # Populate internal handle data.
    if (!DBD::MariaDB::st::_prepare($sth, $statement, $attribs)) {
	$sth = undef;
    }

    $sth;
}

sub table_info {
  my ($dbh, $catalog, $schema, $table, $type, $attr) = @_;

  return unless $dbh->func('_async_check');

  local $dbh->{mariadb_server_prepare} = 0;
  my @names = qw(TABLE_CAT TABLE_SCHEM TABLE_NAME TABLE_TYPE REMARKS);
  my @rows;

  my $sponge = DBI->connect("DBI:Sponge:", '','')
    or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");

# Return the list of catalogs
  if (defined $catalog && $catalog eq "%" &&
      (!defined($schema) || $schema eq "") &&
      (!defined($table) || $table eq ""))
  {
    @rows = (); # Empty, because MySQL doesn't support catalogs (yet)
  }
  # Return the list of schemas
  elsif (defined $schema && $schema eq "%" &&
      (!defined($catalog) || $catalog eq "") &&
      (!defined($table) || $table eq ""))
  {
    my $sth = $dbh->prepare("SHOW DATABASES")
      or return undef;

    $sth->execute()
      or return DBI::set_err($dbh, $sth->err(), $sth->errstr());

    while (my $ref = $sth->fetchrow_arrayref())
    {
      push(@rows, [ undef, $ref->[0], undef, undef, undef ]);
    }
  }
  # Return the list of table types
  elsif (defined $type && $type eq "%" &&
      (!defined($catalog) || $catalog eq "") &&
      (!defined($schema) || $schema eq "") &&
      (!defined($table) || $table eq ""))
  {
    @rows = (
        [ undef, undef, undef, "TABLE", undef ],
        [ undef, undef, undef, "VIEW",  undef ],
        );
  }
  # Special case: a catalog other than undef, "", or "%"
  elsif (defined $catalog && $catalog ne "" && $catalog ne "%")
  {
    @rows = (); # Nothing, because MySQL doesn't support catalogs yet.
  }
  # Uh oh, we actually have a meaty table_info call. Work is required!
  else
  {
    my @schemas;
    # If no table was specified, we want them all
    $table ||= "%";

    # If something was given for the schema, we need to expand it to
    # a list of schemas, since it may be a wildcard.
    if (defined $schema && $schema ne "")
    {
      my $sth = $dbh->prepare("SHOW DATABASES LIKE " .
          $dbh->quote($schema))
        or return undef;
      $sth->execute()
        or return DBI::set_err($dbh, $sth->err(), $sth->errstr());

      while (my $ref = $sth->fetchrow_arrayref())
      {
        push @schemas, $ref->[0];
      }
    }
    # Otherwise we want the current database
    else
    {
      push @schemas, $dbh->selectrow_array("SELECT DATABASE()");
    }

    # Figure out which table types are desired
    my ($want_tables, $want_views);
    if (defined $type && $type ne "")
    {
      $want_tables = ($type =~ m/table/i);
      $want_views  = ($type =~ m/view/i);
    }
    else
    {
      $want_tables = $want_views = 1;
    }

    for my $database (@schemas)
    {
      my $sth = $dbh->prepare("SHOW /*!50002 FULL*/ TABLES FROM " .
          $dbh->quote_identifier($database) .
          " LIKE " .  $dbh->quote($table))
          or return undef;

      $sth->execute()
          or return DBI::set_err($dbh, $sth->err(), $sth->errstr());

      while (my $ref = $sth->fetchrow_arrayref())
      {
        my $type = (defined $ref->[1] &&
            $ref->[1] =~ /view/i) ? 'VIEW' : 'TABLE';
        next if $type eq 'TABLE' && not $want_tables;
        next if $type eq 'VIEW'  && not $want_views;
        push @rows, [ undef, $database, $ref->[0], $type, undef ];
      }
    }
  }

  my $sth = $sponge->prepare("table_info",
  {
    rows          => \@rows,
    NUM_OF_FIELDS => scalar @names,
    NAME          => \@names,
  })
    or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

  return $sth;
}

sub column_info {
  my ($dbh, $catalog, $schema, $table, $column) = @_;

  return unless $dbh->func('_async_check');

  local $dbh->{mariadb_server_prepare} = 0;

  # ODBC allows a NULL to mean all columns, so we'll accept undef
  $column = '%' unless defined $column;

  my $ER_NO_SUCH_TABLE= 1146;
  my $ER_BAD_FIELD_ERROR = 1054;

  my $table_id = $dbh->quote_identifier($catalog, $schema, $table);

  my @names = qw(
      TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME
      DATA_TYPE TYPE_NAME COLUMN_SIZE BUFFER_LENGTH DECIMAL_DIGITS
      NUM_PREC_RADIX NULLABLE REMARKS COLUMN_DEF
      SQL_DATA_TYPE SQL_DATETIME_SUB CHAR_OCTET_LENGTH
      ORDINAL_POSITION IS_NULLABLE CHAR_SET_CAT
      CHAR_SET_SCHEM CHAR_SET_NAME COLLATION_CAT COLLATION_SCHEM COLLATION_NAME
      UDT_CAT UDT_SCHEM UDT_NAME DOMAIN_CAT DOMAIN_SCHEM DOMAIN_NAME
      SCOPE_CAT SCOPE_SCHEM SCOPE_NAME MAX_CARDINALITY
      DTD_IDENTIFIER IS_SELF_REF
      mariadb_is_pri_key mariadb_type_name mariadb_values
      mariadb_is_auto_increment
      );
  my %col_info;

  local $dbh->{FetchHashKeyName} = 'NAME_lc';
  # only ignore ER_NO_SUCH_TABLE in internal_execute if issued from here
  my $desc_sth = $dbh->prepare("DESCRIBE $table_id " . $dbh->quote($column));
  my $desc = $dbh->selectall_arrayref($desc_sth, { Columns=>{} });

  #return $desc_sth if $desc_sth->err();
  if (my $err = $desc_sth->err())
  {
    # return the error, unless it is due to the table not
    # existing per DBI spec
    if ($err != $ER_NO_SUCH_TABLE)
    {
      return undef;
    }
    $dbh->set_err(undef,undef);
    $desc = [];
  }

  my $ordinal_pos = 0;
  my @fields;
  for my $row (@$desc)
  {
    my $type = $row->{type};
    $type =~ m/^(\w+)(\((.+)\))?\s?(.*)?$/;
    my $basetype  = lc($1);
    my $typemod   = $3;
    my $attr      = $4;

    push @fields, $row->{field};
    my $info = $col_info{ $row->{field} }= {
	    TABLE_CAT               => $catalog,
	    TABLE_SCHEM             => $schema,
	    TABLE_NAME              => $table,
	    COLUMN_NAME             => $row->{field},
	    NULLABLE                => ($row->{null} eq 'YES') ? 1 : 0,
	    IS_NULLABLE             => ($row->{null} eq 'YES') ? "YES" : "NO",
	    TYPE_NAME               => uc($basetype),
	    COLUMN_DEF              => $row->{default},
	    ORDINAL_POSITION        => ++$ordinal_pos,
	    mariadb_is_pri_key      => ($row->{key}  eq 'PRI'),
	    mariadb_type_name       => $row->{type},
	    mariadb_is_auto_increment => ($row->{extra} =~ /auto_increment/i ? 1 : 0),
    };
    #
	  # This code won't deal with a pathological case where a value
	  # contains a single quote followed by a comma, and doesn't unescape
	  # any escaped values. But who would use those in an enum or set?
    #
	  my @type_params= ($typemod && index($typemod,"'")>=0) ?
      ("$typemod," =~ /'(.*?)',/g)  # assume all are quoted
			: split /,/, $typemod||'';      # no quotes, plain list
	  s/''/'/g for @type_params;                # undo doubling of quotes

	  my @type_attr= split / /, $attr||'';

  	$info->{DATA_TYPE}= SQL_VARCHAR();
    if ($basetype =~ /^(char|varchar|\w*text|\w*blob)/)
    {
      $info->{DATA_TYPE}= SQL_CHAR() if $basetype eq 'char';
      if ($type_params[0])
      {
        $info->{COLUMN_SIZE} = $type_params[0];
      }
      else
      {
        $info->{COLUMN_SIZE} = 65535;
        $info->{COLUMN_SIZE} = 255        if $basetype =~ /^tiny/;
        $info->{COLUMN_SIZE} = 16777215   if $basetype =~ /^medium/;
        $info->{COLUMN_SIZE} = 4294967295 if $basetype =~ /^long/;
      }
    }
	  elsif ($basetype =~ /^(binary|varbinary)/)
    {
      $info->{COLUMN_SIZE} = $type_params[0];
	    # SQL_BINARY & SQL_VARBINARY are tempting here but don't match the
	    # semantics for mysql (not hex). SQL_CHAR &  SQL_VARCHAR are correct here.
	    $info->{DATA_TYPE} = ($basetype eq 'binary') ? SQL_CHAR() : SQL_VARCHAR();
    }
    elsif ($basetype =~ /^(enum|set)/)
    {
	    if ($basetype eq 'set')
      {
		    $info->{COLUMN_SIZE} = length(join ",", @type_params);
	    }
	    else
      {
        my $max_len = 0;
        length($_) > $max_len and $max_len = length($_) for @type_params;
        $info->{COLUMN_SIZE} = $max_len;
	    }
	    $info->{"mariadb_values"} = \@type_params;
    }
    elsif ($basetype =~ /int/ || $basetype eq 'bit' )
    {
      # big/medium/small/tiny etc + unsigned?
	    $info->{DATA_TYPE} = SQL_INTEGER();
	    $info->{NUM_PREC_RADIX} = 10;
	    $info->{COLUMN_SIZE} = $type_params[0];
    }
    elsif ($basetype =~ /^decimal/)
    {
      $info->{DATA_TYPE} = SQL_DECIMAL();
      $info->{NUM_PREC_RADIX} = 10;
      $info->{COLUMN_SIZE}    = $type_params[0];
      $info->{DECIMAL_DIGITS} = $type_params[1];
    }
    elsif ($basetype =~ /^(float|double)/)
    {
	    $info->{DATA_TYPE} = ($basetype eq 'float') ? SQL_FLOAT() : SQL_DOUBLE();
	    $info->{NUM_PREC_RADIX} = 2;
	    $info->{COLUMN_SIZE} = ($basetype eq 'float') ? 32 : 64;
    }
    elsif ($basetype =~ /date|time/)
    {
      # date/datetime/time/timestamp
	    if ($basetype eq 'time' or $basetype eq 'date')
      {
		    #$info->{DATA_TYPE}   = ($basetype eq 'time') ? SQL_TYPE_TIME() : SQL_TYPE_DATE();
        $info->{DATA_TYPE}   = ($basetype eq 'time') ? SQL_TIME() : SQL_DATE();
        $info->{COLUMN_SIZE} = ($basetype eq 'time') ? 8 : 10;
      }
	    else
      {
        # datetime/timestamp
        #$info->{DATA_TYPE}     = SQL_TYPE_TIMESTAMP();
		    $info->{DATA_TYPE}        = SQL_TIMESTAMP();
		    $info->{SQL_DATA_TYPE}    = SQL_DATETIME();
        $info->{SQL_DATETIME_SUB} = $info->{DATA_TYPE} - ($info->{SQL_DATA_TYPE} * 10);
        $info->{COLUMN_SIZE}      = ($basetype eq 'datetime') ? 19 : $type_params[0] || 14;
	    }
	    $info->{DECIMAL_DIGITS}= 0; # no fractional seconds
    }
    elsif ($basetype eq 'year')
    {
      # no close standard so treat as int
	    $info->{DATA_TYPE}      = SQL_INTEGER();
	    $info->{NUM_PREC_RADIX} = 10;
	    $info->{COLUMN_SIZE}    = 4;
	  }
	  else
    {
        return $dbh->DBI::set_err($ER_BAD_FIELD_ERROR, "column_info: unrecognized column type '$basetype' of $table_id.$row->{field} treated as varchar");
    }
    $info->{SQL_DATA_TYPE} ||= $info->{DATA_TYPE};
  }

  my $sponge = DBI->connect("DBI:Sponge:", '','')
    or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");

  my $sth = $sponge->prepare("column_info $table", {
      rows          => [ map { [ @{$_}{@names} ] } map { $col_info{$_} } @fields ],
      NUM_OF_FIELDS => scalar @names,
      NAME          => \@names,
      })
      or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

  return $sth;
}


sub primary_key_info {
  my ($dbh, $catalog, $schema, $table) = @_;

  return unless $dbh->func('_async_check');

  local $dbh->{mariadb_server_prepare} = 0;

  my $table_id = $dbh->quote_identifier($catalog, $schema, $table);

  my @names = qw(
      TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME KEY_SEQ PK_NAME
      );
  my %col_info;

  local $dbh->{FetchHashKeyName} = 'NAME_lc';
  my $desc_sth = $dbh->prepare("SHOW KEYS FROM $table_id");
  my $desc= $dbh->selectall_arrayref($desc_sth, { Columns=>{} });
  my $ordinal_pos = 0;
  for my $row (grep { $_->{key_name} eq 'PRIMARY'} @$desc)
  {
    $col_info{ $row->{column_name} }= {
      TABLE_CAT   => $catalog,
      TABLE_SCHEM => $schema,
      TABLE_NAME  => $table,
      COLUMN_NAME => $row->{column_name},
      KEY_SEQ     => $row->{seq_in_index},
      PK_NAME     => $row->{key_name},
    };
  }

  my $sponge = DBI->connect("DBI:Sponge:", '','')
    or return $dbh->DBI::set_err($DBI::err, "DBI::Sponge: $DBI::errstr");

  my $sth= $sponge->prepare("primary_key_info $table", {
      rows          => [
        map { [ @{$_}{@names} ] }
        sort { $a->{KEY_SEQ} <=> $b->{KEY_SEQ} }
        values %col_info
      ],
      NUM_OF_FIELDS => scalar @names,
      NAME          => \@names,
      })
      or return $dbh->DBI::set_err($sponge->err(), $sponge->errstr());

  return $sth;
}


sub foreign_key_info {
    my ($dbh,
        $pk_catalog, $pk_schema, $pk_table,
        $fk_catalog, $fk_schema, $fk_table,
       ) = @_;

    return unless $dbh->func('_async_check');

    # INFORMATION_SCHEMA.KEY_COLUMN_USAGE was added in 5.0.6
    return if $dbh->FETCH('mariadb_serverversion') < 50006;

    my $sql = <<'EOF';
SELECT NULL AS PKTABLE_CAT,
       A.REFERENCED_TABLE_SCHEMA AS PKTABLE_SCHEM,
       A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,
       A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME,
       A.TABLE_CATALOG AS FKTABLE_CAT,
       A.TABLE_SCHEMA AS FKTABLE_SCHEM,
       A.TABLE_NAME AS FKTABLE_NAME,
       A.COLUMN_NAME AS FKCOLUMN_NAME,
       A.ORDINAL_POSITION AS KEY_SEQ,
       NULL AS UPDATE_RULE,
       NULL AS DELETE_RULE,
       A.CONSTRAINT_NAME AS FK_NAME,
       NULL AS PK_NAME,
       NULL AS DEFERABILITY,
       NULL AS UNIQUE_OR_PRIMARY
  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A,
       INFORMATION_SCHEMA.TABLE_CONSTRAINTS B
 WHERE A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME
   AND A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND B.CONSTRAINT_TYPE IS NOT NULL
EOF

    my @where;
    my @bind;

    # catalogs are not yet supported by MySQL

#    if (defined $pk_catalog) {
#        push @where, 'A.REFERENCED_TABLE_CATALOG = ?';
#        push @bind, $pk_catalog;
#    }

    if (defined $pk_schema) {
        push @where, 'A.REFERENCED_TABLE_SCHEMA = ?';
        push @bind, $pk_schema;
    }

    if (defined $pk_table) {
        push @where, 'A.REFERENCED_TABLE_NAME = ?';
        push @bind, $pk_table;
    }

#    if (defined $fk_catalog) {
#        push @where, 'A.TABLE_CATALOG = ?';
#        push @bind,  $fk_schema;
#    }

    if (defined $fk_schema) {
        push @where, 'A.TABLE_SCHEMA = ?';
        push @bind,  $fk_schema;
    }

    if (defined $fk_table) {
        push @where, 'A.TABLE_NAME = ?';
        push @bind,  $fk_table;
    }

    if (@where) {
        $sql .= ' AND ';
        $sql .= join ' AND ', @where;
    }
    $sql .= " ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, A.ORDINAL_POSITION";

    local $dbh->{FetchHashKeyName} = 'NAME_uc';
    my $sth = $dbh->prepare($sql);
    $sth->execute(@bind);

    return $sth;
}
# #86030: PATCH: adding statistics_info support
# Thank you to David Dick http://search.cpan.org/~ddick/
sub statistics_info {
    my ($dbh,
        $catalog, $schema, $table,
        $unique_only, $quick,
       ) = @_;

    return unless $dbh->func('_async_check');

    # INFORMATION_SCHEMA.KEY_COLUMN_USAGE was added in 5.0.6
    return if $dbh->FETCH('mariadb_serverversion') < 50006;

    my $sql = <<'EOF';
SELECT TABLE_CATALOG AS TABLE_CAT,
       TABLE_SCHEMA AS TABLE_SCHEM,
       TABLE_NAME AS TABLE_NAME,
       NON_UNIQUE AS NON_UNIQUE,
       NULL AS INDEX_QUALIFIER,
       INDEX_NAME AS INDEX_NAME,
       LCASE(INDEX_TYPE) AS TYPE,
       SEQ_IN_INDEX AS ORDINAL_POSITION,
       COLUMN_NAME AS COLUMN_NAME,
       COLLATION AS ASC_OR_DESC,
       CARDINALITY AS CARDINALITY,
       NULL AS PAGES,
       NULL AS FILTER_CONDITION
  FROM INFORMATION_SCHEMA.STATISTICS
EOF

    my @where;
    my @bind;

    # catalogs are not yet supported by MySQL

#    if (defined $catalog) {
#        push @where, 'TABLE_CATALOG = ?';
#        push @bind, $catalog;
#    }

    if (defined $schema) {
        push @where, 'TABLE_SCHEMA = ?';
        push @bind, $schema;
    }

    if (defined $table) {
        push @where, 'TABLE_NAME = ?';
        push @bind, $table;
    }

    if (@where) {
        $sql .= ' WHERE ';
        $sql .= join ' AND ', @where;
    }
    $sql .= " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";

    local $dbh->{FetchHashKeyName} = 'NAME_uc';
    my $sth = $dbh->prepare($sql);
    $sth->execute(@bind);

    return $sth;
}

####################
# get_info()

# SQL_DRIVER_VER should be formatted as ##.##.####
my $odbc_driver_ver = $DBD::MariaDB::VERSION;
$odbc_driver_ver .= '_00' if $odbc_driver_ver =~ /^\d+\.\d+$/;
$odbc_driver_ver = sprintf("%02u.%02u.%04u", split(/[\._]/, $odbc_driver_ver));

my @odbc_keywords = qw(
    BIGINT
    BLOB
    DEFAULT
    KEYS
    LIMIT
    LONGBLOB
    MEDIMUMBLOB
    MEDIUMINT
    MEDIUMTEXT
    PROCEDURE
    REGEXP
    RLIKE
    SHOW
    TABLES
    TINYBLOB
    TINYTEXT
    UNIQUE
    UNSIGNED
    ZEROFILL
);

my %odbc_info_constants = (
     20 => 'N',                                # SQL_ACCESSIBLE_PROCEDURES
     19 => 'Y',                                # SQL_ACCESSIBLE_TABLES
    116 => 0,                                  # SQL_ACTIVE_ENVIRONMENTS
    169 => 127,                                # SQL_AGGREGATE_FUNCTIONS
    117 => 0,                                  # SQL_ALTER_DOMAIN
     86 => 3,                                  # SQL_ALTER_TABLE
  10021 => 2,                                  # SQL_ASYNC_MODE
    120 => 2,                                  # SQL_BATCH_ROW_COUNT
    121 => 2,                                  # SQL_BATCH_SUPPORT
     82 => 0,                                  # SQL_BOOKMARK_PERSISTENCE
    114 => 1,                                  # SQL_CATALOG_LOCATION
  10003 => 'Y',                                # SQL_CATALOG_NAME
     41 => '.',                                # SQL_CATALOG_NAME_SEPARATOR
     42 => 'database',                         # SQL_CATALOG_TERM
     92 => 29,                                 # SQL_CATALOG_USAGE
  10004 => '',                                 # SQL_COLLATION_SEQ
     87 => 'Y',                                # SQL_COLUMN_ALIAS
     22 => 0,                                  # SQL_CONCAT_NULL_BEHAVIOR
     53 => 259071,                             # SQL_CONVERT_BIGINT
     54 => 0,                                  # SQL_CONVERT_BINARY
     55 => 259071,                             # SQL_CONVERT_BIT
     56 => 259071,                             # SQL_CONVERT_CHAR
     57 => 259071,                             # SQL_CONVERT_DATE
     58 => 259071,                             # SQL_CONVERT_DECIMAL
     59 => 259071,                             # SQL_CONVERT_DOUBLE
     60 => 259071,                             # SQL_CONVERT_FLOAT
     48 => 0,                                  # SQL_CONVERT_FUNCTIONS
#   173 => undef,                              # SQL_CONVERT_GUID
     61 => 259071,                             # SQL_CONVERT_INTEGER
    123 => 0,                                  # SQL_CONVERT_INTERVAL_DAY_TIME
    124 => 0,                                  # SQL_CONVERT_INTERVAL_YEAR_MONTH
     71 => 0,                                  # SQL_CONVERT_LONGVARBINARY
     62 => 259071,                             # SQL_CONVERT_LONGVARCHAR
     63 => 259071,                             # SQL_CONVERT_NUMERIC
     64 => 259071,                             # SQL_CONVERT_REAL
     65 => 259071,                             # SQL_CONVERT_SMALLINT
     66 => 259071,                             # SQL_CONVERT_TIME
     67 => 259071,                             # SQL_CONVERT_TIMESTAMP
     68 => 259071,                             # SQL_CONVERT_TINYINT
     69 => 0,                                  # SQL_CONVERT_VARBINARY
     70 => 259071,                             # SQL_CONVERT_VARCHAR
    122 => 0,                                  # SQL_CONVERT_WCHAR
    125 => 0,                                  # SQL_CONVERT_WLONGVARCHAR
    126 => 0,                                  # SQL_CONVERT_WVARCHAR
     74 => 1,                                  # SQL_CORRELATION_NAME
    127 => 0,                                  # SQL_CREATE_ASSERTION
    128 => 0,                                  # SQL_CREATE_CHARACTER_SET
    129 => 0,                                  # SQL_CREATE_COLLATION
    130 => 0,                                  # SQL_CREATE_DOMAIN
    131 => 0,                                  # SQL_CREATE_SCHEMA
    132 => 1045,                               # SQL_CREATE_TABLE
    133 => 0,                                  # SQL_CREATE_TRANSLATION
    134 => 0,                                  # SQL_CREATE_VIEW
     23 => 2,                                  # SQL_CURSOR_COMMIT_BEHAVIOR
     24 => 2,                                  # SQL_CURSOR_ROLLBACK_BEHAVIOR
  10001 => 0,                                  # SQL_CURSOR_SENSITIVITY
     25 => 'N',                                # SQL_DATA_SOURCE_READ_ONLY
    119 => 7,                                  # SQL_DATETIME_LITERALS
    170 => 3,                                  # SQL_DDL_INDEX
     26 => 2,                                  # SQL_DEFAULT_TXN_ISOLATION
  10002 => 'N',                                # SQL_DESCRIBE_PARAMETER
#   171 => undef,                              # SQL_DM_VER
      3 => 137076632,                          # SQL_DRIVER_HDBC
#   135 => undef,                              # SQL_DRIVER_HDESC
      4 => 137076088,                          # SQL_DRIVER_HENV
#    76 => undef,                              # SQL_DRIVER_HLIB
#     5 => undef,                              # SQL_DRIVER_HSTMT
      6 => 'DBD/MariaDB.pm',                   # SQL_DRIVER_NAME
     77 => '03.51',                            # SQL_DRIVER_ODBC_VER
      7 => $odbc_driver_ver,                   # SQL_DRIVER_VER
    136 => 0,                                  # SQL_DROP_ASSERTION
    137 => 0,                                  # SQL_DROP_CHARACTER_SET
    138 => 0,                                  # SQL_DROP_COLLATION
    139 => 0,                                  # SQL_DROP_DOMAIN
    140 => 0,                                  # SQL_DROP_SCHEMA
    141 => 7,                                  # SQL_DROP_TABLE
    142 => 0,                                  # SQL_DROP_TRANSLATION
    143 => 0,                                  # SQL_DROP_VIEW
    144 => 0,                                  # SQL_DYNAMIC_CURSOR_ATTRIBUTES1
    145 => 0,                                  # SQL_DYNAMIC_CURSOR_ATTRIBUTES2
     27 => 'Y',                                # SQL_EXPRESSIONS_IN_ORDERBY
      8 => 63,                                 # SQL_FETCH_DIRECTION
     84 => 0,                                  # SQL_FILE_USAGE
    146 => 97863,                              # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1
    147 => 6016,                               # SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2
     81 => 11,                                 # SQL_GETDATA_EXTENSIONS
     88 => 3,                                  # SQL_GROUP_BY
     28 => 4,                                  # SQL_IDENTIFIER_CASE
     29 => '`',                                # SQL_IDENTIFIER_QUOTE_CHAR
    148 => 0,                                  # SQL_INDEX_KEYWORDS
    149 => 0,                                  # SQL_INFO_SCHEMA_VIEWS
    172 => 7,                                  # SQL_INSERT_STATEMENT
     73 => 'N',                                # SQL_INTEGRITY
    150 => 0,                                  # SQL_KEYSET_CURSOR_ATTRIBUTES1
    151 => 0,                                  # SQL_KEYSET_CURSOR_ATTRIBUTES2
     89 => (join ',', @odbc_keywords),         # SQL_KEYWORDS
    113 => 'Y',                                # SQL_LIKE_ESCAPE_CLAUSE
     78 => 0,                                  # SQL_LOCK_TYPES
# 20000 => undef,                              # SQL_MAXIMUM_STMT_OCTETS
# 20001 => undef,                              # SQL_MAXIMUM_STMT_OCTETS_DATA
# 20002 => undef,                              # SQL_MAXIMUM_STMT_OCTETS_SCHEMA
  10022 => 1,                                  # SQL_MAX_ASYNC_CONCURRENT_STATEMENTS
    112 => 0,                                  # SQL_MAX_BINARY_LITERAL_LEN
     34 => 64,                                 # SQL_MAX_CATALOG_NAME_LEN
    108 => 0,                                  # SQL_MAX_CHAR_LITERAL_LEN
     97 => 0,                                  # SQL_MAX_COLUMNS_IN_GROUP_BY
     98 => 32,                                 # SQL_MAX_COLUMNS_IN_INDEX
     99 => 0,                                  # SQL_MAX_COLUMNS_IN_ORDER_BY
    100 => 0,                                  # SQL_MAX_COLUMNS_IN_SELECT
    101 => 0,                                  # SQL_MAX_COLUMNS_IN_TABLE
     30 => 64,                                 # SQL_MAX_COLUMN_NAME_LEN
      1 => 0,                                  # SQL_MAX_CONCURRENT_ACTIVITIES
     31 => 18,                                 # SQL_MAX_CURSOR_NAME_LEN
      0 => 0,                                  # SQL_MAX_DRIVER_CONNECTIONS
  10005 => 64,                                 # SQL_MAX_IDENTIFIER_LEN
    102 => 500,                                # SQL_MAX_INDEX_SIZE
     33 => 0,                                  # SQL_MAX_PROCEDURE_NAME_LEN
    104 => 0,                                  # SQL_MAX_ROW_SIZE
    103 => 'Y',                                # SQL_MAX_ROW_SIZE_INCLUDES_LONG
     32 => 0,                                  # SQL_MAX_SCHEMA_NAME_LEN
     35 => 64,                                 # SQL_MAX_TABLE_NAME_LEN
    107 => 16,                                 # SQL_MAX_USER_NAME_LEN
     37 => 'Y',                                # SQL_MULTIPLE_ACTIVE_TXN
     36 => 'Y',                                # SQL_MULT_RESULT_SETS
    111 => 'N',                                # SQL_NEED_LONG_DATA_LEN
     75 => 1,                                  # SQL_NON_NULLABLE_COLUMNS
     85 => 2,                                  # SQL_NULL_COLLATION
     49 => 16777215,                           # SQL_NUMERIC_FUNCTIONS
      9 => 1,                                  # SQL_ODBC_API_CONFORMANCE
    152 => 2,                                  # SQL_ODBC_INTERFACE_CONFORMANCE
     12 => 1,                                  # SQL_ODBC_SAG_CLI_CONFORMANCE
     15 => 1,                                  # SQL_ODBC_SQL_CONFORMANCE
     10 => '03.80',                            # SQL_ODBC_VER
    115 => 123,                                # SQL_OJ_CAPABILITIES
     90 => 'Y',                                # SQL_ORDER_BY_COLUMNS_IN_SELECT
     38 => 'Y',                                # SQL_OUTER_JOINS
    153 => 2,                                  # SQL_PARAM_ARRAY_ROW_COUNTS
    154 => 3,                                  # SQL_PARAM_ARRAY_SELECTS
     80 => 3,                                  # SQL_POSITIONED_STATEMENTS
     79 => 31,                                 # SQL_POS_OPERATIONS
     21 => 'N',                                # SQL_PROCEDURES
     40 => '',                                 # SQL_PROCEDURE_TERM
     93 => 3,                                  # SQL_QUOTED_IDENTIFIER_CASE
     11 => 'N',                                # SQL_ROW_UPDATES
     39 => '',                                 # SQL_SCHEMA_TERM
     91 => 0,                                  # SQL_SCHEMA_USAGE
     43 => 7,                                  # SQL_SCROLL_CONCURRENCY
     44 => 17,                                 # SQL_SCROLL_OPTIONS
     14 => '\\',                               # SQL_SEARCH_PATTERN_ESCAPE
     94 => ' !"#%&\'()*+,-.:;<=>?@[\]^`{|}~',  # SQL_SPECIAL_CHARACTERS
    155 => 7,                                  # SQL_SQL92_DATETIME_FUNCTIONS
    156 => 0,                                  # SQL_SQL92_FOREIGN_KEY_DELETE_RULE
    157 => 0,                                  # SQL_SQL92_FOREIGN_KEY_UPDATE_RULE
    158 => 8160,                               # SQL_SQL92_GRANT
    159 => 0,                                  # SQL_SQL92_NUMERIC_VALUE_FUNCTIONS
    160 => 0,                                  # SQL_SQL92_PREDICATES
    161 => 466,                                # SQL_SQL92_RELATIONAL_JOIN_OPERATORS
    162 => 32640,                              # SQL_SQL92_REVOKE
    163 => 7,                                  # SQL_SQL92_ROW_VALUE_CONSTRUCTOR
    164 => 255,                                # SQL_SQL92_STRING_FUNCTIONS
    165 => 0,                                  # SQL_SQL92_VALUE_EXPRESSIONS
    118 => 4,                                  # SQL_SQL_CONFORMANCE
    166 => 2,                                  # SQL_STANDARD_CLI_CONFORMANCE
    167 => 97863,                              # SQL_STATIC_CURSOR_ATTRIBUTES1
    168 => 6016,                               # SQL_STATIC_CURSOR_ATTRIBUTES2
     83 => 7,                                  # SQL_STATIC_SENSITIVITY
     50 => 491519,                             # SQL_STRING_FUNCTIONS
     95 => 0,                                  # SQL_SUBQUERIES
     51 => 7,                                  # SQL_SYSTEM_FUNCTIONS
     45 => 'table',                            # SQL_TABLE_TERM
    109 => 0,                                  # SQL_TIMEDATE_ADD_INTERVALS
    110 => 0,                                  # SQL_TIMEDATE_DIFF_INTERVALS
     52 => 106495,                             # SQL_TIMEDATE_FUNCTIONS
     46 => 3,                                  # SQL_TXN_CAPABLE
     72 => 15,                                 # SQL_TXN_ISOLATION_OPTION
     96 => 0,                                  # SQL_UNION
  10000 => 1992,                               # SQL_XOPEN_CLI_YEAR
);

my %odbc_info_subs = (
      2 => sub { "DBI:MariaDB:" . $_[0]->{Name} },                                                                                        # SQL_DATA_SOURCE_NAME
     17 => sub { ($_[0]->FETCH('mariadb_serverinfo') =~ /MariaDB|-maria-/) ? 'MariaDB' : 'MySQL' },                                       # SQL_DBMS_NAME
     18 => sub { my $ver = $_[0]->FETCH('mariadb_serverversion'); sprintf("%02u.%02u.%02u00", $ver/10000, ($ver%10000)/100, $ver%100) },  # SQL_DBMS_VER
    105 => sub { $_[0]->FETCH('mariadb_max_allowed_packet') },                                                                            # SQL_MAX_STATEMENT_LEN
    106 => sub { $_[0]->FETCH('mariadb_serverversion') >= 50000 ? 63 : 31 },                                                              # SQL_MAX_TABLES_IN_SELECT
     13 => sub { $_[0]->FETCH('mariadb_hostinfo') },                                                                                      # SQL_SERVER_NAME
     47 => sub { $_[0]->{Username} },                                                                                                     # SQL_USER_NAME
);

sub get_info {
    my ($dbh, $type) = @_;
    return $odbc_info_constants{$type} if exists $odbc_info_constants{$type};
    return $odbc_info_subs{$type}->($dbh) if exists $odbc_info_subs{$type};
    return undef;
}

BEGIN {
    my @needs_async_check = qw/begin_work/;

    foreach my $method (@needs_async_check) {
        no strict 'refs';

        my $super = "SUPER::$method";
        *$method  = sub {
            my $h = shift;
            return unless $h->func('_async_check');
            return $h->$super(@_);
        };
    }
}


# ====== STATEMENT ======
package # hide from PAUSE
    DBD::MariaDB::st;

use strict;

BEGIN {
    my @needs_async_result = qw/fetchrow_hashref fetchall_hashref/;
    my @needs_async_check = qw/bind_param_array bind_col bind_columns execute_for_fetch/;

    foreach my $method (@needs_async_result) {
        no strict 'refs';

        my $super = "SUPER::$method";
        *$method = sub {
            my $sth = shift;
            if(defined $sth->mariadb_async_ready) {
                return unless $sth->mariadb_async_result;
            }
            return $sth->$super(@_);
        };
    }

    foreach my $method (@needs_async_check) {
        no strict 'refs';

        my $super = "SUPER::$method";
        *$method = sub {
            my $h = shift;
            return unless $h->func('_async_check');
            return $h->$super(@_);
        };
    }
}

1;
