# -*- cperl -*-

use strict;
use warnings;
use utf8;

use 5.008_001;
use Config;
use Getopt::Long;
use ExtUtils::MakeMaker;
use Data::Dumper;
use Devel::CheckLib 1.12;
use File::Spec;
require DBI;
require DBI::DBD;

my $TESTDB = "test";

# On WIN32 windows.h and winsock.h need to be included before mysql.h
# Otherwise SOCKET type which is needed for mysql.h is not defined
my @mysql_headers;
push @mysql_headers, 'windows.h', 'winsock.h' if $^O eq 'MSWin32';
push @mysql_headers, 'mysql.h';

our $opt = { "help" => \&Usage, };

my ($test_host, $test_port, $test_socket, $test_authplugin);
{
local ($::test_host, $::test_port, $::test_user, $::test_socket, $::test_authplugin, $::test_password, $::test_db, $::test_mysql_config, $::test_cflags, $::test_libs);
eval { require "./t/MariaDB.mtest" } and do {
$opt->{'testuser'} = $::test_user;
$opt->{'testpassword'} = $::test_password;
$opt->{'testdb'} = $::test_db;
$opt->{'mysql_config'} = $::test_mysql_config;
$opt->{'cflags'} = $::test_cflags;
$opt->{'libs'} = $::test_libs;
$test_host = $::test_host;
$test_port = $::test_port;
$test_socket = $::test_socket;
$test_authplugin = $::test_authplugin;
}
}

Getopt::Long::GetOptions(
    $opt,
    "help",
    "testdb:s",
    "testhost:s",
    "testport:s",
    "testuser:s",
    "testpassword:s",
    "testsocket:s",
    "testauthplugin:s",
    "cflags:s",
    "libs:s",
    "mysql_config:s",
    "mariadb_config:s",
    ) || die Usage();

$opt->{mysql_config} = delete $opt->{mariadb_config} if exists $opt->{mariadb_config};

my $source = {};

  #Check for mysql_config first
  $source->{'mysql_config'} = "guessed";

  if ($opt->{'mysql_config'}) {
    $source->{'mysql_config'} = "User's choice";
  }
  if (!$opt->{'mysql_config'} && $ENV{DBD_MARIADB_CONFIG}) {
     $opt->{'mysql_config'} = $ENV{DBD_MARIADB_CONFIG};
     $source->{'mysql_config'} = 'environment';
  }
  if ($opt->{'mysql_config'})
  {
    if ($opt->{'mysql_config'} ne 'skip') {
    $opt->{'mysql_config'} = Win32::GetShortPathName($opt->{'mysql_config'}) || $opt->{'mysql_config'}
        if $^O eq 'MSWin32';
    if (!CanExecute($opt->{'mysql_config'}))
    {
      die <<"MSG";

Specified mysql configuration script '$opt->{'mysql_config'}' doesn't exist.
Please check path/permissions.
MSG
    }
    }
  }
  else
  {
    if (CanExecute("mariadb_config"))
    {
      $opt->{'mysql_config'} = "mariadb_config";
    }
    elsif (CanExecute("mysql_config"))
    {
      $opt->{'mysql_config'} = "mysql_config";
    }
    else
    {
      print <<MSG;

Cannot find the file 'mariadb_config' nor 'mysql_config'! Your execution PATH doesn't seem
not contain the path to mariadb_config or mysql_config. Resorting to guessed values!
MSG
    }
  }

for my $key (qw(testdb testhost testuser testpassword testsocket testauthplugin testport cflags libs))
{
  Configure($opt, $source, $key);
}

if (!$opt->{testport} && (!$opt->{testhost} || $opt->{testhost} eq 'localhost') && !defined $opt->{testsocket} && $test_socket) {
  $opt->{testsocket} = $test_socket;
  $source->{testsocket} = "User's choice";
}

if (!$opt->{testsocket}) {
  if (!defined $opt->{testhost} && $test_host && (!$opt->{testport} || $test_host ne 'localhost')) {
    $opt->{testhost} = $test_host;
    $source->{testhost} = "User's choice";
  }
  if (!defined $opt->{testport} && $test_port && (!$opt->{testhost} || $opt->{testhost} ne 'localhost')) {
    $opt->{testport} = $test_port;
    $source->{testport} = "User's choice";
  }
} else {
  if (!defined $opt->{testhost} && $test_host && $test_host eq 'localhost') {
    $opt->{testhost} = 'localhost';
    $source->{testhost} = "User's choice";
  }
}

#if we have a testport but no host, assume 127.0.0.1
if ( $opt->{testport} && !$opt->{testhost} ) {
  $opt->{testhost} = '127.0.0.1';
  $source->{testhost} = 'guessed';
}

foreach (qw(testhost testport testsocket testauthplugin)) {
  next if defined $opt->{$_};
  $opt->{$_} = '';
  $source->{$_} = 'default';
}

# testsocket makes sense only when testhost is localhost
if ($opt->{testsocket} && $opt->{testhost} && $opt->{testhost} ne 'localhost') {
  die << "MSG";
Option --testport or --testhost different from localhost cannot be specified together with option --testsocket.
MSG
}

# testport cannot be specified when host is localhost
if ($opt->{testport} && $opt->{testhost} && $opt->{testhost} eq 'localhost') {
  die << "MSG";
Option --testport cannot be specified when --testhost is localhost.
MSG
}

# testhost cannot be embedded
if ($opt->{testhost} && $opt->{testhost} eq 'embedded') {
  die << "MSG";
Option --testhost cannot be embedded.
MSG
}

# Separate libs and libdirs from ldflags
# When linking libdirs must be specified before libs due to runtime linking problems
my ($main_lib, @libs, @libdirs, @ldflags);
for (split ' ', $opt->{libs}) {
  if (not defined $main_lib and /^-l(?:.*mariadb.*|.*mysql.*)$/) {
    $main_lib = $_;
  } elsif (/^-l/) {
    push @libs, $_;
  } elsif (/^-L/) {
    push @libdirs, $_;
  } else {
    push @ldflags, $_;
  }
}
if (not defined $main_lib) {
  $main_lib = shift @libs;
}
if (not defined $main_lib) {
  die <<"MSG";
No MariaDB or MySQL client library was specified. Use

  perl Makefile.PL --libs="-L<dir> -l<library>"

to set directory and client library. For details see the DBD::MariaDB::INSTALL,
section "Linker flags" or type

  perl Makefile.PL --help
MSG
}

print <<"MSG";
I will use the following settings for compiling and testing:

MSG

delete $opt->{'help'};
my $keylen = 0;
for my $key (keys %$opt)
{
  $keylen = length($key) if length($key) > $keylen;
}
my $slen = 0;
for my $val (values %$source)
{
  $slen = length($val) if length($val) > $slen;
}

for my $key (sort { $a cmp $b} keys %$opt)
{
  printf("  %-" . $keylen . "s (%-" . $slen . "s) = %s\n",
	 $key, $source->{$key}, $opt->{$key})
}

print <<"MSG";

To change these settings, see 'perl Makefile.PL --help' and
'perldoc DBD::MariaDB::INSTALL'.

MSG

print "Checking if libs and header files are available for compiling...\n";

assert_lib(
  LIBS => (join ' ', @libdirs, $main_lib, @libs),
  ccflags => (join ' ', $opt->{cflags}, ($Config{usedl} ? $Config{cccdlflags} : ())),
  ldflags => (join ' ', @libdirs, @ldflags, ($Config{usedl} ? $Config{lddlflags} : ())),
  header => \@mysql_headers,
  not_execute => 1,
);

print "Checking if correct version of MariaDB or MySQL client is present...\n";

check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags),
  header => \@mysql_headers,
  function => "\n#ifndef MYSQL_VERSION_ID\n#include <mysql_version.h>\n#endif\nreturn (MYSQL_VERSION_ID >= 40108) ? 0 : 1;\n",
) or die <<"MSG";

ERROR: Wrong version or unable to check version of MariaDB/MySQL include files.
To build DBD::MariaDB it is needed MariaDB or MySQL client >= 4.1.8.

MSG

print "Looks good.\n\n";

my $have_embedded = check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags),
  header => \@mysql_headers,
  function => 'return mysql_embedded() ? 0 : 1;',
);

print "Embedded server: " . ($have_embedded ? "supported" : "not supported by client library") . "\n\n";

my $have_get_charset_number = check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags),
  header => \@mysql_headers,
  function => 'unsigned int get_charset_number(const char *cs_name, unsigned int cs_flags); mysql_server_init(-1, 0, 0); get_charset_number("utf8", 32); return 0;',
);

my $have_get_option = check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags),
  header => \@mysql_headers,
  function => 'MYSQL mysql; char buf[1024]; mysql_server_init(-1, 0, 0); mysql_init(&mysql); mysql_get_option(&mysql, 0, &buf); return 0;',
);

my $have_deinitialize_ssl = check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags),
  header => \@mysql_headers,
  function => 'mariadb_deinitialize_ssl = 0; return 0;',
);

# Check if mysql_server_end() deinitialize OpenSSL library functions
# See: https://github.com/perl5-dbi/DBD-MariaDB/issues/119
my $have_problem_with_openssl = !check_lib(
  LIBS => (join ' ', @libdirs, $main_lib),
  ccflags => $opt->{cflags},
  ldflags => (join ' ', @libdirs, @libs, @ldflags, $Config{perllibs}),
  header => \@mysql_headers,
  function => << 'EOF'

#ifndef _WIN32
  #ifndef _GNU_SOURCE
    #define _GNU_SOURCE
  #endif
  #include <dlfcn.h>
  #ifndef RTLD_DEFAULT
    #define RTLD_DEFAULT ((void *)0)
  #endif
#endif

int (*OPENSSL_init_ssl_func)(unsigned long long, const void *);
void (*SSL_load_error_strings_func)(void);
int (*SSL_library_init_func)(void);
void *(*SSL_CTX_new_func)(const void *method);
void (*SSL_CTX_free_func)(void *ctx);
const void *(*TLS_method_func)(void);
const void *method1;
const void *method2;
void *ctx1;
void *ctx2;

#ifdef _WIN32

  BOOL (WINAPI *EnumProcessModules_func)(HANDLE, HMODULE *, DWORD, LPDWORD);
  HMODULE psapi_lib;
  DWORD size, i;
  HMODULE *modules;

  modules = NULL;
  psapi_lib = LoadLibraryA("Psapi.dll");
  if (psapi_lib) {
    EnumProcessModules_func = (BOOL (WINAPI *)(HANDLE, HMODULE *, DWORD, LPDWORD))GetProcAddress(psapi_lib, "EnumProcessModules");
    if (EnumProcessModules_func) {
      if (EnumProcessModules_func((HANDLE)-1, NULL, 0, &size)) {
        modules = (HMODULE *)GlobalAlloc(GPTR, size);
        if (modules && !EnumProcessModules_func((HANDLE)-1, modules, size, &size))
          modules = NULL;
      }
    }
  }

  #define SYMBOL(var, type, sym) do { \
      var = (type)GetProcAddress(GetModuleHandle(NULL), sym); \
      if (!var) { \
        if (!modules) \
          return 1; \
        for (i = 0; i < size/sizeof(*modules); ++i) { \
          var = (type)GetProcAddress(modules[i], sym); \
          if (var) \
            break; \
        } \
      } \
    } while (0)

#else

  #define SYMBOL(var, type, sym) do { var = (type)dlsym(RTLD_DEFAULT, sym); } while (0)

#endif

SYMBOL(OPENSSL_init_ssl_func, int (*)(unsigned long long, const void *), "OPENSSL_init_ssl");
SYMBOL(SSL_library_init_func, int (*)(void), "SSL_library_init");
if (!OPENSSL_init_ssl_func && !SSL_library_init_func)
  return 0;
SYMBOL(SSL_load_error_strings_func, void (*)(void), "SSL_load_error_strings");
if (!OPENSSL_init_ssl_func && !SSL_load_error_strings_func)
  return 0;
SYMBOL(SSL_CTX_new_func, void *(*)(const void *), "SSL_CTX_new");
if (!SSL_CTX_new_func)
  return 0;
SYMBOL(SSL_CTX_free_func, void (*)(void *), "SSL_CTX_free");
if (!SSL_CTX_free_func)
  return 0;

SYMBOL(TLS_method_func, const void *(*)(void), "TLS_method");
if (!TLS_method_func)
  SYMBOL(TLS_method_func, const void *(*)(void), "TLSv1_2_method");
if (!TLS_method_func)
  SYMBOL(TLS_method_func, const void *(*)(void), "TLSv1_1_method");
if (!TLS_method_func)
  SYMBOL(TLS_method_func, const void *(*)(void), "TLSv1_method");
if (!TLS_method_func)
  return 0;

if (OPENSSL_init_ssl_func) {
  OPENSSL_init_ssl_func(0, (void *)0);
} else {
  SSL_library_init_func();
  SSL_load_error_strings_func();
}

method1 = TLS_method_func();
if (!method1)
  return 1;
ctx1 = SSL_CTX_new_func(method1);
if (!ctx1)
  return 1;

mysql_server_init(-1, 0, 0);
mysql_server_end();

method2 = TLS_method_func();
if (!method2)
  return 1;
ctx2 = SSL_CTX_new_func(method2);
if (!ctx2)
  return 1;

SSL_CTX_free_func(ctx1);
SSL_CTX_free_func(ctx2);

return 0;

EOF
);

print "Client library deinitialize OpenSSL library functions: " . ($have_problem_with_openssl ? "yes" : "no") . "\n\n";

my $fileName = File::Spec->catfile("t", "MariaDB.mtest");
(open(FILE, ">$fileName") &&
 (print FILE ("{ local " . Data::Dumper->Dump([$opt], ["opt"]) .
	      " local " . Data::Dumper->Dump([$source], ["source"]) .
	      "\$::test_host = \$opt->{'testhost'};\n" .
	      "\$::test_port = \$opt->{'testport'};\n" .
	      "\$::test_user = \$opt->{'testuser'};\n" .
              "\$::test_socket = \$opt->{'testsocket'};\n" .
              "\$::test_authplugin = \$opt->{'testauthplugin'};\n" .
	      "\$::test_password = \$opt->{'testpassword'};\n" .
	      "\$::test_db = \$opt->{'testdb'};\n" .
	      "\$::test_dsn = \"DBI:MariaDB:\$::test_db\";\n" .
              "\$::test_dsn .= \":\$::test_host\" if \$::test_host;\n" .
	      "\$::test_dsn .= \":\$::test_port\" if \$::test_port;\n".
	      "\$::test_dsn .= \";mariadb_socket=\$::test_socket\" if \$::test_socket;\n" .
	      "\$::test_dsn .= \";mariadb_auth_plugin=\$::test_authplugin\" if \$::test_authplugin;\n" .
	      "\$::test_dsn .= \";mariadb_connect_timeout=120;mariadb_read_timeout=120;mariadb_write_timeout=120\";\n" .
	      "\$::test_mysql_config = \$opt->{'mysql_config'} if \$source->{'mysql_config'} eq 'User\\'s choice';\n" .
	      "\$::test_cflags = \$opt->{'cflags'} if \$source->{'cflags'} eq 'User\\'s choice';\n" .
	      "\$::test_libs = \$opt->{'libs'} if \$source->{'libs'} eq 'User\\'s choice';\n" .
	      "} 1;\n"))  &&
  close(FILE))  ||  die "Failed to create $fileName: $!";


my $cflags = "-I\$(DBI_INSTARCH_DIR) $opt->{'cflags'}";
$cflags .= " -DHAVE_DBI_1_634" if eval { DBI->VERSION(1.634) };
$cflags .= " -DHAVE_DBI_1_642" if eval { DBI->VERSION(1.642) };
$cflags .= " -DHAVE_EMBEDDED" if $have_embedded;
$cflags .= " -DHAVE_GET_CHARSET_NUMBER" if $have_get_charset_number;
$cflags .= " -DHAVE_GET_OPTION" if $have_get_option;
$cflags .= " -DHAVE_DEINITIALIZE_SSL" if $have_deinitialize_ssl;
$cflags .= " -DHAVE_PROBLEM_WITH_OPENSSL" if $have_problem_with_openssl;
my %o =
  (
    'NAME' => 'DBD::MariaDB',
    'INC' => $cflags,
    'dist' => {
      'SUFFIX' => ".gz",
      'DIST_DEFAULT' => 'all tardist',
      'TARFLAGS' => '--format=ustar -c -v -f',
      'COMPRESS' => "gzip -9f",
    },
    'clean' => { 'FILES' => '*.xsi' },
    'realclean' => { 'FILES' => 't/MariaDB.mtest' },
    'C' => [ "dbdimp.c", "MariaDB.c" ],
    'XS' => { 'MariaDB.xs' => 'MariaDB.c' },
    'PM' => {
      'lib/DBD/MariaDB.pm' => '$(INST_LIB)/DBD/MariaDB.pm',
      'lib/DBD/MariaDB.pod' => '$(INST_LIB)/DBD/MariaDB.pod',
      'lib/DBD/MariaDB/INSTALL.pod' => '$(INST_LIB)/DBD/MariaDB/INSTALL.pod',
      'README.pod' => '$(INST_LIB)/DBD/MariaDB/README.pod',
    },
    'MAN3PODS' => {
      'lib/DBD/MariaDB.pod' => '$(INST_MAN3DIR)/DBD::MariaDB.$(MAN3EXT)',
      'lib/DBD/MariaDB/INSTALL.pod' => '$(INST_MAN3DIR)/DBD::MariaDB::INSTALL.$(MAN3EXT)',
      'README.pod' => '$(INST_MAN3DIR)/DBD::MariaDB::README.$(MAN3EXT)',
    },
    'OBJECT' => '$(O_FILES)',
    'LIBS' => (join ' ', @libdirs, $main_lib, @libs),
    @ldflags ? (dynamic_lib => { OTHERLDFLAGS => (join ' ', @ldflags) }) : (),
    'VERSION_FROM' => 'lib/DBD/MariaDB.pm',
    ABSTRACT => 'MariaDB and MySQL driver for the Perl5 Database Interface (DBI)',
    AUTHOR => 'Pali <pali@cpan.org>',
    LICENSE => 'perl',
    MIN_PERL_VERSION => '5.008001',
    META_MERGE => {
      'meta-spec' => {
        version => 2,
      },
      dynamic_config => 0,
      resources => {
        repository  => {
          url  => 'https://github.com/perl5-dbi/DBD-MariaDB.git',
          web  => 'https://github.com/perl5-dbi/DBD-MariaDB',
          type => 'git',
        },
        bugtracker => {
          web  => 'https://github.com/perl5-dbi/DBD-MariaDB/issues',
        },
        x_MailingList => 'mailto:dbi-dev@perl.org',
        license     => [ 'http://dev.perl.org/licenses/' ],
        homepage    => 'https://github.com/perl5-dbi/DBD-MariaDB',
        x_IRC       => 'irc://irc.perl.org/#dbi',
      },
      x_contributors => [
        # a list of our awesome contributors generated from git
        # using the command:
        # git shortlog -se | cut  -f2- | sed "s/^/        '/;s/$/',/"
        'Alexandr Ciornii <alexchorny@gmail.com>',
        'Alexey Molchanov <alexey.molchanov@portaone.com>',
        'Amiri Barksdale at Home <amiri@roosterpirates.com>',
        'Andrew Miller <ikari7789@yahoo.com>',
        'Aran Deltac <bluefeet@gmail.com>',
        'Bernt M. Johnsen <bernt.johnsen@oracle.com>',
        'Branislav Zahradník <branislav.zahradnik@gooddata.com>',
        'Chase Whitener <chase.whitener@infotechfl.com>',
        'Chip Salzenberg <chip@topsy.com>',
        'Chris Hammond <chrishammond@ymail.com>',
        'Chris Weyl <cweyl@alumni.drew.edu>',
        'Christian Walde <walde.christian@googlemail.com>',
        'Dagfinn Ilmari Mannsåker <ilmari@ilmari.org>',
        'Daisuke Murase <typester@cpan.org>',
        'Damyan Ivanov <dmn@debian.org>',
        'Dan Book <grinnz@gmail.com>',
        'Daniël van Eeden <daniel.vaneeden@booking.com>',
        'Dave Lambley <davel@isosceles.(none)>',
        'David Farrell <davidnmfarrell@gmail.com>',
        'David Steinbrunner <dsteinbrunner@pobox.com>',
        'Dominik Sauer <dominik.sauer@gooddata.com>',
        'Giovanni Bechis <giovanni@bigio.snb.it>',
        'Graham Ollis <plicease@cpan.org>',
        'H.Merijn Brand - Tux <h.m.brand@xs4all.nl>',
        'Hanno <hanno@gentoo.org>',
        'James McCoy <jamessan@jamessan.com>',
        'Jan Holcapek <jan.holcapek@gooddata.com>',
        'Jim Winstead <jimw@trainedmonkey.com>',
        'Juergen Weigert <jw@suse.com>',
        'Kenny Gryp <kenny.gryp@percona.com>',
        'Lu Shengliang <lushl9301@gmail.com>',
        'Masahiro Chiba <chiba@everqueue.com>',
        'Matthew Horsfall (alh) <WolfSage@gmail.com>',
        'Michiel Beijen <michiel.beijen@gmail.com>',
        'Mike Pomraning <mjp@pilcrow.madison.wi.us>',
        'Mohammad S Anwar <mohammad.anwar@yahoo.com>',
        'Pali <pali@cpan.org>',
        'Patrick Galbraith <patg@patg.net>',
        'Perlover <perlover@perlover.com>',
        'Peter Botha <peterb@striata.com>',
        'Petr Písař <ppisar@redhat.com>',
        'Reini Urban <rurban@cpanel.net>',
        'Rob Hoelz <rhoelz@inoc.com>',
        'Rob Van Dam <rvandam00@gmail.com>',
        'Rudy Lippan <rlippan@remotelinux.com>',
        'Scimon <simon.proctor@gmail.com>',
        'Sergey Zhuravlev <zhurs@ya.ru>',
        'Sergiy Borodych <Sergiy.Borodych@gmail.com>',
        'Sharif Nassar <mrwacky42+github@gmail.com>',
        'Steffen Mueller <smueller@cpan.org>',
        'Steven Hartland <steven.hartland@multiplay.co.uk>',
        'Taro Kobayashi <9re.3000@gmail.com>',
        'Tatsuhiko Miyagawa <miyagawa@bulknews.net>',
        'Tim Mullin <tim@cpanel.net>',
        'Ville Skyttä <ville.skytta@iki.fi>',
        'Vladimir Marek <vlmarek@volny.cz>',
        'katyavoid <katyavoid@gmail.com>',
        'kmx <kmx@cpan.org>',
        'tokuhirom <tokuhirom@gmail.com>',
        'zefram <zefram@fysh.org>',
        'zentooo <ankerasoy@gmail.com>',
      ],
      prereqs => {
        test => {
          recommends => {
            'Net::SSLeay' => '1.43',
            'Proc::ProcessTable' => 0,
            'TAP::Harness' => '3.31',
            'CPAN::Meta::YAML' => 0,
          },
          suggests => {
            'Storable' => 0,
            'Test::Pod' => '1.41',
            'Test::DistManifest' => 0,
          },
        },
      },
    },
    CONFIGURE_REQUIRES => {
      'strict' => 0,
      'utf8' => 0,
      'warnings' => 0,
      'Config' => 0,
      'Data::Dumper' => 0,
      'DBI' => '1.608',
      'Devel::CheckLib' => '1.12',
      'ExtUtils::MakeMaker' => 0,
      'File::Spec' => 0,
      'Getopt::Long' => 0,
    },
    PREREQ_PM => {
      'strict' => 0,
      'warnings' => 0,
      'DBI' => 1.608,
      'DynaLoader' => 0,
    },
    TEST_REQUIRES => {
      'bigint' => 0,
      'constant' => 0,
      'lib' => 0,
      'strict' => 0,
      'utf8' => 0,
      'vars' => 0,
      'warnings' => 0,
      'B' => 0,
      'DBI' => 0,
      'DBI::Const::GetInfoType' => 0,
      'Data::Dumper' => 0,
      'Encode' => 0,
      'File::Temp' => 0,
      'FindBin' => 0,
      'Test::Deep' => 0,
      'Test::More' => '0.90',
      'Time::HiRes' => 0,
    },
  );

WriteMakefile1(%o);
exit 0;


############################################################################
#
#   Name:    Usage
#
#   Purpose: Print Usage message and exit with error status.
#
############################################################################

sub Usage {
  print STDERR <<"USAGE";
Usage: perl $0 [options]

Possible options are:

  --cflags=<flags>       Use <flags> for running the C compiler; defaults
                         to the value of "mariadb_config --cflags" or a guessed
                         value
  --libs=<libs>          Use <libs> for running the linker; defaults
                         to the value of "mariadb_config --libs" or a guessed
                         value
  --testdb=<db>          Use the database <db> for running the test suite;
                         defaults to $TESTDB
  --testuser=<user>      Use the username <user> for running the test suite;
                         defaults to no username
  --testpassword=<pwd>   Use the password <pwd> for running the test suite;
                         defaults to no password
  --testhost=<host>      Use <host> as a database server for running the
                         test suite; defaults to localhost.
  --testport=<port>      Use <port> as the port number of the database;
                         by default the port number is chosen from the
                         mariadb/mysqlclient library; ignored when --testhost
                         is set to "localhost"
  --testsocket=<socket>  Use <socket> as a unix socket for connecting to
                         the database server; by default unix socket is chosen
                         by mariadb/mysqlclient library; takes effect only
                         when --testhost is set to "localhost"
  --testauthplugin=<ap>  Use <ap> auth plugin when doing user authentication
                         handshake with server; for older server versions it is
                         needed to pass "mysql_native_password"
  --mariadb_config       Synonym for --mysql_config, override it
  --mysql_config=<path>  Specify <path> for mariadb_config or mysql_config script
  --help                 Print this message and exit

All options may be configured on the command line. If they are
not present on the command line, then mariadb_config is called (if
it can be found):

  mariadb_config --cflags
  mariadb_config --libs

and so on. See DBD::MariaDB::INSTALL for details.
USAGE
  exit 1;
}


sub CanExecute {
  my ($command) = @_;
  return MM->maybe_command($command) if File::Spec->file_name_is_absolute($command);
  foreach my $dir (File::Spec->path) {
    return 1 if MM->maybe_command(File::Spec->catfile($dir, $command));
  }
  return 0;
}


my %ParsedCache;
sub ParseMySQLConfig {
  my ($opt, $param) = @_;
  return $ParsedCache{$param} if exists $ParsedCache{$param};
  return undef unless $opt->{'mysql_config'} and $opt->{'mysql_config'} ne 'skip';
  my $output = `$opt->{'mysql_config'} --$param 2>&1`;
  if ($? == 0 and defined $output and $output !~ /Options:|Usage:|unrecognized option/) {
    $output =~ s/\s+$//s;
    $output =~ s/^\s+//s;
    $ParsedCache{$param} = $output;
  } else {
    $ParsedCache{$param} = undef;
  }
  return $ParsedCache{$param};
}


############################################################################
#
#   Name:    Configure
#
#   Purpose: Automatic configuration
#
#   Inputs:  $param - Name of the parameter being configured
#
#   Returns: Generated value, never undef
#
############################################################################

sub Configure {
    my($opt, $source, $param) = @_;
    if (defined($opt->{$param})) {
        $source->{$param} = "User's choice";
        return;
    }

    # First try to get options values from mysql_config
    if ($param eq 'cflags') {
        my $str = ParseMySQLConfig($opt, 'cflags');
        if (defined $str) {
            $opt->{$param} = $str;
            $source->{$param} = "mysql_config";
            return;
        }
    } elsif ($param eq 'libs') {
        foreach (qw(libmysqld-libs libs_r libs)) {
            my $str = ParseMySQLConfig($opt, $_);
            next unless defined $str;
            # Unfortunately ExtUtils::MakeMaker doesn't deal very well with -L'...'
            $str =~ s/\-L\'(.*?)\'/-L$1/sg;
            $str =~ s/\-L\"(.*?)\"/-L$1/sg;
            # libmysqld-libs and libs_r do not have to be always installed
            # check that we use correct library, sometimes library reported
            # by mysql_config is not present but linker find different
            # version in system wide location; so check that version in
            # header file matches version in library
            my ($main_lib, @libdirs, @ldflags);
            for (split ' ', $str) {
                if (not defined $main_lib and /^-l(?:.*mariadb.*|.*mysql.*)$/) {
                    $main_lib = $_;
                } elsif (/^-L/) {
                    push @libdirs, $_;
                } else {
                    push @ldflags, $_;
                }
            }
            my $check_passed;
            if (defined $main_lib) {
                my $function = "\n#ifndef MYSQL_VERSION_ID\n#include <mysql_version.h>\n#endif\n";
                if ($_ eq 'libmysqld-libs') {
                    # libmariadbd.so from MariaDB 10.3.1 - 10.3.7 is broken, unusable
                    # and always crash in mysql_real_connect() function, so avoid its usage
                    # https://jira.mariadb.org/browse/MDEV-16478
                    $function .= "\n#if defined(MARIADB_BASE_VERSION) || defined(MARIADB_PACKAGE_VERSION)\nif (mysql_get_client_version() >= 100301 && mysql_get_client_version() < 100308) return 1;\n#endif\n";
                    # libmysqld.a from MySQL 8.x is broken too
                    $function .= "\n#if !defined(MARIADB_BASE_VERSION) && !defined(MARIADB_PACKAGE_VERSION)\nif (mysql_get_client_version() >= 80000) return 1;\n#endif\n";
                }
                # MariaDB Connector/C 3.1.10+ has broken mysql_get_client_version() function, so use mariadb_get_infov(MARIADB_CLIENT_VERSION_ID) instead
                $function .= "size_t version;\n";
                $function .= "#if defined(MARIADB_PACKAGE_VERSION) && defined(MARIADB_PACKAGE_VERSION_ID) && MARIADB_PACKAGE_VERSION_ID >= 30000\n";
                $function .= "if (mariadb_get_infov((void *)0, MARIADB_CLIENT_VERSION_ID, &version) != 0)\n";
                $function .= "#endif\n";
                $function .= "version = mysql_get_client_version();\n";
                $function .= 'return (version == MYSQL_VERSION_ID) ? 0 : 1;';
                # libmysqld is built using g++ rather than gcc and sometimes
                # we have to use libstdc++ to resolve linking problems
                foreach my $add_ldflags (undef, '-lstdc++') {
                    my $ldflags = join ' ', @ldflags, (defined $add_ldflags ? $add_ldflags : ());
                    if ($Config{usedl}) {
                        # check that we can create a shared library (not executable)
                        check_lib(
                            LIBS => (join ' ', @libdirs, $main_lib),
                            ldflags => (join ' ', @libdirs, $ldflags, $Config{lddlflags}),
                            ccflags => (join ' ', $opt->{cflags}, $Config{cccdlflags}),
                            header => \@mysql_headers,
                            function => $function,
                            not_execute => 1,
                        ) or next;
                    }
                    # and now create executable and do real version check
                    check_lib(
                        LIBS => (join ' ', @libdirs, $main_lib),
                        ldflags => (join ' ', @libdirs, $ldflags),
                        ccflags => $opt->{cflags},
                        header => \@mysql_headers,
                        function => $function,
                    ) or next;
                    $str .= ' ' . $add_ldflags if defined $add_ldflags;
                    $check_passed = 1;
                    last;
                }
            }
            # libs param is checked as last, so ensure that mysql_config
            # error is reported back to the caller
            next unless $_ eq 'libs' or $check_passed;
            $opt->{$param} = $str;
            $source->{$param} = "mysql_config";
            return;
        }
    }

    # Ok, mysql_config doesn't work. We need to do our best
    # First check environment variables
    my $normparam = uc($param);
    $normparam =~ tr/-/_/;
    if (defined($ENV{'DBD_MARIADB_'.$normparam})) {
        $opt->{$param} = $ENV{'DBD_MARIADB_'.$normparam};
        $source->{$param} = 'environment';
    }

    # Then try to guess
    unless ($opt->{$param}) {
      if ($param eq 'testuser') {
        my $user = $ENV{USER} || '';
        print "

PLEASE NOTE:

For 'make test' to run properly, you must ensure that the
database user '$user' can connect to your MariaDB or MySQL server
and has the proper privileges that these tests require such
as 'drop table', 'create table', 'drop procedure', 'create procedure'
as well as others.

mysql> grant all privileges on test.* to '$user'\@'localhost' identified by 's3kr1t';

For MySQL 8 it is needed to use different syntax:

mysql> create user '$user'\@'localhost' identified by 's3kr1t';
mysql> grant all privileges on test.* to '$user'\@'localhost';

You can also optionally set the user to run 'make test' with:

perl Makefile.PL --testuser=username

";
      $opt->{$param} = $user;
	  $source->{$param} = 'guessed';
    }
    elsif ($param eq "testdb") {
      $source->{$param} = "default";
      $opt->{$param} = $TESTDB;
    }
    elsif ($param eq "testpassword") {
      $source->{$param} = "default";
      $opt->{$param} = "";
    }
    elsif ($param eq "cflags") {
      $source->{$param} = "guessed";
      my ($dir, $file) = SearchFor('include', 'mysql.h');
      if ($dir) {
        $opt->{$param} = "-I$dir";
        return;
      }
      die <<"MSG";
Failed to determine directory of mysql.h. Use

  perl Makefile.PL --cflags=-I<dir>

to set this directory. For details see DBD::MariaDB::INSTALL,
section "C Compiler flags" or type

  perl Makefile.PL --help
MSG
    }
    elsif ($param eq "libs") {
      $source->{$param} = "guessed";
      my @files = qw(mariadb.lib libmariadb.lib mariadbclient.lib mysqlclient.lib
                     libmysqld.so libmysqld.a libmariadb.so libmariadb.a
                     libmysqlclient.so libmysqlclient.a);
      my ($dir, $file) = SearchFor('lib', @files);
      if ($dir) {
          my $lib = $file;
          $lib =~ s/\.[^.]+$//;
          $lib =~ s/^lib//;
          $opt->{$param} = "-L$dir -l$lib";
          return;
      }
      die <<"MSG";
Failed to determine directory of MariaDB or MySQL client library. Use

  perl Makefile.PL --libs="-L<dir> -l<library>"

to set directory and client library. For details see the DBD::MariaDB::INSTALL,
section "Linker flags" or type

  perl Makefile.PL --help
MSG
    }
    elsif ($param eq "testhost" || $param eq "testport" || $param eq "testsocket" || $param eq "testauthplugin") {
      # known parameter, but do nothing
    }
    else {
      die "Unknown configuration parameter: $param";
    }
  }
}

sub SearchFor {
  my($subdir, @files) = @_;

  my @dirs = ($^O eq 'MSWin32') ? qw(C:) : qw(/usr/local /usr /opt);

  for my $f (@files) {
    for my $dir (@dirs) {
      my $try1 = File::Spec->catdir($dir, $subdir);
      my $try2 = File::Spec->catdir($try1, "mariadb");
      my $try3 = File::Spec->catdir($try1, "mysql");
      for my $path ($try1, $try2, $try3) {
	my $file = File::Spec->catfile($path, $f);
	if (-f $file) {
          my (undef, undef, $file) = File::Spec->splitpath($file);
          return ($path, $file);
	}
      }
    }
  }
}

package MY;
sub postamble { return DBI::DBD::dbd_postamble(@_); }

package main;

sub WriteMakefile1 {  #Written by Alexandr Ciornii, version 0.21. Added by eumm-upgrade.
  my %params=@_;
  my $eumm_version=$ExtUtils::MakeMaker::VERSION;
  $eumm_version=eval $eumm_version;
  die "EXTRA_META is deprecated" if exists $params{EXTRA_META};
  die "License not specified" if not exists $params{LICENSE};
  if ($params{META_MERGE} and $params{META_MERGE}->{'meta-spec'}->{version} >= 2 and $eumm_version < 6.68) {
    #EUMM 6.68 has problems with meta-spec 2
    delete $params{META_MERGE}->{'meta-spec'};
    if ($params{META_MERGE}->{resources}) {
      foreach (values %{$params{META_MERGE}->{resources}}) {
        $_ = $_->{url} || $_->{web} if ref $_ eq 'HASH';
      }
    }
    if ($params{META_MERGE}->{prereqs}) {
      $params{CONFIGURE_REQUIRES} = { %{$params{CONFIGURE_REQUIRES} || {'ExtUtils::MakeMaker' => 0}}, %{$params{META_MERGE}->{prereqs}->{configure}->{requires} || {}} };
      $params{BUILD_REQUIRES} = { %{$params{BUILD_REQUIRES} || {'ExtUtils::MakeMaker' => 0}}, %{$params{META_MERGE}->{prereqs}->{build}->{requires} || {}} };
      $params{TEST_REQUIRES} = { %{$params{TEST_REQUIRES} || {}}, %{$params{META_MERGE}->{prereqs}->{test}->{requires} || {}} };
      $params{PREREQ_PM} = { %{$params{PREREQ_PM} || {}}, %{$params{META_MERGE}->{prereqs}->{runtime}->{requires} || {}} };
      my @recommends = (
        %{$params{META_MERGE}->{prereqs}->{configure}->{recommends} || {}},
        %{$params{META_MERGE}->{prereqs}->{configure}->{suggests} || {}},
        %{$params{META_MERGE}->{prereqs}->{build}->{recommends} || {}},
        %{$params{META_MERGE}->{prereqs}->{build}->{suggests} || {}},
        %{$params{META_MERGE}->{prereqs}->{test}->{recommends} || {}},
        %{$params{META_MERGE}->{prereqs}->{test}->{suggests} || {}},
        %{$params{META_MERGE}->{prereqs}->{runtime}->{recommends} || {}},
        %{$params{META_MERGE}->{prereqs}->{runtime}->{suggests} || {}},
      );
      $params{META_MERGE}->{recommends} = { %{$params{META_MERGE}->{recommends} || {}}, @recommends } if @recommends;
      delete $params{META_MERGE}->{prereqs};
    }
  }
  if ($params{BUILD_REQUIRES} and $eumm_version < 6.5503) {
    #EUMM 6.5502 has problems with BUILD_REQUIRES
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , %{$params{BUILD_REQUIRES}} };
    delete $params{BUILD_REQUIRES};
  }
  if ($params{TEST_REQUIRES} and $eumm_version < 6.64) {
    #EUMM 6.64 has problems with TEST_REQUIRES
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , %{$params{TEST_REQUIRES}} };
    delete $params{TEST_REQUIRES};
  }
  if ($params{CONFIGURE_REQUIRES} and $eumm_version < 6.52) {
    #EUMM prior to 6.52 has problems with CONFIGURE_REQUIRES
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , %{$params{CONFIGURE_REQUIRES}} };
    delete $params{CONFIGURE_REQUIRES};
  }
  if ($params{MIN_PERL_VERSION} and $eumm_version < 6.48) {
    #EUMM prior to 6.48 has problems with MIN_PERL_VERSION
    $params{PREREQ_PM}={ %{$params{PREREQ_PM} || {}} , perl => $params{MIN_PERL_VERSION} };
    delete $params{MIN_PERL_VERSION};
  }
  delete $params{META_MERGE} if $eumm_version < 6.46;
  delete $params{META_ADD} if $eumm_version < 6.46;
  delete $params{LICENSE} if $eumm_version < 6.31;
  delete $params{AUTHOR} if $] < 5.005;
  delete $params{ABSTRACT_FROM} if $] < 5.005;
  delete $params{BINARY_LOCATION} if $] < 5.005;

  ExtUtils::MakeMaker::WriteMakefile(%params);
}
