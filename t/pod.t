use Test::More;

BEGIN {
  eval {
    require Test::Pod;
    Test::Pod->VERSION(1.41);
  } or do {
    plan skip_all => "Test::Pod 1.41 required for testing POD";
  };
  Test::Pod->import();
}

all_pod_files_ok();
