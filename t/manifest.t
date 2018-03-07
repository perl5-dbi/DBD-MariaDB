use Test::More;

BEGIN {
  unless ($ENV{RELEASE_TESTING}) {
    plan skip_all => 'these tests are for release testing';
  }
  unless (eval { require Test::DistManifest }) {
    plan skip_all => 'Test::DistManifest required to test MANIFEST';
  }
  Test::DistManifest->import();
}

manifest_ok();
