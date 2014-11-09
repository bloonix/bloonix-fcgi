use strict;
use warnings;
use Module::Build;

my $build = Module::Build->new(
    create_makefile_pl => "traditional",
    module_name => "Bloonix::FCGI",
    dist_author => "Jonny Schulz",
    dist_abstract => "FCGI wrapper",
    dist_version => "0.3",
    sign => 0,
    requires => {
        "Bloonix::Core" => 0,
        "FCGI" => 0,
        "IO::Socket" => 0,
        "JSON" => 0,
        "Log::Handler" => 0,
        "Params::Validate" => 0,
        "POSIX" => 0,
        "Time::HiRes" => 0
    },
);

$build->create_build_script;
