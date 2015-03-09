package Bloonix::FCGI::Request;

use strict;
use warnings;
use Encode;
use JSON;
use MIME::Base64 ();

use base qw(Bloonix::Accessor);
__PACKAGE__->mk_accessors(qw/content_type content_types charset boundary postdata jsondata is_json maxlen log/);

use constant OS => $^O || do {
    no warnings "once";
    require Config;
    $Config::Config{"osname"};
};

use constant CRLF => do {
    OS =~ m/VMS/i
        ? "\n"
        : "\t" ne "\011"
            ? "\r\n"
            : "\015\012";
};

sub new {
    my ($class, %opts) = @_;

    my $self = bless {
        params => { },
        maxlen => $opts{maxlen} // 5242880,
        log => Log::Handler->get_logger("bloonix"),
    }, $class;

    my $content_type = $ENV{CONTENT_TYPE} || "";
    $content_type =~ s/\s//g;

    my @content_types = split /\s*;\s*/, $content_type;
    $self->{content_type} = shift @content_types || "";

    foreach my $pair (@content_types) {
        my ($name, $value) = split /=/, $pair, 2;

        if ($name eq "charset" || $name eq "boundary") {
            $self->{$name} = $value;
        }

        $self->{content_types}->{$name} = $value;
    }

    $self->{charset} ||= "";
    $self->{boundary} ||= "";
    $self->{is_json} = $self->content_type eq "application/json";
    $self->_process_post_data;
    $self->_process_query_string($self->query_string);

    return $self;
}

sub param {
    my ($self, $param) = @_;
    my $params = $self->{params};

    if (!defined $param) {
        die "no param set";
    }

    if (!exists $params->{$param}) {
        return wantarray ? () : undef;
    }

    if (ref $params->{$param} ne "ARRAY") {
        # Normally () is not necessary...
        return wantarray ? ($params->{$param}) : $params->{$param};
    }

    if (wantarray) {
        return @{$params->{$param}};
    }

    return $params->{$param}->[0];
}

sub params {
    my $self = shift;
    my %params;

    foreach my $param (@_) {
        my $value = $self->param($param);

        # If a value is not defined then the parameter does not exist
        # in the request. In this case the parameter is not set.
        if (defined $value) {
            $params{$param} = $self->param($param);
        }
    }

    return \%params;
}

sub exist {
    my ($self, $param) = @_;
    my $params = $self->{params};

    return exists $params->{$param} ? 1 : 0;
}

sub cookie {
    my $self = shift;

    if (@_ == 1) {
        my $name = shift;
        my $cookie = $self->http_cookie || "";

        if ($cookie) {
            foreach my $pair (split /\s*;\s*/, $cookie) {
                my ($key, $value) = split /\s*=\s*/, $pair;
                if ($key eq $name) {
                    return $value;
                }
            }
        }
        return "";
    }

    my %opts = @_;
    $opts{"-path"} //= "/";
    $opts{"-value"} //= "";

    if (!defined $opts{"-name"} || $opts{"-name"} !~ /\S/) {
        die "invalid cookie name: ". $opts{"-name"};
    }

    my @cookie = (join("=", $opts{"-name"}, $opts{"-value"}));

    if ($opts{"-expires"}) {
        push @cookie, "expires=" . $self->_expires($opts{"-expires"});
    }

    foreach my $key (qw/path domain/) {
        if (defined $opts{"-$key"}) {
            push @cookie, "$key=" . $opts{"-$key"};
        }
    }

    return join("; ", @cookie);
}

sub unescape {
    my ($self, $string) = @_;

    if (defined $string) {
        $string =~ s/(?:%([0-9A-Fa-f]{2})|\+)/defined $1 ? chr(hex($1)) : ' '/eg;
    }

    return $string;
}

sub upload {
    my $self = shift;

    return $self->{uploads} ? $self->{uploads}->[0] : undef;
}

sub uploads {
    my $self = shift;

    return wantarray ? @{$self->{uploads}} : $self->{uploads};
}

sub server_name          { $ENV{SERVER_NAME}          }
sub script_name          { $ENV{SCRIPT_NAME}          }
sub http_accept_encoding { $ENV{HTTP_ACCEPT_ENCODING} }
sub server_admin         { $ENV{SERVER_ADMIN}         }
sub http_connection      { $ENV{HTTP_CONNECTION}      }
sub http_accept          { $ENV{HTTP_ACCEPT}          }
sub request_method       { $ENV{REQUEST_METHOD}       }
sub script_filename      { $ENV{SCRIPT_FILENAME}      }
sub server_software      { $ENV{SERVER_SOFTWARE}      }
sub http_accept_charset  { $ENV{HTTP_ACCEPT_CHARSET}  }
sub http_cookie          { $ENV{HTTP_COOKIE}          }
sub http_te              { $ENV{HTTP_TE}              }
sub http_user_agent      { $ENV{HTTP_USER_AGENT}      }
sub remote_port          { $ENV{REMOTE_PORT}          }
sub query_string         { $ENV{QUERY_STRING}         }
sub server_signature     { $ENV{SERVER_SIGNATURE}     }
sub server_port          { $ENV{SERVER_PORT}          }
sub http_accept_language { $ENV{HTTP_ACCEPT_LANGUAGE} }
sub server_protocol      { $ENV{SERVER_PROTOCOL}      }
sub path                 { $ENV{PATH}                 }
sub path_info            { $ENV{SCRIPT_NAME}          }
sub gateway_interface    { $ENV{GATEWAY_INTERFACE}    }
sub request_uri          { $ENV{REQUEST_URI}          }
sub server_addr          { $ENV{SERVER_ADDR}          }
sub document_root        { $ENV{DOCUMENT_ROOT}        }
sub http_host            { $ENV{HTTP_HOST}            }
sub content_length       { $ENV{CONTENT_LENGTH} || 0  }

sub referer  { $ENV{HTTP_REFERRER} || $ENV{HTTP_REFERER} }
sub referrer { $ENV{HTTP_REFERRER} || $ENV{HTTP_REFERER} }

sub remote_addr {
    $ENV{HTTP_X_REAL_IP}
    || $ENV{X_REAL_IP}
    || $ENV{HTTP_X_FORWAREDED_FOR}
    || $ENV{X_FORWARDED_FOR}
    || $ENV{REMOTE_ADDR}
    || "::"
}

sub _process_query_string {
    my $self = shift;
    my $params = $self->{params};

    if ($_[0]) {
        foreach my $pair (split /[&;]/, $_[0]) {
            my ($param, $value) = split(/=/, $pair, 2);

            if (!defined $param) {
                next;
            }

            if (!defined $value) {
                $value = "";
            }

            $param = $self->unescape($param);
            $value = $self->unescape($value);
            $self->_add_param_value($param, $value);
        }
    }
}

sub _process_post_data {
    my $self = shift;
    my $params = $self->{params};

    if ($self->content_length > 0 && (!$self->{maxlen} || $self->content_length <= $self->{maxlen})) {
        read(\*STDIN, $self->{postdata}, $self->content_length);
    }

    if ($self->{postdata}) {
        if ($self->content_type =~ m!^application/x-www-form-urlencoded!) {
            $self->_process_query_string($self->{postdata});
        } elsif ($self->content_type =~ m!^multipart/form-data!) {
            $self->_process_multipart($self->{postdata});
        } elsif ($self->is_json) {
            my $jsondata;
            local $SIG{__DIE__} = "DEFAULT";
            eval { $jsondata = JSON->new->utf8->decode($self->{postdata}) };

            if ($@) {
                $self->log->warning("unable to de-serialize json string - fixing string - $@");
                $self->{postdata} = Encode::decode("utf8", $self->{postdata});
                $self->{postdata} = Encode::encode("utf8", $self->{postdata});
                eval { $jsondata = JSON->new->utf8->decode($self->{postdata}) };
                if ($@) {
                    $self->log->warning("unable to de-serialize json string '$self->{postdata}' - $@");
                }
            } else {
                $self->{jsondata} = $jsondata;

                if (ref $jsondata eq "HASH") {
                    $self->{params} = $jsondata;
                }
            }
        }
    }
}

sub _process_multipart {
    my $self = shift;

    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1
    # Content-Disposition: form-data; name="invoice_date"
    # 
    # 01.01.2012
    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1
    # Content-Disposition: form-data; name="booking_date"
    # 
    # 01.01.2012
    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1
    # Content-Disposition: form-data; name="value_date"
    # 
    # 01.01.2012
    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1
    # Content-Disposition: form-data; name="purpose"
    # 
    # Foobar
    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1
    # Content-Disposition: form-data; name="file"; filename=""
    # Content-Type: application/octet-stream
    # 
    # ------WebKitFormBoundaryvPjZgS1pozQfuWN1--

    my $crlf = CRLF;
    my $boundary = "--" . $self->boundary;
    my @parts = split /(?:^|$crlf)\Q$boundary\E/, $_[0];

    if ($parts[0] eq "") {
        shift @parts;
    }

    if ($parts[$#parts] =~ /^\-\-($crlf|\z)/) {
        pop @parts;
    }

    foreach my $part (@parts) {
        my ($filename, $name, $content_type, $charset);
        my ($header, $body) = $part =~ /^(?:$crlf)*(.+?)$crlf$crlf(.*)/s;
        $header //= "";
        $body //= "";

        foreach my $head (split /$crlf/, $header) {
            if ($head =~ /^Content\-Disposition:\s+form\-data;.*\sfilename=(["'])([^\1]*?)\1/) {
                $filename = $2;
                if ($head =~ /\sname=(["'])([^\1]*?)\1/) {
                    $name = $2;
                }
            } elsif ($head =~ /^Content\-Transfer\-Encoding:\s+base64/) {
                $body = MIME::Base64::decode_base64($body);
            } elsif ($head =~ m!^Content\-Type:\s+([a-zA-Z_0-9\-/]+)!) {
                $content_type = $1;
                if ($head =~ /\scharset=([^\s]+)/) {
                    $charset = $1;
                }
            } elsif ($head =~ /^Content\-Disposition:\s+form\-data;.*\sname=(["'])([^\1]*?)\1/) {
                $name = $2;
            }
        }

        if ($filename) {
            my $tmpfile = qx{/bin/mktemp --suffix ".DRAGON"};
            chomp $tmpfile;

            open my $fh, ">>", $tmpfile or die "unable to open tmpfile '$tmpfile' - $!";
            print $fh $body or die "unable to write to tmpfile '$tmpfile' - $!";
            close $fh or die "unable to close tmpfile - '$tmpfile' - $!";

            my $file = {
                size => length $body,
                filename => $filename,
                tmpfile => $tmpfile,
                charset => $charset // "",
                content_type => $content_type // ""
            };

            push @{$self->{uploads}}, $file;
            $body = $file;

        }

        if (defined $name && $name =~ /\S/) {
            $self->_add_param_value($name, $body);
        }
    }
}

sub _add_param_value {
    my ($self, $param, $value) = @_;
    my $params = $self->{params};

    if (exists $params->{$param}) {
        if (ref $params->{$param} ne "ARRAY") {
            $params->{$param} = [ $params->{$param} ];
        }
        push @{$params->{$param}}, $value;
    } else {
        $params->{$param} = $value;
    }
}

sub _expires {
    my ($self, $time) = @_;

    my @mon =qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
    my @wday = qw(Sun Mon Tue Wed Thu Fri Sat);
    my %t2s = (s => 1, m => 60, h => 3_600, d => 86_400, M => 2_592_000, y => 31_536_000);

    if ($time eq "now") {
        $time = time;
    } elsif ($time =~ /^([+-]?(?:\d+|\d*\.\d*))([smhdMy])/) {
        $time = time + ($t2s{$2} * $1);
    } elsif ($time !~ /^\d+/) {
        return $time;
    }

    my ($sec, $min, $hour, $mday, $mon, $year, $wday) = gmtime($time);
    $year += 1900;

    return sprintf(
        "%s, %02d-%s-%04d %02d:%02d:%02d GMT",
        $wday[$wday], $mday, $mon[$mon], $year, $hour, $min, $sec
    );
}

sub DESTROY {
    my $self = shift;

    if ($self->{uploads}) {
        foreach my $upload (@{$self->{uploads}}) {
            if ($upload->{filename} && -e $upload->{filename}) {
                unlink($upload->{filename});
            }
        }
    }
}

1;
