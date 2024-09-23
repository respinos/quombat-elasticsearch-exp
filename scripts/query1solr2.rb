#!/usr/bin/env perl

use utf8;
use feature qw(say);
use JSON::XS ();
use WebService::Solr::Tiny;
use DBI;
use Data::Dumper;

use open ':std', ':encoding(UTF-8)';

sub _fetch {
    my ( $string ) = @_;
    unless ( $string ) { return []; }
    return [ split(/\|/, lc $string) ];
}

my $collid = shift @ARGV;

my $dsn = "DBI:mysql:database=dlxs;host=127.0.0.1;port=33306";
my $dbh = DBI->connect( $dsn, 'dlxsadm', 'a!!pri^' );

# collmgr = DB[:Collection].join(:ImageClass, :collid => :collid, :userid => :userid).where(Sequel.lit('Collection.userid = ? AND Collection.collid = ?', 'roger', collid)).order(Sequel.lit('Collection.collid')).to_a.first
my $collmgr_sql = <<SQL;
SELECT * FROM Collection JOIN ImageClass ON 
    ImageClass.collid = Collection.collid AND 
    ImageClass.userid = Collection.userid
WHERE Collection.userid = 'roger' AND Collection.collid = ?
SQL
my $collmgr = $dbh->selectrow_hashref($collmgr_sql, undef, $collid);
my $browsefields = [];
foreach my $field ( @{ _fetch($$collmgr{browsefields}) } ) {
  push @$browsefields, $field . '_ss';
}
say Dumper($browsefields);

# my $browsefields = [
#   'sdlhomes_orig_usage_s',
#   'sdlhomes_present_usage_s',
#   'sdlhomes_ownership_s',
#   'sdlhomes_date_construction_s',
#   'sdlhomes_builder_s'
# ];

my $url = q{http://localhost:8983/solr/gettingstarted/select};
my $solr = WebService::Solr::Tiny->new( url => $url, decoder => \&JSON::XS::decode_json );

my @q = (); my @fq = ();
foreach my $arg ( @ARGV ) {
  if ( $arg =~ m,_ss:, ) {
    push @fq, $arg;
  } else {
    push @q, $arg;
  }
}

my $q1 = join(" ", @q);
my $results = $solr->search($q1, 
  fq => [ 
    "collid_s:$collid",
    @fq,
  ],
  facet => 'true',
  'facet.field' => $browsefields);

# say Dumper($results);
say Dumper( $$results{responseHeader} );
my $docs = $$results{response}{docs};
say "-- total: ", scalar @$docs, " / " . $$results{response}{numFound} . "\n";
my $facet_fields = $$results{facet_counts}{facet_fields};
foreach my $key ( sort keys %$facet_fields ) {
  my @values = @{ $$facet_fields{$key} };
  while ( my ( $term, $count ) = splice(@values, 0, 2) ) {
    next unless ( $count );
    say "-- :: $key :: $term :: $count";
  }
  say "";
}


say "-30-";