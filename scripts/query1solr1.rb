#!/usr/bin/env perl

use feature qw(say);
use Interchange::Search::Solr;
use DBI;
use Data::Dumper;

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
WHERE Collection.userid = 'dlxsadm' AND Collection.collid = ?
SQL
my $collmgr = $dbh->selectrow_hashref($collmgr_sql, undef, $collid);
my $browsefields = _fetch($$collmgr{browsefields});
say Dumper($browsefields);

# my $browsefields = [
#   'sdlhomes_orig_usage_s',
#   'sdlhomes_present_usage_s',
#   'sdlhomes_ownership_s',
#   'sdlhomes_date_construction_s',
#   'sdlhomes_builder_s'
# ];

my $url = q{http://localhost:8983/solr/gettingstarted};
my $solr = Interchange::Search::Solr->new(
  solr_url => $url,
  search_fields => [qw/_text_/]
);

$solr->rows(10);
$solr->start(0);
$solr->facets($browsefields);
# $solr->search_fields([qw/text_txt/]);
$solr->search('Saline');
$results = $solr->results;
say Dumper($results);
say Dumper($solr->facets_found);

say "-30-";