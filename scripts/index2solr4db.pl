#!/usr/bin/env perl

use feature qw(say);
use WebService::Solr;
use DBI;
use Data::Dumper;
use HTML::Entities;

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

my $field_admin_maps = {};
my $line = $$collmgr{field_admin_maps};
foreach my $line_ ( split(/\|/, lc $line) ) {
    my ( $key, $values ) = split(/:::/, $line_);
    $values =~ s,^\s+,,; $values =~ s,\s+$,,;
    $$field_admin_maps{$key} = [ split(/ /, $values) ];
}

my $field_xcoll_maps = {};
$line = $$collmgr{field_xcoll_maps};
foreach my $line_ ( split( /\|/, lc $line ) ) {
    my ( $key, $values ) = split( /:::/, $line_ );
    if ( $values =~ m,^", ) {
        $$field_xcoll_maps{$key} = substr($values, 1, -1);
    } else {
        $values =~ s,^\s+,,;
        $values =~ s,\s+$,,;
        $$field_xcoll_maps{$key} = [ split( / /, $values ) ];
    }
}

# build the DC mapping
my $dc_map = {};
foreach my $key ( qw/
  dc_ti
  dc_cr
  dc_su
  dc_de 
  dc_id 
  dc_pu 
  dc_da 
  dc_fo 
  dc_so 
  dc_rel 
  dc_type
  dc_ri
  dc_la
  dc_cov
  dc_ge
/ ) {
    if ( ref($$field_xcoll_maps{$key}) ) {
        $$dc_map{$key} = $$field_xcoll_maps{$key};
    }
}

my $browsefields = _fetch($$collmgr{browsefields});
my $sortflds = _fetch($$collmgr{sortflds});
my $srchflds = _fetch($$collmgr{dfltsrchflds});
my $ic_all = $$field_admin_maps{ic_all};

my $stoppable = shift @ARGV || -1;
my $n = 0;
my $limit = 1000;
my $offset = 0;

my $solr = WebService::Solr->new('http://localhost:8983/solr/gettingstarted');

my @docs = ();
my $value;
while(1) {
    last if ( $stoppable > 0 && $n >= $stoppable );
    my $sql = <<SQL;
SELECT * FROM $collid JOIN $collid\_media ON m_id = ic_id
WHERE m_searchable = 1
LIMIT $limit OFFSET $offset
SQL
    my $rows = $dbh->selectall_arrayref($sql, {Slice=>{}});
    last if ( scalar @$rows == 0 );

    foreach my $row_ ( @$rows ) {

        my $row = { map { lc $_ => $$row_{$_} } keys %$row_ };

        my $doc = WebService::Solr::Document->new;
        my $idno = $$row{istruct_isentryid};
        my @fields = ();
        push @fields, [ id => $idno ];
        my $buffer = [];
        foreach my $field ( @$ic_all ) {
            $value = $$row{$field};
            next unless ( $value );
            push @$buffer, split(/\|\|\|/, decode_entities($value));
        }
        push @fields, [ '_text_' => join(' ', @$buffer) ];
        push @fields, [ 'm_id_s' => $$row{m_id} ];
        push @fields, [ 'm_iid_s' => $$row{m_iid} ];
        push @fields, [ 'collid_s' => $collid ];
        push @fields, [ 'istruct_ms_s' => $$row{istruct_ms} || 'X' ];

        foreach my $dc_field ( keys %$dc_map ) {
            my $values = [];
            if ( ref($$dc_map{$dc_field}) ) {
                foreach my $field ( @{ $$dc_map{$dc_field} } ) {
                    # say "--> $dc_field :: $field :: $$row{$field}";
                    $value = $$row{$field};
                    next unless ( $value );
                    push @$values, split(/\|\|\|/, decode_entities($value));
                }
            }
            if ( scalar @$values ) {
                my $index_key = $dc_field eq 'dc_de' ? 'dc_de_txt' : "$dc_field\_ss";
                foreach my $value ( @$values ) {
                    next unless ( $value );
                    push @fields, [ $index_key => $value ];
                }
            }
        }
        my $possible_maps = {};
        foreach my $field (@$browsefields) {
            my $sql = <<SQL;
SELECT * FROM ItemBrowseNext WHERE collid = ? AND field = ? AND idno = ?
SQL
            foreach my $row_ (
                @{
                    $dbh->selectall_arrayref( $sql, { Slice => {} },
                        $collid, $field, $$row{istruct_isentryid} )
                }
                )
            {
                my $value = $$row_{value};
                unless ( ref( $$possible_maps{$field} ) ) {
                    $$possible_maps{$field} = [];
                }

                # push @{ $$possible_maps{$field} }, $value;
                push @fields, [ "$field\_ss" => decode_entities($value) ];
            }
        }

        foreach my $field (@$srchflds) {
            next if ( $field eq 'ic_all' );
            my $value = $$row{$field};
            next unless ($value);
            foreach my $value_ ( split( /\|\|\|/, decode_entities($value) ) ) {
                next unless ( $value_ );
                push @fields, [ "$field\_txt" => $value_ ];
            }
        }

        # say Dumper($dc_map);
        # say Dumper(\@fields); exit;

        eval {
            $doc->add_fields(@fields);
        };
        if ( my $err = $@ ) {
            say $err;
            say Dumper(\@fields);
            exit;
        }
        # say $doc->to_xml;
        # $solr->add($doc);
        push @docs, $doc;
        say $$row{istruct_isentryid};

        # last;

    }

    $offset += $limit;
    $solr->add(\@docs);
    $solr->commit;
    @docs = ();
    say "-- committing $offset";

    # last;
}

$solr->commit;


say "-30-";