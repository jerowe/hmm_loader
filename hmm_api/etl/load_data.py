from hmm_api.utils.deploy import Deploy
import sqlite3
from Bio import SearchIO

class HmmSearch(object):
    """ HMMSearch class - parse hmm file and add to the database
    Optionally there is a field for sample_id
    1. Read in HMM file using Bio SearchIO
    2. Iterate over queries
    3. Iterate over hits
    4. Iterate over hsps
    """

    def __init__(self,
            sample_name = None,
            search_files = [],
            db_file = 'hmm.db',
            drop_db = False,
            search_type = 'hmmsearch3-domtab',
            ):

        self.search_files = search_files
        self.search_type = search_type
        self.db_file = db_file
        self.sample_name = sample_name

        self.sample_id = None

        self.sql = Deploy(db_file, drop_db)
        self.sql.connect.row_factory = sqlite3.Row

        self.create_sample()

    def iter_results(self):
        """ Iterate over the entire hmm file """

        for search_file in self.search_files:
            searchio = SearchIO.parse(search_file, self.search_type)

            for qresult in searchio:
                self.iter_queries(qresult)


        self.sql.connect.commit()
        self.sql.connect.close()

    def create_sample(self):
        """ Create a sample row """

        if self.sample_name is None:
            pass

        sname = self.sample_name
        sample_id = self.find_sample()

        if not sample_id:
            try:
                res = self.sql.cursor.execute('INSERT INTO sample (sample_name) VALUES (?)', [sname])

                sample_id = res.lastrowid
            except Exception as e:
                print("Exception {}".format(e))
                # trans.rollback()
                raise

        self.sample_id = sample_id

    ##TODO This should go someplace else
    def find_sample(self):
        """ See if a sample name already exists """

        e = self.sql.cursor.execute('SELECT sample_id from sample WHERE sample_name = (?)', [self.sample_name])

        row = e.fetchone()

        if row:
            return row['sample_id']
        else:
            return False


    def iter_queries(self, qresult):
        """ Iterate over the queries """

        qname = qresult.id
        qlen = qresult.seq_len
        hits = qresult.hits
        query_id = self.find_query(qname)

        if not query_id:
            try:
                res = self.sql.cursor.execute('INSERT INTO query (query_name, query_len) VALUES (?, ?)', [qname, qlen])

                query_id = res.lastrowid
            except Exception as e:
                print(" We encountered an error! Error is {}".format(e))
                pass

        self.iter_hits(query_id, hits)

    def find_query(self, qname):
        """ See if a query name already exists """

        e = self.sql.cursor.execute('SELECT query_id from query WHERE query_name = (?)', [qname])

        row = e.fetchone()
        if row:
            return row['query_id']
        else:
            return False

    def iter_hits(self, query_id, hits):
        """ Iterate over the hits """

        for hit in hits:
            self.create_hit(query_id, hit)

    def create_hit(self, query_id, hit):
        """ Create the hit row in the DB """

        #TODO Add a find or create method
        bitscore = hit.bitscore
        evalue = hit.evalue
        fullname = hit.id
        slen = hit.seq_len
        l = fullname.split(':')
        frame = l.pop()
        name = ':'.join(l)

        hsps = hit.hsps
        hit_id = None

        try:

            if self.sample_id is None:

                res = self.sql.cursor.execute('INSERT INTO hit (hit_bitscore, query_id,'
                                                ' hit_evalue, hit_fullname, hit_len,'
                                                ' hit_frame, hit_name) VALUES (?,?,?,?,?,?,?)',
                                                [bitscore, query_id, evalue, fullname, slen, frame, name])
                hit_id = res.lastrowid
            else:
                res = self.sql.cursor.execute(' INSERT INTO hit (hit_bitscore, query_id, '
                                                ' hit_evalue, hit_fullname, hit_len,'
                                                ' hit_frame, hit_name, sample_id) '
                                                ' VALUES (?,?,?,?,?,?,?,?) ',
                                                [bitscore, query_id, evalue, fullname, slen, frame, name, self.sample_id])
                hit_id = res.lastrowid
        except Exception as e:
            print("We got an exception {}".format(str(e)))
            raise

        self.iter_hsp(hit_id, hsps)

    def iter_hsp(self, hit_id, hsps):
        """ Iterate over the HSPs """

        for hsp in hsps:
            self.create_hsp(hsp, hit_id)

    def create_hsp(self, hsp, hit_id):
        """Create the HSP entry """

        bias = hsp.bias
        bitscore = hsp.bitscore
        evalue = hsp.evalue
        evalue_cond = hsp.evalue_cond
        hit_from = hsp.hit_start
        hit_to = hsp.hit_end
        hit_strand = hsp.hit_strand
        query_from = hsp.query_start
        query_to = hsp.query_end
        query_strand = hsp.query_strand

        try:

            self.sql.cursor.execute('INSERT INTO hsp (hit_id, hsp_bias,'
                    'hsp_bitscore, hsp_evalue, hsp_evalue_cond, hit_from,   '
                    'hit_to, hit_strand, query_from, query_to, query_strand) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    [hit_id, bias, bitscore, evalue, evalue_cond, hit_from,
                        hit_to, hit_strand, query_from, query_to, query_strand])

        except Exception as e:
            print("We got an exception {}".format(str(e)))
