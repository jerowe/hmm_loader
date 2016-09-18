from hmm_api.utils.main import Conn
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
            db_drive  = 'sql'):

        self.search_files = search_files
        self.search_type = search_type
        self.db_file = db_file
        self.db_drive = db_drive
        self.sample_name = sample_name

        self.sample_id = None

        if db_drive == 'sql':
            self.sql = Deploy(db_file, drop_db)
            self.sql.connect.row_factory = sqlite3.Row
        else:
            self.sql = Conn(db_file, drop_db)

        self.create_sample()

    def iter_results(self):
        """ Iterate over the entire hmm file """

        if self.db_drive == 'orm':
            trans = self.sql.connect.begin()

        for search_file in self.search_files:
            searchio = SearchIO.parse(search_file, self.search_type)

            for qresult in searchio:
                self.iter_queries(qresult)


        if self.db_drive == 'orm':
            trans.commit()
        else:
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
                if self.db_drive == 'sql':
                    res = self.sql.cursor.execute('INSERT INTO sample (sample_name) VALUES (?)', [sname])
                else:
                    i = self.sql.sample.insert()
                    res = i.execute(sample_name = sname)

                sample_id = res.lastrowid
            except Exception as e:
                print("Exception {}".format(e))
                # trans.rollback()
                raise

        self.sample_id = sample_id

    ##TODO This should go someplace else
    def find_sample(self):
        """ See if a sample name already exists """

        # Sqlite
        if self.db_drive == 'sql':
            e = self.sql.cursor.execute('SELECT sample_id from sample WHERE sample_name = (?)', [self.sample_name])
        else:
        # SqlAlchemy
            s = self.sql.sample.select()
            res = s.where(self.sql.sample.c.sample_name == self.sample_name)
            e = self.sql.connect.execute(res)

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
                # Sqlite
                if self.db_drive == 'sql':
                    res = self.sql.cursor.execute('INSERT INTO query (query_name, query_len) VALUES (?, ?)', [qname, qlen])
                else:
                # SqlAlchemy
                    i = self.sql.query.insert()
                    res = i.execute(query_len=qlen, query_name=qname)

                query_id = res.lastrowid
            except Exception as e:
                print(" We encountered an error! Error is {}".format(e))
                pass

        self.iter_hits(query_id, hits)

    def find_query(self, qname):
        """ See if a query name already exists """

        # Sqlite
        if self.db_drive == 'sql':
            e = self.sql.cursor.execute('SELECT query_id from query WHERE query_name = (?)', [qname])
        else:
        # SqlAlchemy
            s = self.sql.query.select()
            res = s.where(self.sql.query.c.query_name == qname)
            e = self.sql.connect.execute(res)

        row = e.fetchone()
        if row:
            return row['query_id']
        else:
            return False

    def iter_hits(self, query_id, hits):
        """ Iterate over the hits """

        for hit in hits:
            self.create_hit(hit)

    def create_hit(self, hit):
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

        try:

            if self.sample_id is None:

                # Sqlite
                if self.db_drive == 'sql':
                    res = self.sql.cursor.execute('INSERT INTO hit (hit_bitscore,'
                                                  ' hit_evalue, hit_fullname, hit_len,'
                                                  ' hit_frame, hit_name) VALUES (?,?,?,?,?,?)',
                                                  [bitscore, evalue, fullname, slen, frame, name])
                else:
                # SqlAlchemy
                    i = self.sql.hit.insert()
                    res = i.execute(hit_bitscore = bitscore,
                            hit_evalue = evalue, hit_fullname = fullname,
                            hit_len = slen, hit_frame = frame, hit_name = name)

            else:
                if self.db_drive == 'sql':
                # Sqlite
                    res = self.sql.cursor.execute(' INSERT INTO hit (hit_bitscore, '
                                                  ' hit_evalue, hit_fullname, hit_len,'
                                                  ' hit_frame, hit_name, sample_id) '
                                                  ' VALUES (?,?,?,?,?,?,?) ',
                                                  [bitscore, evalue, fullname, slen, frame, name, self.sample_id])
                else:
                # SqlAlchemy
                    i = self.sql.hit.insert()
                    res = i.execute(sample_id = self.sample_id,
                            hit_bitscore = bitscore, hit_evalue = evalue,
                            hit_fullname = fullname, hit_len = slen,
                            hit_frame = frame, hit_name = name)

            hit_id = res.lastrowid
        except Exception as e:
            print("We got an exception {}".format(str(e)))

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

            if self.db_drive == 'sql':
                self.sql.cursor.execute('INSERT INTO hsp (hit_id, hsp_bias,'
                        'hsp_bitscore, hsp_evalue, hsp_evalue_cond, hit_from,   '
                        'hit_to, hit_strand, query_from, query_to, query_strand) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                        [hit_id, bias, bitscore, evalue, evalue_cond, hit_from,
                            hit_to, hit_strand, query_from, query_to, query_strand])
            else:
                i = self.sql.hsp.insert()
                i.execute(hit_id = hit_id, hsp_bias = bias, hsp_bitscore = bitscore, hsp_evalue = evalue,
                        hsp_evalue_cond = evalue_cond, hit_from = hit_from,
                        hit_to = hit_to, hit_strand = hit_strand, query_from = query_from,
                        query_to = query_to, query_strand = query_strand)

        except Exception as e:
            print("We got an exception {}".format(str(e)))
