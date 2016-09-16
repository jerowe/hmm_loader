# from hmm_api.utils.main import Conn
from hmm_api.utils.deploy import Deploy
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
            search_type = 'hmmsearch3-domtab'):

        self.search_files = search_files
        self.search_type = search_type
        self.db_file = db_file
        self.sample_name = sample_name
        self.sample_id = None

        self.sql = Deploy(db_file, drop_db)
        self.create_sample()

    def iter_results(self):
        """ Iterate over the entire hmm file """

        for search_file in self.search_files:
            searchio = SearchIO.parse(search_file, self.search_type)

            for qresult in searchio:
                self.iter_queries(qresult)

        self.sql.connect.commit()

    def create_sample(self):
        """ Create a sample row """

        if self.sample_name is None:
            pass

        # trans = self.sql.connect.begin()

        sname = self.sample_name
        sample_id = self.find_sample()

        if not sample_id:
            try:
                res = self.sql.cursor.execute('INSERT INTO sample (sample_name) VALUES (?)', [sname])
                sample_id = res.lastrowid

                # i = self.sql.sample.insert()
                # res = i.execute(sample_name = sname)
                # sample_id = res.lastrowid
                # trans.commit()
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

        # s = self.sql.sample.select()
        # res = s.where(self.sql.sample.c.sample_name == self.sample_name)
        # e = self.sql.connect.execute(res)
        # row = e.fetchone()

        if row:
            # return row['sample_id']
            row[0]
        else:
            return False


    def iter_queries(self, qresult):
        """ Iterate over the queries """

        qname = qresult.id
        qlen = qresult.seq_len
        hits = qresult.hits
        query_id = self.find_query(qname)

        if query_id:
            # print('Query is id %s' % query_id)
            self.iter_hits(query_id, hits)
            pass

        try:
            res = self.sql.cursor.execute('INSERT INTO query (query_name) VALUES (?)', [qname])
            # sample_id = res.lastrowid
            # i = self.sql.query.insert()
            # res = i.execute(query_len=qlen, query_name=qname)
            query_id = res.lastrowid
        except Exception as e:
            print(" We encountered an error! Error is {}".format(e))
            pass

        self.iter_hits(query_id, hits)

    def find_query(self, qname):
        """ See if a query name already exists """

        # s = self.sql.query.select()
        # res = s.where(self.sql.query.c.query_name == qname)
        # e = self.sql.connect.execute(res)
        # row = e.fetchone()

        e = self.sql.cursor.execute('SELECT query_id from query WHERE query_name = (?)', [qname])
        row = e.fetchone()

        if row:
            # return row['query_id']
            return row[0]
        else:
            return False

    # hit_fullname TEXT,
    # hit_name TEXT,
    # hit_frame TEXT,
    # hit_len INTEGER,
    # sample_id INTEGER,
    # hit_bitscore REAL,
    # hit_evalue REAL,
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

        # print("We are creating the hit %s" % fullname)

        try:
            # i = self.sql.hit.insert()

            if self.sample_id is None:
                # res = i.execute(hit_bitscore = bitscore,
                        # hit_evalue = evalue, hit_fullname = fullname,
                        # hit_len = slen, hit_frame = frame, hit_name = name)

                res = self.sql.cursor.execute('INSERT INTO hit (hit_bitscore, hit_evalue, hit_fullname, hit_len, hit_frame, hit_name) VALUES (?,?,?,?,?,?)', [bitscore, evalue, fullname, slen, frame, name])
            else:
                # res = i.execute(sample_id = self.sample_id,
                        # hit_bitscore = bitscore, hit_evalue = evalue,
                        # hit_fullname = fullname, hit_len = slen,
                        # hit_frame = frame, hit_name = name)
                res = self.sql.cursor.execute('INSERT INTO hit (hit_bitscore, hit_evalue, hit_fullname, hit_len, hit_frame, hit_name, sample_id) VALUES (?,?,?,?,?,?,?)', [bitscore, evalue, fullname, slen, frame, name, self.sample_id])

            hit_id = res.lastrowid
        except Exception as e:
            print("We got an exception {}".format(str(e)))

        self.iter_hsp(hit_id, hsps)

    def iter_hsp(self, hit_id, hsps):
        """ Iterate over the HSPs """

        for hsp in hsps:
            self.create_hsp(hsp, hit_id)

    # hsp_bias REAL,
    # hsp_bitscore REAL,
    # hsp_evalue REAL,
    # hsp_evalue_cond REAL,
    # hit_from INTEGER,
    # hit_to INTEGER,
    # query_to INTEGER,
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
            # i = self.sql.hsp.insert()
            # res = i.execute(hit_id = hit_id, hsp_bias = bias, hsp_bitscore = bitscore, hsp_evalue = evalue,
                    # hsp_evalue_cond = evalue_cond, hit_from = hit_from,
                    # hit_to = hit_to, hit_strand = hit_strand, query_from = query_from,
                    # query_to = query_to, query_strand = query_strand)

            res = self.sql.cursor.execute('INSERT INTO hsp (hit_id, hsp_bias, hsp_bitscore, hsp_evalue, hsp_evalue_cond, hit_from, hit_to, hit_strand, query_from, query_to, query_strand) VALUES (?,?,?,?,?,?,?,?,?,?,?)', [hit_id, bias, bitscore, evalue, evalue_cond, hit_from, hit_to, hit_strand, query_from, query_to, query_strand])
            hsp_id = res.lastrowid
        except Exception as e:
            print("We got an exception {}".format(str(e)))
