from hmm_api.utils.main import Conn
from Bio import SearchIO


class HmmSearch(object):
    """ HMMSearch class - parse hmm file and add to the database
    1. Read in HMM file using Bio SearchIO
    2. Iterate over queries
    3. Iterate over hits
    4. Iterate over hsps
    """

    def __init__(self,
            search_file='/home/jillian/Dropbox/projects/'
            'python/hmm_api/MWG01.TIGR00484.domtblout',
            search_type='hmmsearch3-domtab'):
        self.searchio = SearchIO.parse(search_file, search_type)
        self.sql = Conn()
        self.sample_id = None

    def create_dummy_sample(self):
        """ This is only here for testing purposes """
        pass

    ##TODO This should go someplace else
    def find_sample(self, sname):
        """ See if a sample name already exists """

        s = self.sql.sample.select()
        res = s.where(self.sql.sample.c.sample_name == qname)
        e = self.sql.connect.execute(res)
        row = e.fetchone()

        if row:
            return row['sample_id']
        else:
            return False

    def iter_results(self):
        """ Iterate over the entire hmm file """

        for qresult in self.searchio:
            self.iter_queries(qresult)

    def iter_queries(self, qresult):
        """ Iterate over the queries """

        qname = qresult.id
        qlen = qresult.seq_len
        hits = qresult.hits
        query_id = self.find_query(qname)

        if query_id:
            print('Query is id %s' % query_id)
            self.iter_hits(query_id, hits)
            pass

        try:
            i = self.sql.query.insert()
            res = i.execute(query_len=qlen, query_name=qname)
            query_id = res.lastrowid
        except Exception as e:
            if 'UNIQUE' in str(e):
                #TODO Get the row id for this query name
                pass
            else:
                #raise?
                print(" We encountered an error! Error is {}".format(e))
                pass

        self.iter_hits(query_id, hits)

    def find_query(self, qname):
        """ See if a query name already exists """

        s = self.sql.query.select()
        res = s.where(self.sql.query.c.query_name == qname)
        e = self.sql.connect.execute(res)
        row = e.fetchone()

        if row:
            return row['query_id']
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

        print("We are creating the hit %s" % fullname)

        try:
            i = self.sql.hit.insert()
            res = i.execute(hit_bitscore = bitscore, hit_evalue = evalue, hit_fullname = fullname, hit_len = slen, hit_frame = frame, hit_name = name)
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
            i = self.sql.hsp.insert()
            res = i.execute(hit_id = hit_id, hsp_bias = bias, hsp_bitscore = bitscore, hsp_evalue = evalue,
                    hsp_evalue_cond = evalue_cond, hit_from = hit_from,
                    hit_to = hit_to, hit_strand = hit_strand, query_from = query_from,
                    query_to = query_to, query_strand = query_strand)
            hsp_id = res.lastrowid
            print("HSP Id is {}".format(hsp_id))
        except Exception as e:
            print("We got an exception {}".format(str(e)))
