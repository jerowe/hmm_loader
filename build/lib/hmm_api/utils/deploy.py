import sqlite3

class Deploy(object):

    def __init__(self, db_file='test.db', drop=False):
        self.drop = drop
        self.tables = ['hsp', 'hit', 'query', 'sample']
        self.db_file = db_file
        self.connect = sqlite3.connect(db_file)

    @property
    def cursor(self):
        return self.connect.cursor()

    def deploy_schema(self):

        if self.drop:
            self.drop_tables()

        self.deploy_samples()
        self.deploy_query()
        self.deploy_hit()
        self.deploy_hsp()

    def drop_tables(self):

        for table in self.tables:
            drop = "DROP TABLE IF EXISTS {}".format(table)
            self.cursor.execute(drop)

    def deploy_samples(self):
        """ Deploy samples table """
        self.cursor.execute('''
        /*
        Metagenomics sqlite3 DB
        */

        /*
        Samples - to be shared across hmm, blast, etc
        */

        CREATE TABLE IF NOT EXISTS sample(
            sample_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            sample_name TEXT,
            UNIQUE(sample_name)
        );
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fsample ON sample (sample_id);
        ''')

    def deploy_query(self):
        ''' Deploy query table
        Queries are the hmm databases - in our case
        publically available TIGRFAM
        '''
        self.cursor.execute('''
        /*
        Query - public databases
        */
        CREATE TABLE IF NOT EXISTS query(
            query_id      INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            query_len     INTEGER,
            query_name    TEXT,
            UNIQUE(query_name)
        );
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fquery ON query (query_id);
        ''')

    def deploy_hit(self):

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS hit(
            hit_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            sample_id INTEGER,
            hit_fullname TEXT,
            hit_name TEXT,
            hit_frame TEXT,
            hit_len INTEGER,
            hit_bitscore REAL,
            hit_evalue REAL,
            UNIQUE(sample_id, hit_fullname, hit_name),
            FOREIGN KEY (sample_id) REFERENCES sample (sample_id)
        );
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhit ON hit (hit_id);
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhit_hit_sample ON hit (hit_id, sample_id);
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhit_hit_evalue ON hit (hit_id, hit_evalue);
        ''')

    def deploy_hsp(self):

        self.cursor.execute('''
        /*
        HSP  - Highest scoring pairs
        */
        CREATE TABLE IF NOT EXISTS hsp(
            hsp_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            hit_id INTEGER,
            hsp_bias REAL,
            hsp_bitscore REAL,
            hsp_evalue REAL,
            hsp_evalue_cond REAL,
            hit_from INTEGER,
            hit_to INTEGER,
            hit_strand INTEGER,
            query_from INTEGER,
            query_to INTEGER,
            query_strand INTEGER,
            FOREIGN KEY (hit_id) REFERENCES hit (hit_id)
        );
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhsp ON hsp (hsp_id);
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhsp_hit ON hsp (hit_id);
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhsp_hsp_hit ON hsp (hsp_id, hit_id);
        ''')
        self.cursor.execute('''
        CREATE INDEX IF NOT EXISTS Fhsp_hsp_evalue ON hsp (hsp_id, hsp_evalue);
        ''')
