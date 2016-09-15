from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from hmm_api.utils.deploy import Deploy


class Sample(object):
    """Table sample in db """
    pass


class Query(object):
    """Table query in db """
    pass


class Hit(object):
    """Table hit in db """
    pass


class HSP(object):
    """Table hsp in db """
    pass


class Conn(object):
    """ Connect to the DB using the SqlAlchemy DB """

    def __init__(self, db_file='test.db'):
        self.db_file = db_file
        self.sample = None
        self.query = None
        self.hit = None
        self.hsp = None
        self.map_meta()

    @property
    def engine(self):
        return create_engine('sqlite:///%s' % self.db_file, echo=False)

    @property
    def connect(self):
        return self.engine.connect()

    @property
    def session(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        return session

    def ensure_deploy(self):
        """ Ensure we have deployed the schema """

        #Change this to false when not testing
        d = Deploy(self.db_file, True)
        d.deploy_schema()

    def map_sample(self, metadata):

        try:
            sample  = Table('sample', metadata, autoload=True)
            mapper(Sample, sample)
            self.sample = sample
        except:
            pass

    def map_query(self, metadata):

        try:
            query  = Table('query', metadata, autoload=True)
            mapper(Query, query)
            self.query = query
        except:
            pass

    def map_hit(self, metadata):

        try:
            hit  = Table('hit', metadata, autoload=True)
            mapper(Hit, hit)
            self.hit = hit
        except:
            pass

    def map_hsp(self, metadata):

        try:
            hsp  = Table('hsp', metadata, autoload=True)
            mapper(HSP, hsp)
            self.hsp = hsp
        except:
            pass

    def map_meta(self):
        """ Map the table into the sqlalchemy expected classes """

        self.ensure_deploy()

        metadata = MetaData(self.engine)

        self.map_sample(metadata)
        self.map_query(metadata)
        self.map_hit(metadata)
        self.map_hsp(metadata)
