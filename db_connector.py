import os
import pandas as pd

class DBConnector(object):
    def __init__(self):
        self.card_db = None
        self.alias_db = None

    def load(self, card_db_path, alias_db_path):
        assert(os.path.exists(card_db_path))
        assert (os.path.exists(alias_db_path))
        self.card_db = pd.read_hdf(card_db_path)
        self.alias_db = pd.read_hdf(alias_db_path)

    def query_info(self, query):
        assert(self.card_db is not None)
        query_str = []
        for k, v in query.items():
            query_str.append('%s == \"%s\"' % (k , v))
        ret = self.card_db.query(' & '.join(query_str))
        return ret