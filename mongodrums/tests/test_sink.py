import pymongo
import random

import mongodrums.instrument

from . import BaseTest
from mongodrums.collection import IndexProfileCollection, QueryProfileCollection
from mongodrums.config import get_config, update
from mongodrums.instrument import instrument
from mongodrums.sink import IndexProfileSink, QueryProfileSink


class ProfileSinkTest(BaseTest):
    SINK_TEST_DB = 'mongodrums_index_profile_test'

    def setUp(self):
        super(ProfileSinkTest, self).setUp()
        self.sink_db = self.client[self.__class__.SINK_TEST_DB]
        self._setup_test_collection()
        self._real_push = mongodrums.instrument.push
        self._msgs = []
        mongodrums.instrument.push = self._push
        update({
            'instrument': {'sample_frequency': 1},
            'index_profile_sink': {
                'mongo_uri': 'mongodb://127.0.0.1:27017/%s' %
                             (self.__class__.SINK_TEST_DB)
            },
            'query_profile_sink': {
                'mongo_uri': 'mongodb://127.0.0.1:27017/%s' %
                             (self.__class__.SINK_TEST_DB)
            }
        })
        self._index_profile_sink = IndexProfileSink()
        self._query_profile_sink = QueryProfileSink()

    def tearDown(self):
        super(ProfileSinkTest, self).tearDown()
        mongodrums.instrument.push = self._real_push
        self.client.drop_database(self.__class__.SINK_TEST_DB)

    def _push(self, msg):
        msg['session'] = 'test'
        self._msgs.append(msg)

    def _setup_test_collection(self):
        self.db.foo.ensure_index([('store', pymongo.ASCENDING),
                                  ('widget', pymongo.ASCENDING),
                                  ('sold', pymongo.DESCENDING)])
        self.db.foo.ensure_index([('store', pymongo.ASCENDING),
                                  ('widget', pymongo.ASCENDING),
                                  ('in_stock', pymongo.ASCENDING)])
        sold = range(1000)
        random.shuffle(sold)
        in_stock = range(1000)
        random.shuffle(in_stock)
        stores = ['store_%d' % (i) for i in xrange(10)]
        widgets = ['widget_%d' % (i) for i in xrange(100)]
        docs = []
        for i in xrange(len(stores)):
            for j in xrange(len(widgets)):
                docs.append({'store': stores[i],
                             'widget': widgets[j],
                             'sold': sold[len(widgets) * i + j],
                             'in_stock': in_stock[len(widgets) * i + j]})
        self.db.foo.insert(docs)

    def test_find_explain_logging(self):
        with instrument():
            count = self.db.foo.find({'sold': {'$gt': 100}}).count()
            self.assertEqual(count, 899)
            self.assertEqual(len(self._msgs), 2)
            count = self.db.foo.find().count()
            self.assertEqual(count, 1000)
            self.assertEqual(len(self._msgs), 4)
        for msg in self._msgs:
            self._index_profile_sink.handle(msg, ('127.0.0.1', 65535))
            self._query_profile_sink.handle(msg, ('127.0.0.1', 65535))
        query_profile_col = QueryProfileCollection.get_collection_name()
        index_profile_col = IndexProfileCollection.get_collection_name()
        self.assertEqual(self.sink_db[query_profile_col].find().count(), 2)
        self.assertEqual(self.sink_db[index_profile_col].find().count(), 1)

    def test_update_explain_logging(self):
        with instrument():
            ret = self.db.foo.update({'store': 'store_0'},
                                      {'$inc': {'sold': 1},
                                       '$inc': {'in_stock': -1}},
                                      multi=True)
            self.assertEqual(ret['n'], 100)
            self.assertEqual(len(self._msgs), 1)
        for msg in self._msgs:
            self._index_profile_sink.handle(msg, ('127.0.0.1', 65535))
            self._query_profile_sink.handle(msg, ('127.0.0.1', 65535))
        query_profile_col = QueryProfileCollection.get_collection_name()
        index_profile_col = IndexProfileCollection.get_collection_name()
        self.assertEqual(self.sink_db[query_profile_col].find().count(), 1)
        self.assertEqual(self.sink_db[index_profile_col].find().count(), 1)

