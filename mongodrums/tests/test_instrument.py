import inspect

import pymongo

from mock import patch

from . import BaseTest
from mongodrums.instrument import (
    _CursorMethodWrapper, _CursorNextWrapper, UpdateWrapper, FindWrapper,
    start, stop, instrument, instrumented
)

from mongodrums.config import update


class InstrumentTest(BaseTest):
    def setUp(self):
        super(InstrumentTest, self).setUp()
        self.db.foo.insert([{'_id': 1, 'name': 'bob'},
                            {'_id': 2, 'name': 'alice'},
                            {'_id': 3, 'name': 'zed'},
                            {'_id': 4, 'name': 'yohan'}])
        self.db.foo.ensure_index('name')

    def test_instrument_update(self):
        update = pymongo.collection.Collection.update
        UpdateWrapper.wrap()
        try:
            self.assertNotEqual(pymongo.collection.Collection.update, update)
            self.assertIsInstance(pymongo.collection.Collection.update, UpdateWrapper)
        finally:
            UpdateWrapper.unwrap()
        self.assertEqual(pymongo.collection.Collection.update, update)
        self.assertNotIsInstance(pymongo.collection.Collection.update, UpdateWrapper)

    def test_instrument_find(self):
        find = pymongo.collection.Collection.find
        FindWrapper.wrap()
        try:
            self.assertNotEqual(pymongo.collection.Collection.find, find)
            self.assertIsInstance(pymongo.collection.Collection.find, FindWrapper)
        finally:
            FindWrapper.unwrap()
        self.assertEqual(pymongo.collection.Collection.find, find)
        self.assertNotIsInstance(pymongo.collection.Collection.find,
                                 FindWrapper)

    def test_chained_call(self):
        update({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            # chain call
            curs = self.db.foo.find({'name': 'bob'}).limit(1)
            self.assertEqual(push_mock.call_count, 0)
            self.assertTrue(hasattr(curs.next, 'func'))
            self.assertIsInstance(curs.next.func, _CursorNextWrapper)
            # use iter instead of direct call to next
            doc = [d for d in curs][0]
            self.assertEqual(push_mock.call_count, 1)
            self.assertEqual(doc, {'_id': 1, 'name': 'bob'})
            self.assertIn('allPlans', push_mock.call_args[0][0]['explain'])
        self.assertNotIsInstance(pymongo.collection.Collection.find,
                                 FindWrapper)
        self.assertNotIsInstance(self.db.foo.find,
                                 FindWrapper)

    def test_or_query(self):
        update({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            docs = [d for d in self.db.foo.find({'$or': [{'name': 'bob'},
                                                         {'name': 'alice'}]},
                                                {'name': 1})]
            self.assertEqual(len(docs), 2)
            self.assertItemsEqual(docs, [{'_id': 1, 'name': 'bob'},
                                         {'_id': 2, 'name': 'alice'}])
            self.assertEqual(push_mock.call_count, 1)

    def test_find_push(self):
        update({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            doc = self.db.foo.find_one({'name': 'bob'})
            self.assertEqual(doc, {'_id': 1, 'name': 'bob'})
            self.assertEqual(push_mock.call_count, 1)
            self.assertIn('allPlans', push_mock.call_args[0][0]['explain'])
        self.assertNotIsInstance(pymongo.collection.Collection.find,
                                 FindWrapper)
        self.assertNotIsInstance(self.db.foo.find,
                                 FindWrapper)

    def test_config_update(self):
        with instrument():
            self.assertEqual(pymongo.collection.Collection.find._frequency,
                             self.saved_config.instrument.sample_frequency)
            update({'instrument': {'sample_frequency': 1}})
            self.assertEqual(pymongo.collection.Collection.find._frequency, 1)

    def test_get_source(self):
        update({'instrument': {'sample_frequency': 1}})
        with patch('mongodrums.instrument.push') as push_mock, \
             FindWrapper.instrument():
            doc = self.db.foo.find_one({'name': 'bob'})
            frame_info = inspect.getframeinfo(inspect.currentframe())
            source = '%s:%d' % (frame_info[0], frame_info[1] - 1)
            self.assertEqual(push_mock.call_args[0][0]['source'], source)

    def test_instrumented(self):
        self.assertFalse(instrumented())
        with instrument():
            self.assertTrue(instrumented())
        self.assertFalse(instrumented())

    def test_explain_type_error(self):
        docs = []
        with patch('pymongo.cursor.Cursor.explain') as explain_mock, \
             patch('mongodrums.instrument.push') as push_mock:
            explain_mock.side_effect = TypeError()
            push_mock.side_effect = docs.append
            with instrument():
                self.db.foo.find({'name': 'zed'})
                self.db.foo.update({'name': 'zed'}, {'$set': {'age': 40}})
        for doc in docs:
            self.assertIn('error', doc['explain'])
