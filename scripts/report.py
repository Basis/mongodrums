import argparse
import copy
import logging
import json
import os
import re
import sys
import traceback

from urlparse import urlparse

from pymongo import MongoClient

from mongodrums.collection import (
    SessionCollection, IndexProfileCollection, QueryProfileCollection
)
from mongodrums.util import get_default_database


_DEFAULT_URI = 'mongodb://localhost:27017/mongodrums'
_INDEXES_TO_SKIP = [re.compile(r'BasicCursor.*'),
                    re.compile(r'(BtreeCursor )?_id_( .+)?')]


def _get_size(bytes_, unit):
    conv= {'b': ('bytes', 2**0),
           'k': ('Kbs', 2**10),
           'm': ('Mbs', 2**20),
           'g': ('Gbs', 2**30)}
    if unit not in conv.keys():
        raise ValueError('bad unit: %s' % (unit))
    return '%d %s' % (bytes_ / conv[unit][1], conv[unit][0])


class Report(object):
    def __init__(self, database, current_indexes, output_stream, session=None,
                 unit='m'):
        self._database = database
        self._current_indexes = current_indexes
        self._output_stream = output_stream
        self._session = session
        self._unit = unit

    def build(self):
        index_col = \
            IndexProfileCollection(
                self._database[IndexProfileCollection.get_collection_name()])
        query_col = \
            QueryProfileCollection(
                self._database[QueryProfileCollection.get_collection_name()])
        query = {} if self._session is None else {'session': self._session}
        for doc in index_col.find_iter(query):
            if any([r.match(doc['index']) for r in _INDEXES_TO_SKIP]):
                continue
            logging.debug('working on index %s in collection %s' %
                          (doc['index'], doc['collection']))
            index_name = doc['index'].split()[1]
            try:
                stats = self._current_indexes[doc['collection']].get('__stats',
                                                                     None)
                index = self._current_indexes[doc['collection']][index_name]
                index['query_count'] = len(doc['queries'])
                index['used_count'] = sum([q['count'] for q in doc['queries']])
                index['queries'] = dict([(q['query'], {}) for q in doc['queries']])
                if stats is not None:
                    index['total_size'] = stats['indexSizes'][index_name]
                    try:
                        index['index_size_ratio'] = (float(index['total_size']) /
                                                     stats['totalIndexSize'])
                    except ZeroDivisionError:
                        index['index_size_ratio'] = float(index['total_size'])
                    try:
                        index['collection_size_ratio'] = \
                            float(index['total_size']) / stats['size']
                    except ZeroDivisionError:
                        index['collection_size_ratio'] = float(index['total_size'])
                    # FIXME: make this more meaningful
                    index['removal_score'] = index['index_size_ratio'] * \
                                             index['collection_size_ratio']

                for q in doc['queries']:
                    logging.debug('gathering query informatin for index %s, ' \
                                  'query %s' % (doc['index'], q['query']))
                    query = {'query': q['query'],
                             'collection': doc['collection'],
                             'explain.cursor': doc['index']}
                    if self._session is not None:
                        query.update({'session': self._session})
                    for query_doc in query_col.find_iter(query):
                        queries = \
                            self._current_indexes[doc['collection']][index_name]['queries']
                        if query_doc['source'] not in queries[q['query']]:
                            queries[q['query']][query_doc['source']] = 0
                        queries[q['query']][query_doc['source']] += 1

            except KeyError:
                logging.warning('skipping index %s on collection %s:\n%s' %
                                (index_name, doc['collection'],
                                 traceback.format_exc()))

    def _print(self, str_):
        self._output_stream.write(str_ + '\n')

    def _prep_current_indexes_for_dump(self):
        indexes = copy.deepcopy(self._current_indexes)
        for col in indexes:
            indexes[col].pop('__stats', None)
            for key in filter(lambda k: any([r.match(k) for r in
                                             _INDEXES_TO_SKIP]),
                              indexes.keys()):
                indexes.pop(key)
        return indexes

    def dump_mark_down(self):
        self._print('# index use by collection')
        indexes = self._prep_current_indexes_for_dump()
        for col in sorted(indexes.keys()):
            self._print('\n## %s' % (col))
            sort_key = \
                'removal_score' \
                if any([i.get('removal_score', 0) > 0
                        for i in indexes[col].values()]) \
                else 'used_count'

            for name, index in sorted(indexes[col].items(),
                                      key=lambda x: x[1].get(sort_key, 0),
                                      reverse=True):
                if any([r.match(name) for r in _INDEXES_TO_SKIP]) or \
                   name == '__stats':
                    continue
                self._print('\n### `%s`' % (name))
                if index.get('used_count', 0) == 0:
                    self._print('* used *NEVER*')
                else:
                    self._print('* used %d times by %d quer%s' %
                                (index['used_count'], index['query_count'],
                                 ['y', 'ies'][index['query_count'] > 0]))
                    for q in index['queries']:
                        self._print('    * %s' % (q))
                        for line in index['queries'][q]:
                            self._print('        * %s hit %d times' %
                                        (line, index['queries'][q][line]))
                self._print('* removal score is %.03f' % (index['removal_score']))
                try:
                    index_size_ratio = '%.03f' % index['index_size_ratio']
                except TypeError:
                    index_size_ratio = index['index_size_ratio']
                self._print('* index size ratio is %s' % (index_size_ratio))
                try:
                    collection_size_ratio = \
                        '%.03f' % (index['collection_size_ratio'])
                except TypeError:
                    collection_size_ratio = index['collection_size_ratio']
                self._print('* collection size ratio is %s' %
                            (collection_size_ratio))
                try:
                    total_size = _get_size(index['total_size'], self._unit)
                except TypeError:
                    total_size = index['total_size']
                self._print('* total size is %s' % (total_size))
            self._print('\n---\n')

    def dump_json(self):
        """
        indexes = copy.deepcopy(self._current_indexes
        self._print(json.dumps(self.
        """
        pass


def load_indexes(uri):
    def _organize(index_dump):
        indexes = {}
        for entry in index_dump:
            if any([r.match(entry['name']) for r in _INDEXES_TO_SKIP]):
                continue
            col = entry['ns'].split('.')[-1]
            if col not in indexes:
                indexes[col] = {}
            indexes[col][entry['name']] = entry
            indexes[col][entry['name']]['total_size'] = 'N/A'
            indexes[col][entry['name']]['index_size_ratio'] = 'N/A'
            indexes[col][entry['name']]['collection_size_ratio'] = 'N/A'
            indexes[col][entry['name']]['removal_score'] = 0
        return indexes
    parts = urlparse(uri)
    indexes = None
    if parts.scheme == 'file':
        indexes = _organize(json.loads(open(parts.path).read()))
    elif parts.scheme == 'mongodb':
        client = MongoClient(uri)
        database = get_default_database(client, uri)
        indexes = _organize([i for i in database['system.indexes'].find()])
        for col in indexes:
            indexes[col]['__stats'] = database.command('collstats', col)
    else:
        raise ValueError('unknown source_uri scheme %s' % (parts.scheme))
    return indexes


def run_report(args):
    client = MongoClient(args.uri)
    database = get_default_database(client, args.uri)
    indexes = load_indexes(args.source_uri)
    stream = sys.stdout if args.out is None else open(args.out, 'w')
    report = Report(database, indexes, stream, args.session)
    report.build()

    if args.type == 'markdown':
        report.dump_mark_down()
    elif args.type == 'json':
        report.dump_json()

    stream.close()


def main():
    parser = argparse.ArgumentParser(description='generate a report from '
                                                 'instrumentation data')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='log debug output [default: %(default)s]')
    parser.add_argument('-s', '--session', metavar='SESSION',
                        help='the instrumentation session to report on '
                             '[default: <all sessions>]')
    parser.add_argument('-o', '--out', metavar='PATH',
                        help='where to write the report [default: stdout]')
    parser.add_argument('-u', '--uri', metavar='URI', default=_DEFAULT_URI,
                        help='mongod/database to connect to (-d overrides the '
                             'database in the uri) [default: %(default)s]')
    parser.add_argument('-t', '--type', metavar='TYPE', default='json',
                        choices=['markdown', 'json'],
                        help='the type of report you want to generate '
                             '[default: %(default)s, choices: %(choices)s]')
    parser.add_argument('source_uri', metavar='SOURCE_URI',
                        help='the source database uri or file uri to load '
                             'current index state from (if a file is uri '
                             'is given, the file should be in the format '
                             'produced by "dump_indexes.js"')
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    run_report(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())

