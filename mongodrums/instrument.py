import logging
import random
import socket
import Queue
import threading
import traceback

from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from functools import partial, update_wrapper
from types import MethodType
from weakref import WeakSet

import pymongo

from bson.json_util import dumps
from bunch import Bunch
from pymongo.cursor import Cursor
from pymongo.errors import OperationFailure

from .config import (
    configure, get_config, register_update_callback, unregister_update_callback
)
from .pusher import push
from .util import get_source


class Wrapper(object):
    __metaclass__ = ABCMeta

    _lock = threading.RLock()

    def __init__(self, func):
        self._lock = threading.RLock()
        self._func = func
        self._configure(get_config())

    def _configure(self, config):
        self._frequency = config.instrument.sample_frequency
        self._filter_packages = config.instrument.filter_packages

    def __get__(self, owner, owner_type):
        if owner is None:
            return self
        return partial(self, owner)

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    @classmethod
    @abstractmethod
    def wrap(cls):
        pass

    @classmethod
    @abstractmethod
    def unwrap(cls):
        pass

    @classmethod
    @contextmanager
    def instrument(cls):
        cls.wrap()
        try:
            yield
        finally:
            cls.unwrap()


class _CursorMethodWrapper(Wrapper):
    _ids = WeakSet()
    _ids_lock = threading.RLock()
    _method_name = None

    def __new__(cls, *args, **kwargs):
        if cls._method_name is None:
            raise RuntimeError('_method_name must be declared')
        return super(_CursorMethodWrapper, cls).__new__(cls, *args, **kwargs)

    @classmethod
    def track_cursor(cls, cursor):
        with cls._ids_lock:
            cls._ids.add(cursor)

    def __call__(self, self_, *args, **kwargs):
        with self.__class__._ids_lock:
            tracking = self_ in self.__class__._ids
        if tracking:
            with self.__class__._ids_lock:
                assert(self_ in self.__class__._ids)
                self.__class__._ids.discard(self_)
            try:
                explain = self_.explain()
            except (TypeError, OperationFailure):
                explain = {'error': traceback.format_exc()}
                logging.exception('error trying to run explain on curs')
            try:
                push({'type': 'explain',
                      'function': 'find',
                      'database': self_.collection.database.name,
                      'collection': self_.collection.name,
                      'query': dumps(args[0] if len(args) > 0 else {},
                                     sort_keys=True),
                      'explain': explain,
                      'source': get_source(self._filter_packages)})
            except Exception:
                logging.exception('exception pushing explain data for find')
        return self._func(self_, *args, **kwargs)

    @classmethod
    def wrap(cls):
        with cls._lock:
            meth = getattr(pymongo.cursor.Cursor, cls._method_name)
            if not isinstance(meth, cls):
                meth = cls(meth)
                setattr(pymongo.cursor.Cursor, cls._method_name, meth)
                register_update_callback(meth._configure)
        return meth

    @classmethod
    def unwrap(cls):
        with cls._lock:
            meth = getattr(pymongo.cursor.Cursor, cls._method_name)
            if isinstance(meth, cls):
                unregister_update_callback(meth._configure)
                setattr(pymongo.cursor.Cursor, cls._method_name, meth._func)


class _CursorNextWrapper(_CursorMethodWrapper):
    _method_name = 'next'


class _CursorCountWrapper(_CursorMethodWrapper):
    _method_name = 'count'


class _CursorDistinctWrapper(_CursorMethodWrapper):
    _method_name = 'distinct'


_cursor_terminators = [_CursorNextWrapper, _CursorCountWrapper,
                       _CursorDistinctWrapper]


class FindWrapper(Wrapper):
    def __init__(self, func):
        super(FindWrapper, self).__init__(func)
        self._cursor_wrappers = None

    def __call__(self, self_, *args, **kwargs):
        curs = self._func(self_, *args, **kwargs)
        if random.random() < self._frequency:
            assert(self._cursor_wrappers is not None)
            _CursorMethodWrapper.track_cursor(curs)
        return curs

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.find, cls):
                instance = cls(pymongo.collection.Collection.find)
                register_update_callback(instance._configure)
                instance._cursor_wrappers = []
                for cursor_wrapper in _cursor_terminators:
                    instance._cursor_wrappers.append(cursor_wrapper.wrap())
                pymongo.collection.Collection.find = instance
        return pymongo.collection.Collection.find

    @classmethod
    def unwrap(cls):
        with cls._lock:
            if isinstance(pymongo.collection.Collection.find, cls):
                instance = pymongo.collection.Collection.find
                for cursor_wrapper in instance._cursor_wrappers:
                    cursor_wrapper.unwrap()
                instance._cursor_wrappers = None
                unregister_update_callback(instance._configure)
                pymongo.collection.Collection.find = instance._func


class UpdateWrapper(Wrapper):
    def __call__(self, self_, *args, **kwargs):
        if random.random() < self._frequency:
            curs = self_.find(args[0])
            try:
                explain = curs.explain()
            except (TypeError, OperationFailure):
                explain = {'error': traceback.format_exc()}
                logging.exception('error trying to run explain on curs')
            try:
                push({'type': 'explain',
                      'function': 'update',
                      'database': self_.database.name,
                      'collection': self_.name,
                      'query': dumps(args[0], sort_keys=True),
                      'explain': explain,
                      'source': get_source(self._filter_packages)})
            except Exception:
                logging.exception('exception pushing explain data for update')
        return self._func(self_, *args, **kwargs)

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.update, cls):
                pymongo.collection.Collection.update = \
                    cls(pymongo.collection.Collection.update)
                register_update_callback(
                    pymongo.collection.Collection.update._configure)
        return pymongo.collection.Collection.update

    @classmethod
    def unwrap(cls):
        with cls._lock:
            if isinstance(pymongo.collection.Collection.update, cls):
                unregister_update_callback(
                    pymongo.collection.Collection.update._configure)
                pymongo.collection.Collection.update = \
                    pymongo.collection.Collection.update._func


def start(config=None):
    if config is not None:
        configure(config)
    FindWrapper.wrap()
    UpdateWrapper.wrap()


def stop():
    UpdateWrapper.unwrap()
    FindWrapper.unwrap()


@contextmanager
def instrument(config=None):
    start(config)
    try:
        yield
    finally:
        stop()

def instrumented():
    return any([isinstance(pymongo.collection.Collection.update, Wrapper),
                isinstance(pymongo.collection.Collection.find, Wrapper)])
