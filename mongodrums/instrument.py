import inspect
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

import pymongo

from bson.json_util import dumps
from bunch import Bunch
from pymongo.errors import OperationFailure

from .config import (
    configure, get_config, register_update_callback, unregister_update_callback
)
from .pusher import push


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

    def _get_pkg(globals_):
        if globals_.get('__package__', None) is not None:
            return globals_['__package__']
        return globals_['__name__'].rpartition('.')[0]

    def _get_source(self):
        # cut the first two frames off the stack since the first is this frame
        # and the second *should be* the wrapper's frame
        frame = \
            filter(lambda f: self._get_pkg(f[0].f_globals) \
                             not in self._filter_packages,
                   inspect.stack()[2:])[0]
        try:
            return '%s:%d' % (frame[1], frame[2])
        finally:
            del frame

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


class FindWrapper(Wrapper):
    def __call__(self, self_, *args, **kwargs):
        curs = self._func(self_, *args, **kwargs)
        if random.random() < self._frequency:
            try:
                explain = curs.explain()
            except (TypeError, OperationFailure):
                explain = {'error': traceback.format_exc()}
                logging.exception('error trying to run explain on curs')
            push({'type': 'explain',
                  'function': 'find',
                  'database': self_.database.name,
                  'collection': self_.name,
                  'query': dumps(args[0] if len(args) > 0 else {},
                                 sort_keys=True),
                  'explain': explain,
                  'source': self._get_source()})
        return curs

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.find, cls):
                pymongo.collection.Collection.find = \
                    cls(pymongo.collection.Collection.find)
                register_update_callback(
                    pymongo.collection.Collection.find._configure)

    @classmethod
    def unwrap(cls):
        with cls._lock:
            if isinstance(pymongo.collection.Collection.find, cls):
                unregister_update_callback(
                    pymongo.collection.Collection.find._configure)
                pymongo.collection.Collection.find = \
                    pymongo.collection.Collection.find._func


class UpdateWrapper(Wrapper):
    def __call__(self, self_, *args, **kwargs):
        if random.random() < self._frequency:
            curs = self_.find(args[0])
            try:
                explain = curs.explain()
            except (TypeError, OperationFailure):
                explain = {'error': traceback.format_exc()}
                logging.exception('error trying to run explain on curs')
            push({'type': 'explain',
                  'function': 'update',
                  'database': self_.database.name,
                  'collection': self_.name,
                  'query': dumps(args[0], sort_keys=True),
                  'explain': explain,
                  'source': self._get_source()})
        return self._func(self_, *args, **kwargs)

    @classmethod
    def wrap(cls):
        with cls._lock:
            if not isinstance(pymongo.collection.Collection.update, cls):
                pymongo.collection.Collection.update = \
                    cls(pymongo.collection.Collection.update)
                register_update_callback(
                    pymongo.collection.Collection.update._configure)

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
    FindWrapper.unwrap()
    UpdateWrapper.unwrap()


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
