import inspect
import re
import urlparse

from datetime import datetime

from bson.json_util import loads, dumps
from bson.binary import Binary
from bson.code import Code
from bson.dbref import DBRef
from bson.errors import InvalidDocument
from bson.objectid import ObjectId
from bson.son import SON


BSON_TYPES = set([
    int,
    long,
    str,
    unicode,
    bool,
    float,
    datetime,
    ObjectId,
    type(re.compile('')),
    Code,
    type(None),
    Binary,
    DBRef,
    SON,
])


# _p_skeleton function courtesy of https://github.com/dcrosta/professor
def _p_skeleton(query_part):
    """
    Generate a "skeleton" of a document (or embedded document). A
    skeleton is a (unicode) string indicating the keys present in
    a document, but not the values, and is used to group queries
    together which have identical key patterns regardless of the
    particular values used. Keys in the skeleton are always sorted
    lexicographically.

    Raises :class:`~bson.errors.InvalidDocument` when the document
    cannot be converted into a skeleton (this usually indicates that
    the type of a key or value in the document is not known to
    Professor).

    """
    t = type(query_part)
    if t == list:
        out = []
        for element in query_part:
            sub = _p_skeleton(element)
            if sub is not None:
                out.append(sub)
        return u'[%s]' % ','.join(out)
    elif t in (dict, SON):
        out = []
        for key in sorted(query_part.keys()):
            sub = _p_skeleton(query_part[key])
            if sub is not None:
                out.append('%s:%s' % (key, sub))
            else:
                out.append(key)
        return u'{%s}' % ','.join(out)
    elif t not in BSON_TYPES:
        raise InvalidDocument('unknown BSON type %r' % t)


# _p_sanitize function courtesy of https://github.com/dcrosta/professor
def _p_sanitize(value):
    """"Sanitize" a value (e.g. a document) for safe storage
in MongoDB. Converts periods (``.``) and dollar signs
(``$``) in key names to escaped versions. See
:func:`~professor.skeleton.desanitize` for the inverse.
"""
    t = type(value)
    if t == list:
        return map(_p_sanitize, value)
    elif t == dict:
        return dict((k.replace('$', '_$_').replace('.', '_,_'), _p_sanitize(v))
                    for k, v in value.iteritems())
    elif t not in BSON_TYPES:
        raise InvalidDocument('unknown BSON type %r' % t)
    else:
        return value

# _p_desanitize function courtesy of https://github.com/dcrosta/professor
def _p_desanitize(value):
    """Does the inverse of :func:`~professor.skeleton.sanitize`.
"""
    t = type(value)
    if t == list:
        return map(_p_desanitize, value)
    elif t == dict:
        return dict((k.replace('_$_', '$').replace('_,_', '.'), _p_desanitize(v))
                    for k, v in value.iteritems())
    elif t not in BSON_TYPES:
        raise InvalidDocument('unknown BSON type %r' % t)
    else:
        return value


def skeleton(o):
    if isinstance(o, basestring):
        o = loads(o)
    return dumps(_p_skeleton(o))


def sanitize(value):
    return _p_sanitize(value)


def desanitize(value):
    return _p_desanitize(value)


def get_default_database(client, mongo_uri):
    return client[urlparse.urlparse(mongo_uri).path.strip('/')]


def get_pkg(globals_):
    if '__package__' in globals_:
        return globals_['__package__']
    elif '__name__' in globals_:
        return globals_['__name__'].rpartition('.')[0]
    return None


def get_source(filter_packages=None, up=2):
    # cut the first two frames off the stack since the first is this frame
    # and the second *should be* the wrapper's frame
    stack = inspect.stack()
    frame = stack[up]
    if filter_packages is not None:
        frame = \
            filter(lambda f: get_pkg(f[0].f_globals) \
                             not in filter_packages,
                   stack[up:])[0]
    try:
        return '%s:%d' % (frame[1], frame[2])
    finally:
        del frame
        del stack

