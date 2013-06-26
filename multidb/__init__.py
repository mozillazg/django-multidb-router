"""
With :class:`multidb.MasterSlaveRouter` all read queries will go to a slave
database;  all inserts, updates, and deletes will do to the ``default``
database.

First, define ``SLAVE_DATABASES`` in your settings.  It should be a list of
database aliases that can be found in ``DATABASES``::

    DATABASES = {
        'default': {...},
        'shadow-1': {...},
        'shadow-2': {...},
    }
    SLAVE_DATABASES = ['shadow-1', 'shadow-2']

Then put ``multidb.MasterSlaveRouter`` into DATABASE_ROUTERS::

    DATABASE_ROUTERS = ('multidb.MasterSlaveRouter',)

The slave databases will be chosen in round-robin fashion.

If you want to get a connection to a slave in your app, use
:func:`multidb.get_slave`::

    from django.db import connections
    import multidb

    connection = connections[multidb.get_slave()]
"""
import itertools
import random
import datetime
from django.conf import settings


from .pinning import this_thread_is_pinned, db_write

import logging
logger = logging.getLogger(__name__)

UNIMAGINEABLE_FUTURE = datetime.datetime.now() + datetime.timedelta(days=365*1000)

DEFAULT_DB_ALIAS = 'default'
RECONNECT_TIME = datetime.timedelta(seconds=60 * getattr(settings, 'RECONNECT_TIME', 60))



for db in getattr(settings, 'SLAVE_DATABASES'):
    settings.DATABASES[db]['TEST_MIRROR'] = DEFAULT_DB_ALIAS
    
    
class SlavesProcessor:
    slaves_statistics = dict((db, {'available': True}) for db in getattr(settings, 'SLAVE_DATABASES'))  
    
    @classmethod
    def reset_slave_databases(cls):
        if getattr(settings, 'SLAVE_DATABASES'):
            dbs = filter(lambda x: cls.slaves_statistics[x]['available'], cls.slaves_statistics.keys())
            if dbs:                
                cls.slaves = itertools.cycle(dbs)
                return            
        cls.slaves = itertools.repeat(DEFAULT_DB_ALIAS)
            
    @classmethod
    def get_slave(cls):
        """Returns the alias of a slave database."""
        from django.db import connections
        next = cls.slaves.next()
        try:
            connections[next].cursor()
        except Exception as e:
            cls.disable_connection(next)
            return DEFAULT_DB_ALIAS
        else:
            return next
        
    @classmethod
    def disable_connection(cls, alias):
        cls.slaves_statistics[alias]['available'] = False
        cls.reset_slave_databases()
    
SlavesProcessor.reset_slave_databases()


class MasterSlaveRouter(object):
    """Router that sends all reads to a slave, all writes to default."""

    def db_for_read(self, model, **hints):
        """Send reads to slaves in round-robin."""
        if model._meta.app_label == 'auth':
            return DEFAULT_DB_ALIAS
        return SlavesProcessor.get_slave()

    def db_for_write(self, model, **hints):
        """Send all writes to the master."""
        return DEFAULT_DB_ALIAS

    def allow_relation(self, obj1, obj2, **hints):
        """Allow all relations, so FK validation stays quiet."""
        return True

    def allow_syncdb(self, db, model):
        """Only allow syncdb on the master."""
        return db == DEFAULT_DB_ALIAS


class PinningMasterSlaveRouter(MasterSlaveRouter):
    """Router that sends reads to master iff a certain flag is set. Writes
    always go to master.

    Typically, we set a cookie in middleware when the request is a POST and
    give it a max age that's certain to be longer than the replication lag. The
    flag comes from that cookie.

    """
    def db_for_read(self, model, **hints):
        """Send reads to slaves in round-robin unless this thread is "stuck" to
        the master."""
        if model._meta.app_label == 'auth':
            return DEFAULT_DB_ALIAS
        return DEFAULT_DB_ALIAS if this_thread_is_pinned() else SlavesProcessor.get_slave()
