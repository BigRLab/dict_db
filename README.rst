DictDb
======
| "Work with DBs as if they are Python dictionaries"
| Database as dict
| Python Database interface

Install
-------
* Currently not ready for pip

Usage
-----
- Use the DictDbFactory to init a database as a dictionary - and just use it
- see the various data-types for each db under %db%_ds

Example:
~~~~~~~~
.. code:: python

    In[3]: from dict_db import DictDbFactory, Consts
    # Access Redis
    In[4]: db = DictDbFactory(Consts.DB_REDIS).create("test", "sample")
    In[5]: db[1] = "Hello Db!"
    In[6]: db
    Out[6]: {'1': u'Hello Db!'}
    In[7]: db.__class__
    Out[7]: dict_db.redis_ds.redis_hash_dict.JSONRedisHashDict
    # Access Elastic-Search
    In[4]: db = DictDbFactory(Consts.DB_ELASTIC).create("test", "sample")
    In[5]: db[1] = "Hello Db!"
    In[6]: db
    Out[6]: {'1': u'Hello Db!'}

Next in module Development
--------------------------
- Organizing the interface
- Adding additional DB support
- wrapping queries into __getitem__

Contact me (py@bitweis.com) if you need these anytime soon.
