import logging
from common import get_env

import asyncpg

database: asyncpg.Pool = None


async def connect():
    """Connect to postgres"""
    global database
    database = await asyncpg.create_pool(get_env("MAKI_UpdaterDatabaseConnection"), min_size=1, max_size=10)
    logging.info("Connected to sql database")


async def disconnect():
    """Disconnect from postgres"""
    await database.close()
    logging.info("Disconnected from sql database")


##########################################################################################
# Helper functions


async def execute(query: str , *args):
    """Execute query without any returned value"""
    await database.execute(query, *args )



async def execute_many(query: str , args):
    """Execute the query many times. Args is an array of tuples containing arguments"""
    await database.executemany(query, args)



async def fetch_one(query: str, *args, return_dict=False):
    """Execute query and return first row"""

    result = await database.fetchrow(query, *args)
    if return_dict:
        return dict(result)
    else:
        return result



async def fetch_all(query: str, *args, return_dict=False):
    """Execute query and return all rows"""

    result = await database.fetch(query, *args)
    if return_dict:
        return [dict(x) for x in result]
    else:
        return result


async def fetch_value(query, *args):
    """Execute query and return the value of the first col of the first row"""
    return await database.fetchval(query, *args)
