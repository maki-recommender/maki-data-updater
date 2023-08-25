import asyncio
import logging

import database as db
import anilistdataupdater as adu

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(module)s@%(funcName)s: %(message)s")


async def run():
    await db.connect()

    await adu.periodically_update_anime_data()

    await db.disconnect()



loop = asyncio.get_event_loop()
loop.run_until_complete(run())