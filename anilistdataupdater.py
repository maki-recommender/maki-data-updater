import asyncio
import datetime
import logging
import random

import aiohttp

from common import Anime, get_env
import database as db

CLIENT_SESSION: aiohttp.ClientSession = None

ANILIST_API_URL = "https://graphql.anilist.co"

ANILIST_FETCH_QUERY = """
query ($page: Int, $formats: [MediaFormat]) {
  Page(page: $page, perPage: 50) {
    pageInfo {
      lastPage
    }
    media(format_in: $formats) {
      id
      idMal
      format
      status
      title {
        romaji
      }
      seasonYear
      coverImage {
        large
      }
      genres
      averageScore
    }
  }
}

"""

logger = logging.getLogger(__name__)

################################################################################
# data structure helpers

class AnilistAnimeDataBatch:
    """Class representing a bulk/batched insert into the database"""

    def __init__(self) -> None:
        self.genre_cache = {} # use this cache to check duplicates
        self.bulk_anime = []
        self.bulk_anilist_ids = []
        self.bulk_genre = []
        self.bulk_anime_genre = []

    
    def append(self, anime: Anime):
        anime.validate()
        # anime tuple tu insert
        self.bulk_anime.append(
            (
                anime.anilist,
                anime.title,
                anime.cover_url,
                anime.format,
                anime.release_year,
                anime.status,
                anime.normalized_score,
                anime.mal
            )
        )

        # ids to use to remove genres associations
        self.bulk_anilist_ids.append(
            (anime.anilist, )
        )

        # new genere associations tuples
        for genre in anime.genres:
            genre = genre.lower().replace(" ", "_")
            
            # append genres only once for genre insert
            if genre not in self.genre_cache:
                self.genre_cache[genre] = 1
                self.bulk_genre.append((genre,))
            
            # prepare anime-genre matches
            self.bulk_anime_genre.append((anime.anilist, genre))

    def clear(self):
        self.genre_cache.clear()
        self.bulk_anime.clear()
        self.bulk_anilist_ids.clear()
        self.bulk_genre.clear()
        self.bulk_anime_genre.clear()


    def __len__(self):
        return len(self.bulk_anilist_ids)

    async def save(self):
        """Save the whole list inside the database with an upsert operation"""
        # also insert anime but preserve old id
        await db.execute_many(
            """
            INSERT INTO animes (
                id,
                anilist_id,
                title,
                anilist_cover,
                format_id,
                release_year,
                status_id,
                anilist_normalized_score,
                mal_id,
                created_at,
                updated_at
                )
            VALUES ( 
                (SELECT coalesce(max(id), 0) + 1 FROM animes),
                $1,
                $2,
                $3,
                (SELECT id FROM anime_formats WHERE anilist = $4),
                $5,
                (SELECT id FROM anime_air_statuses WHERE anilist = $6),
                $7,
                $8,
                NOW(),
                NOW()
            )
            ON CONFLICT (anilist_id) DO UPDATE SET
                anilist_id=EXCLUDED.anilist_id,
                title=EXCLUDED.title,
                anilist_cover=EXCLUDED.anilist_cover,
                format_id=EXCLUDED.format_id,
                release_year=EXCLUDED.release_year,
                status_id=EXCLUDED.status_id,
                anilist_normalized_score=EXCLUDED.anilist_normalized_score,
                mal_id=EXCLUDED.mal_id,
                updated_at=EXCLUDED.updated_at
            """,
            self.bulk_anime
        )


        # insert new genres
        await db.execute_many(
            """
            INSERT INTO genres (name) VALUES ($1)
            ON CONFLICT (name) DO NOTHING
            """,
            self.bulk_genre
        )
        
        # remove old genres associations to let the new ones become the new genres
        await db.execute_many (
            """DELETE from anime_genres WHERE anime_id = (SELECT id FROM animes WHERE anilist_id = $1)""",
            self.bulk_anilist_ids
        )

        # insert new genre associations
        await db.execute_many(
            """
            INSERT INTO anime_genres (anime_id, genre_id) VALUES (
                (SELECT id FROM animes WHERE anilist_id = $1),
                (SELECT id FROM genres WHERE name = $2)
            ) ON CONFLICT (anime_id, genre_id) DO NOTHING
            """,
            self.bulk_anime_genre
        )

        logging.info("Inserted/Update %s animes", len(self.bulk_anime))


################################################################################
# initialization

def _alloc_client_session_if_missing():
    global CLIENT_SESSION
    if CLIENT_SESSION is None:
        CLIENT_SESSION = aiohttp.ClientSession()


async def _create_tracking_table():
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS anilist_update_tracking (
            page INTEGER PRIMARY KEY,
            next_scheduled_update TIMESTAMP DEFAULT NOW()
        );
        """
    )

################################################################################
# page management

async def _get_last_page() -> int:
    """Check how many pages are saved in the database"""
    return await db.fetch_value( "SELECT COUNT(page) FROM anilist_update_tracking")


async def _get_page_to_fetch() -> int:
    """Chose the best page to fetch from anilist api

    Returns
        page number to fetch
    """

    page = await db.fetch_value(
        """
        SELECT page
        FROM anilist_update_tracking
        WHERE next_scheduled_update - NOW() < INTERVAL '1 day'
        ORDER BY next_scheduled_update - NOW()
        LIMIT 1
        """
    )

    # return 1 only if no update has ever been run (page count == 0)
    if page is None and await _get_last_page() == 0: 
        return 1
    else:
        return page



async def _add_untracked_pages(pages_available: int):
    """Update the tracking table in the databased with missing pages for all anime formats

    Args:
        pages_available: number of pages on anilist api that are available
    """
    bulk_pages = []
    last_known_page = await _get_last_page()
    for page in range(pages_available - last_known_page):
        # +1 because pages start at one
        bulk_pages.append((last_known_page + page + 1, ))

    if len(bulk_pages) == 0:
        logger.info("No new pages detected")
        return

    await db.execute_many(
        """
        INSERT INTO anilist_update_tracking (page) VALUES ($1)
        """,
        bulk_pages
    )


async def _set_page_scheduled_update(page_number:int, latest_anime_year:int):
    """Calculate and se the new update time for the specified page based on the latest anime in the page
    
    Args:
        page_number: page number
        last_anime_year: year of the latest anime found in the specified page
    """

    now = datetime.datetime.now()

    if latest_anime_year == -1:
        latest_anime_year = now.year - 4 # if no year recheck next month

    delta_weeks = datetime.timedelta(weeks=abs(latest_anime_year - now.year))
    now += delta_weeks

    await db.execute(
        """
        UPDATE anilist_update_tracking SET next_scheduled_update = $1 WHERE page = $2
        """,
        now, page_number
    )

################################################################################
# data fetch and update

async def insert_animes(animes) -> int:
    """Insert anime data into the database

    Args:
        animes: array of anime json fetched from anilist api (see query above to see the required fields)
    
    Returns:
        Latest release year inserted into the database. Returns -1 if no latest year
        is known
    """
    latest_year = -1

    animeBatch = AnilistAnimeDataBatch()

    for anime in animes:

        a = Anime(
                anilist=anime["id"],
                title=anime["title"]["romaji"],
                mal=anime["idMal"],
                cover_url= anime["coverImage"]["large"],
                format=anime["format"],
                release_year= anime["seasonYear"],
                status= anime["status"],
                normalized_score= anime["averageScore"] / 100,
                genres=anime["genres"]
            )

        animeBatch.append(a)

        if a.release_year is not None and a.release_year > latest_year:
            latest_year = a.release_year
        
    await animeBatch.save()

    logger.info("Updated %d animes", len(animes))   

    return latest_year


async def fetch_anime_data():
    """Update/Insert anime based on anilist id. Does nothing if id is wrong
    """

    logger.info("Fetching anime data...")

    page_to_request = await _get_page_to_fetch()

    if page_to_request is None:
        logger.info("No data requires update. Skipping this update tick")
        return

    logger.info("Updating anilist page: %d", page_to_request)

    # make a api request to anilist api to fetch a page of anime data
    request_data = {
        "query": ANILIST_FETCH_QUERY,
        "variables": { 
            "page": page_to_request,
            "formats": ["TV", "TV_SHORT", "MOVIE","OVA", "ONA", "SPECIAL", "MUSIC"]
        }
    }

    async with CLIENT_SESSION.post(ANILIST_API_URL, json = request_data) as resp:
        if resp.status == 200:
            data = await resp.json()
            
            # update anime data inside the database
            latest_year = await insert_animes(data["data"]["Page"]["media"])

            # update pages inside the db
            await _add_untracked_pages(data["data"]["Page"]["pageInfo"]["lastPage"])

            # calculate and update next page scheduled update
            await _set_page_scheduled_update(page_to_request, latest_year)

        else:
            logger.error("Unable to reach anilist api")


################################################################################
# background task

async def periodically_update_anime_data():
    """Background task that updates anime data"""
    _alloc_client_session_if_missing()
    await _create_tracking_table()

    try:
        while True:
                
            # do an update run
            await fetch_anime_data()

            # wait some time to do the next update
            update_delta = int(get_env("MAKI_UpdaterRunEverySeconds", 15 * 60))
            random_delay = random.randint(0, int(update_delta / 4))
            delta = update_delta + random_delay
            logger.info("Data update tick ended. Waiting for next one in %d minutes", delta // 60)

            await asyncio.sleep(delta)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.exception(e)
    finally:
        
        await CLIENT_SESSION.close()
