# Maki Data Updater

Service that periodically updates anime data using Anilist API.

For deployment and general information refer to the [main repository](https://github.com/maki-recommender/maki).

## Configuration

This section lists the environment variables that used to configure this service. 
Variables without a default value must be assigned.

`MAKI_UpdaterDatabaseConnection`

**Description:** PostgreSQL database connection url

**Example:** "postgresql://{username}:{password}@{ip}:{port}/maki"

**Default:** -


`MAKI_UpdaterRunEverySeconds`

**Description:** Delay time between two following data updates. A random delay up to 1/4 of this value may be added.

**Default:** 900