import datetime as dt
import random
import re
import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .constants import PATH_CRAWLER_DB
from .utils.logger import Logger

log = Logger('whats_next-dev')

BASE_URL = 'https://letterboxd.com'


def extract_rating(cols: list[BeautifulSoup]) -> int or None:
    """Extract the rating from a list of HTML elements.

    Args:
    ----
        cols (list): The list of HTML elements representing columns.

    Returns:
    -------
        int or None: The extracted rating as an integer, or None if no rating is found.
    """
    rating = cols[4].find('span')
    pattern = r'(\d{1,2})'
    numbers = re.findall(pattern, str(rating))
    if len(numbers) == 0:
        return None
    elif len(numbers) == 1:
        return int(numbers[0])
    else:
        msg = 'Multiple numbers found in rating.'
        raise Exception(msg)


def fill_dataframe(rows: list[BeautifulSoup], df: pd.DataFrame, user: str) -> pd.DataFrame:
    """Fill a DataFrame with data from a list of table rows.

    Args:
    ----
        rows (list[BeautifulSoup]): A list of BeautifulSoup objects representing table rows.
        df (pd.DataFrame): The DataFrame to fill with data.
        user (str): The username for which to fill the DataFrame.

    Returns:
    -------
        pd.DataFrame: The filled DataFrame.

    """
    month = None
    for row_ in rows[1:]:
        cols = row_.find_all('td')
        # Get date
        if cols[0].text.strip() != '':
            month = cols[0].text
        date = pd.to_datetime(cols[1].text + month).date()
        # Get movie
        url = cols[2].find('a')['href'].replace(f'/{user}', '')
        # Get rating
        rating = extract_rating(cols)
        # Append to DataFrame
        df = pd.concat([df, pd.DataFrame([[date, url, rating]], columns=['date', 'movie_url', 'rating'])])
    return df


def get_random_movie_url() -> str:
    """Get the url for a random movie on Letterboxd.

    Returns
    -------
        str: Full url to a random movie.

    """
    soup = BeautifulSoup(requests.get(BASE_URL).content, 'html.parser')
    pattern = r'(\/film\/[^\/]+\/)'
    matched_urls = re.findall(pattern, str(soup))
    return BASE_URL + random.choice(matched_urls)


def get_random_user_url() -> str:
    """Get the url for a random user on Letterboxd.

    Returns
    -------
        str: Full url to the profile of a random user.
    """
    while True:
        random_page = random.randint(1, 250)
        movie_url = get_random_movie_url() + 'members/page/' + str(random_page)
        soup = BeautifulSoup(requests.get(movie_url).content, 'html.parser')
        users = soup.findAll('td', class_='table-person')
        if users:
            break
    user = random.choice(users).find('h3').find('a')['href']
    return BASE_URL + user


def scrape_user_diary(user_url: str) -> pd.DataFrame:
    """Scrapes the user's film diary from a given user URL and returns a pandas DataFrame.

    Args:
    ----
        user_url (str): The URL of the user's profile.

    Returns:
    -------
        pd.DataFrame: The scraped data as a pandas DataFrame, with columns 'date', 'movie_url', and 'rating'.
    """
    diary_url = user_url + 'films/diary/'
    user = user_url.split('/')[-2]
    page = 1
    df = pd.DataFrame(columns=['date', 'movie_url', 'rating'])
    while True:
        soup = BeautifulSoup(requests.get(f'{diary_url}page/{page}').content, 'html.parser')
        # Find the table by ID
        table = soup.find('table', {'id': 'diary-table'})
        # Check if user has any diary entries
        try:
            rows = table.find_all('tr')
        except AttributeError:
            break
        if not rows[1:]:
            break
        df = fill_dataframe(rows, df, user)
        page += 1
    return df


def main() -> None:
    """Run crawler."""
    con = sqlite3.connect(PATH_CRAWLER_DB)
    cur = con.cursor()

    user_stats = {'new': 0, 'old': 0, 'total': cur.execute('SELECT COUNT(*) FROM user').fetchone()[0]}
    diary_stats = {'new': 0, 'total': cur.execute('SELECT COUNT(*) FROM diary').fetchone()[0]}

    start_time = dt.datetime.now()
    while True:
        try:
            random_user = get_random_user_url()
            user_exists = cur.execute('SELECT 1 FROM user WHERE username = ?', (random_user,)).fetchone()
            if user_exists:
                user_stats['old'] += 1
                log.info(f'User already scraped: {random_user}.')
                continue
            # Get data
            diary_df = scrape_user_diary(random_user)

            # Add to user database
            username = random_user.split('/')[-2]
            num_diaries = len(diary_df)
            num_diaries_rated = len(diary_df[diary_df['rating'].notna()])
            timestamp = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql_query = """
                INSERT INTO user (username, num_diaries, num_diaries_rated, timestamp)
                SELECT ?, ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM user WHERE username = ?);
                """
            cur.execute(sql_query, (username, num_diaries, num_diaries_rated, timestamp, username))

            # Add to movie database
            for movie in diary_df['movie_url'].unique():
                sql_query = """
                    INSERT INTO movie (url_movie)
                    SELECT ?
                    WHERE NOT EXISTS (SELECT 1 FROM movie WHERE url_movie = ?);
                    """
                cur.execute(sql_query, (movie, movie))

            # Add to diary database
            for _, row in diary_df.iterrows():
                # Use a single SQL query to insert into "diary" with a JOIN to fetch foreign keys
                sql_query = """
                    INSERT INTO diary (user_id, movie_id, date, rating)
                    SELECT u.id, m.id, ?, ?
                    FROM user u
                    JOIN movie m ON m.url_movie = ?
                    WHERE u.username = ?;
                    """
                cur.execute(sql_query, (row.date, row.rating, row.movie_url, username))

            # Print stats
            user_stats['new'] += 1
            user_stats['total'] += 1
            diary_stats['new'] += len(diary_df)
            diary_stats['total'] += len(diary_df)

            log.info(f'{user_stats["new"]}: Scraped {len(diary_df)} diaries for user {username}.')

            if (user_stats['new'] + user_stats['old']) % 10 == 0:
                time_per_hit = (dt.datetime.now() - start_time) / diary_stats['new']

                est_finish_10m = dt.datetime.now() + time_per_hit * (float('1e7') - diary_stats['total'])
                est_finish_100m = dt.datetime.now() + time_per_hit * (float('1e8') - diary_stats['total'])
                est_finish_1b = dt.datetime.now() + time_per_hit * (float('1e9') - diary_stats['total'])

                log.info(f'Total: {user_stats["total"]:,} users and {diary_stats["total"]:,} diaries.')
                log.info(
                    f'User is new ratio: {user_stats["new"] / (user_stats["new"] + user_stats["old"]) * 100:.2f}%.'
                )
                log.info(f'Est. finish 10m : {est_finish_10m.strftime("%d.%m.%Y at %H:%M:%S")}.')
                log.info(f'Est. finish 100m: {est_finish_100m.strftime("%d.%m.%Y at %H:%M:%S")}.')
                log.info(f'Est. finish 1b  : {est_finish_1b.strftime("%d.%m.%Y at %H:%M:%S")}.')

            # Commit changes
            if (user_stats['new'] + user_stats['old']) % 100 == 0:
                con.commit()

        except Exception as e:
            log.error(f'Error: {e}')
            continue


if __name__ == '__main__':
    main()
