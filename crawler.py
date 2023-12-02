import os
import re
import random
import datetime as dt
import sqlite3

import requests
from bs4 import BeautifulSoup
import pandas as pd

from utils.logger import Logger

log = Logger('whats-next-dev')


def get_random_movie_url():
    url = 'https://letterboxd.com/'
    soup = BeautifulSoup(requests.get(url).content, 'html.parser')
    pattern = r'(\/film\/[^\/]+\/)'
    matched_urls = re.findall(pattern, str(soup))
    return 'https://letterboxd.com' + random.choice(matched_urls)


def get_random_user_url():
    while True:
        random_page = random.randint(1, 250)
        movie_url = get_random_movie_url() + 'members/page/' + str(random_page)
        soup = BeautifulSoup(requests.get(movie_url).content, 'html.parser')
        users = soup.findAll('td', class_='table-person')
        if users:
            break
    user = random.choice(users).find('h3').find('a')['href']
    return 'https://letterboxd.com' + user


def scrape_user_diary(user_url):
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
            rating = cols[4].find('span')
            pattern = r'(\d{1,2})'
            numbers = re.findall(pattern, str(rating))
            if len(numbers) == 0:
                rating = None
            elif len(numbers) == 1:
                rating = int(numbers[0])
            else:
                raise Exception('Multiple numbers found in rating.')

            # Append to DataFrame
            df = pd.concat([df, pd.DataFrame([[date, url, rating]], columns=['date', 'movie_url', 'rating'])])

        page += 1
    return df


if __name__ == '__main__':

    con = sqlite3.connect("/Users/lukas/lt_data/Code/python/whats-next-dev/letterboxd-diaries.sqlite")
    cur = con.cursor()

    stats = {'new': 0, 'old': 0}
    start_time = dt.datetime.now()
    while True:
        try:
            random_user = get_random_user_url()
            user_exists = cur.execute("SELECT 1 FROM user WHERE username = ?", (random_user,)).fetchone()
            if user_exists:
                stats['old'] += 1
                log.info(f'User already scraped: {user}.')
                continue
            # Get data
            diary_df = scrape_user_diary(random_user)

            # Add to user database
            username = random_user.split('/')[-2]
            num_diaries = len(diary_df)
            num_diaries_rated = len(diary_df[diary_df['rating'].notnull()])
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
            for index, row in diary_df.iterrows():

                # Use a single SQL query to insert into "diary" with a JOIN to fetch foreign keys
                sql_query = """
                INSERT INTO diary (user_id, movie_id, date, rating)
                SELECT u.id, m.id, ?, ?
                FROM user u
                JOIN movie m ON m.url_movie = ?
                WHERE u.username = ?;
                """
                cur.execute(sql_query, (row.date, row.rating, row.movie_url, username))

            log.info(f'Scraped {len(diary_df)} diaries for user {username}.')

            # Print stats
            stats['new'] += 1
            stats_percentage = {k: v/sum(stats.values()) for k, v in stats.items()}
            if sum(stats.values()) % 10 == 0:
                time_per_hit = (dt.datetime.now() - start_time) / stats['new']
                num_scraped = len(os.listdir('data/diary'))

                est_finish_100k = dt.datetime.now() + time_per_hit * (100000 - num_scraped)
                est_finish_500k = dt.datetime.now() + time_per_hit * (500000 - num_scraped)
                est_finish_1m = dt.datetime.now() + time_per_hit * (1000000 - num_scraped)

                log.info(f'Scraped: {stats_percentage["scraped"]:.2%}, '
                         f'Already scraped: {stats_percentage["already_scraped"]:.2%}.')
                log.info(f'Est. finish 100k: {est_finish_100k.strftime("%d.%m.%Y at %H:%M:%S")}.')
                log.info(f'Est. finish 500k: {est_finish_500k.strftime("%d.%m.%Y at %H:%M:%S")}.')
                log.info(f'Est. finish 1m  : {est_finish_1m.strftime("%d.%m.%Y at %H:%M:%S")}.')

            # Commit changes
            if sum(stats.values()) % 1 == 0:
                con.commit()

        except Exception as e:
            log.error(f'Error: {e}')
            continue
