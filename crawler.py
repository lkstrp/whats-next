import os
import re
import random
import datetime as dt

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
    # Get random number between 1 and 100

    while True:
        random_page = random.randint(1, 100)
        movie_url = get_random_movie_url() + 'members/page/' + str(random_page)
        soup = BeautifulSoup(requests.get(movie_url).content, 'html.parser')
        users = soup.findAll('td', class_='table-person')
        if users:
            break
    user = random.choice(users).find('h3').find('a')['href']
    return 'https://letterboxd.com' + user

def scrape_and_save_user_diary(user_url):

    diary_url = user_url + 'films/diary/'
    user = user_url.split('/')[-2]

    if os.path.exists(f'data/diary/{user}.csv'):
        log.info(f'User already scraped: {user}.')
        return 'already_scraped'

    page = 1
    df = pd.DataFrame(columns=['date', 'movie', 'rating'])
    while True:
        soup = BeautifulSoup(requests.get(f'{diary_url}page/{page}').content, 'html.parser')
        # Find the table by ID
        table = soup.find('table', {'id': 'diary-table'})
        try:
            rows = table.find_all('tr')
        except AttributeError:
            log.info(f'User has no diaries: {user}.')
            return 'no_diaries'
        if not rows[1:]:
            break
        month = None
        for row in rows[1:]:
            cols = row.find_all('td')
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
            df = pd.concat([df, pd.DataFrame([[date, url, rating]], columns=['date', 'movie', 'rating'])])

        page += 1
    total_scraped = len(os.listdir('data/diary'))
    df.to_csv(f'data/diary/{user}.csv', index=False)

    log.info(f'{total_scraped+1}: Scraped {len(df)} diaries for user {user}.')
    return 'scraped'

if __name__ == '__main__':
    stats = {'scraped': 0, 'no_diaries': 0, 'already_scraped': 0}
    start_time = dt.datetime.now()
    while True:
        try:
            call = scrape_and_save_user_diary(get_random_user_url())
            stats[call] += 1
            stats_percentage = {k: v/sum(stats.values()) for k, v in stats.items()}
            if sum(stats.values()) % 10 == 0:
                time_per_hit = (dt.datetime.now() - start_time) / stats['scraped']
                num_scraped = len(os.listdir('data/diary'))

                est_finish_100k = dt.datetime.now() + time_per_hit * (100000 - num_scraped)
                est_finish_500k = dt.datetime.now() + time_per_hit * (500000 - num_scraped)
                est_finish_1m = dt.datetime.now() + time_per_hit * (1000000 - num_scraped)

                log.info(f'Scraped: {stats_percentage["scraped"]:.2%}, '
                         f'No diaries: {stats_percentage["no_diaries"]:.2%}, '
                         f'Already scraped: {stats_percentage["already_scraped"]:.2%}.')
                log.info(f'Est. finish 100k: {est_finish.strftime("%H:%M:%S")} at {est_finish.strftime("%d.%m")}.')
                log.info(f'Est. finish 500k: {est_finish.strftime("%H:%M:%S")} at {est_finish.strftime("%d.%m")}.')
                log.info(f'Est. finish 1m  : {est_finish.strftime("%H:%M:%S")} at {est_finish.strftime("%d.%m")}.')

        except Exception as e:
            log.error(f'Error: {e}')
            continue
