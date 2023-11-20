import os
import re
import random
import datetime as dt

import requests
from bs4 import BeautifulSoup
import pandas as pd

from utils.logger import Logger

log = Logger('whats-next-dev')

def index_diaries():
    diaries = os.listdir('data/diary')
    try:
        diaries.remove('.DS_Store')
    except ValueError:
        pass
    
    user_df = pd.DataFrame(columns=['num_diaries', 'num_ratings', 'avg_rating', 'username'])
    for index, diary in enumerate(diaries):
        username = diary.split('.')[0]
        if username in user_df['username'].values:
            continue
        df = pd.read_csv(f'data/diary/{diary}')
        num_diaries = len(df)
        num_ratings = len(df[df['rating'] != ''])
        avg_rating = round(df['rating'].mean(),2)
        user_df = pd.concat([user_df, pd.DataFrame([[num_diaries, num_ratings, avg_rating, username]],
                                                   columns=['num_diaries', 'num_ratings', 'avg_rating', 'username'])])
        log.debug(f'{index+1}/{len(diaries)}: {username} indexed.')
        
    user_df.to_csv('data/user_index.csv', index=False)
    
    
if __name__ == '__main__':
    index_diaries()