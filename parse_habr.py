# импорт библиотек
import requests 
from bs4 import BeautifulSoup
import pandas as pd
import re
from multiprocessing import Pool
from tqdm import tqdm
import pickle
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry


'''
задаю переменные для обработки сайта. 
Если при обращении на сайт три раза нет результата или выдаются ошибки статуса, 
то в последующих функциях эта переменная позволит исполнить код и перейти к следуюзему сайту
'''
retry_strategy = Retry(
    total=3,  
    status_forcelist=[429, 500, 502, 503, 504],
    backoff_factor=1 
)
http_adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session() # запускаю сессию для обращения к сайту
session.mount("http://", http_adapter) #задаю конфигурации  обращения
session.mount("https://", http_adapter)

# ключевые слова для поиска по сайтам
keywords = ['kafka', 'spark', 'airflow']



# Главная функция обработки сайтов

def download_articles(url):
    try:
        r = session.get(url, timeout=5)  # делаем запрос на сайт
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, 'html5lib') # парсим всю html страничку
        title_h1 = soup.find('h1') # находим заголовок статьи
        if not title_h1:
            return None
        # предобработка названия статьи и тегов
        title_words = re.sub(r'[^\w\s]', ' ', title_h1.text).lower().split()
        tags_class = soup.find_all('li', {'class': 'tm-separated-list__item'})
        tags = [tag.text.strip().lower() for tag in tags_class]

        # фильтрация содержания статьи с исходными ключевыми словами
        if any(keyword in title_words or keyword in tags for keyword in keywords):
            time = soup.find("span", {"class": "tm-article-datetime-published"}).time['datetime']
            views = soup.find("span", {"class": "tm-icon-counter__value"}).text
            hub_tags = soup.find("div", {"class": "tm-publication-hubs"})
            hub_texts = [hub.text.strip() for hub in hub_tags.find_all('a')] if hub_tags else []
            bookmarks = soup.find('span', {'class': 'bookmarks-button__counter'}).text
            comments = soup.find('span', {'class': 'tm-article-comments-counter-link__value'}).text

            return {
                'url': url,
                'title': title_words,
                'time': time,
                'views': views,
                'tags': tags,
                'hub': hub_texts,
                'bookmarks': bookmarks,
                'comments': comments
            }
        return None
    except RequestException as e:
        print(f"Network error for URL {url}: {e}")
    except Exception as e:
        print(f"Error parsing {url}: {e}")
    return None

'''
функция для разбиения переданного списка статей 
(передается список из 1000 элементов и разбивается на 4 процесса, следовательно паралелльно обрабатываеся 250 статей, 
на самом деле меньше по 50 статей, но параллельно). 
'''
def parallel_parse(urls, num_processes=5):
    with Pool(num_processes) as pool:
        results = []
        for result in tqdm(pool.imap(download_articles, urls, chunksize=50), total=len(urls), desc="Processing URLs"):
            if result is not None:
                results.append(result)
    return results



# задаем url адрес общий для всех статей 
base_url = 'https://habr.com/ru/articles/'
urls = [f"{base_url}{i}/" for i in range(790124, 800000)] # статьи в хабре индексируются по номеру в URL адресе и поэтому эти номера можно положить в цикл и создать список всех url адресов


chunk_size = 1000
chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)] #тут у меня список длинной 900, состоящий из списков по 1000 юрлов

all_results = [] 
checkpoint_file = 'checkpoint5.pkl' #моя директория

for chunk in tqdm(chunks, desc="Processing chunks"): #  chunk в цикле это один список с 1000 юрлами
    try:
        results = parallel_parse(chunk)
        all_results.extend(results)

        # сохраняем промежуточный результат если что-то отвалиться
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(all_results, f)
        print(f"Processed {len(results)} articles in this chunk.")
    except Exception as e:
        print(f"Error processing chunk: {e}")
