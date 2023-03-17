import csv
from datetime import datetime
import os
import time

from dotenv import load_dotenv
import requests

from tqdm import tqdm
from best_posts import pd_files


def get_posts(offset: int) -> dict:
    load_dotenv()
    url = "https://api.vk.com/method/wall.get"
    group_params = {
        "owner_id": os.getenv("GROUP_ID"),
        "count": "100",
        "filter": "all",
        "extended": "1",
        "offset": offset,
    }
    vk_params = {
        "v": "5.131",
        "access_token": os.getenv("SERVICE_KEY")
    }

    response = requests.get(url=url, params={**group_params, **vk_params})
    return response.json()


def data_processing(date_before=datetime(2016, 2, 20)) -> None:
    offset = 0

    while True:
        data = get_posts(offset)
        time.sleep(0.2)

        posts = data['response']['items']
        if not posts:
            print('Выгрузка завершена. Формируем файлы.')
            break

        for post in tqdm(posts):
            if post.get('is_pinned'):
                continue

            post_date = datetime.fromtimestamp(post['date'])

            if post_date < date_before:
                return

            if date_before <= post_date <= datetime.today():
                publication = {}
                publication['id'] = post['id']
                date = post_date.strftime('%d-%m-%Y %H:%M')
                publication['date'] = date
                publication['likes'] = post['likes']['count']
                publication['reposts'] = post['reposts']['count']
                publication['text'] = post['text']
                if post.get('views'):
                    publication['views'] = post['views']['count']
                publication['url'] = f"{os.getenv('GROUP_URL')}{post['id']}"

                upload_to_csv(publication)

        offset += 100


# def upload_to_json(data: list) -> None:
#     with open('posts.json', 'w', encoding='utf-8') as file:
#         json.dump(data, file, ensure_ascii=False)


def upload_to_csv(data: dict) -> None:
    filename = 'posts.csv'
    fieldnames = ['id', 'date', 'likes', 'reposts', 'text', 'views', 'url']

    if os.path.exists(filename):
        with open(filename, 'a', encoding='utf-8', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writerow(data)

    else:
        with open(filename, 'w', encoding='utf-8', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(data)


def delete_file(filename):
    os.remove(filename)


if __name__ == '__main__':
    data_processing()
    pd_files()
    delete_file('posts.csv')
