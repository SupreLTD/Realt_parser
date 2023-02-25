import os
import re
from datetime import datetime
from progress.bar import PixelBar
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from data import Flat
from fake_useragent import UserAgent
from environs import Env

from db_client import DbPostgres

env = Env()
env.read_env()
DBNAME = env.str('DBNAME')
USER = env.str('USER')
PASSWORD = env.str('PASSWORD')
HOST = env.str('HOST')

PARSER_NAME = 'domovita.by'

URL = 'https://domovita.by/minsk/flats/sale'
HEADERS = {
    'authority': 'domovita.by',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,ru;q=0.8',
    # 'cookie': 'PHPSESSID=mmp509p26pno5t8ubp9dmc5982; _csrf=f6b9ef11908cc2dd7fb47af1e40fb15c0f9fab957741c21f8dd4b1dc932ea9dda%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22C-WsVekLLVfV_gVdBZZqQMIv2vlJtJNI%22%3B%7D; _ym_uid=167728414286083575; _ym_d=1677284142; _gcl_au=1.1.2098334539.1677284142; _ym_isad=1; _gid=GA1.2.996573444.1677284143; _ym_visorc=b; showPopupExceptionOFlatsSale=98951759f8d97729f69755f679701d32b01aad7183c7cf099abb668b299f710da%3A2%3A%7Bi%3A0%3Bs%3A28%3A%22showPopupExceptionOFlatsSale%22%3Bi%3A1%3Bb%3A0%3B%7D; showedIdPopups=96c3ff2439421929f29df0aaf48b24a6d44ae731c98dc788bff737121877d026a%3A2%3A%7Bi%3A0%3Bs%3A14%3A%22showedIdPopups%22%3Bi%3A1%3Ba%3A1%3A%7Bi%3A86%3Bi%3A1677305759%3B%7D%7D; sticky-newsOFlatsSale=410b8eb1aeb4db6be051253d098a9e74e313ec0f9dc2955cf0d82a74379fe6fea%3A2%3A%7Bi%3A0%3Bs%3A21%3A%22sticky-newsOFlatsSale%22%3Bi%3A1%3Bi%3A1%3B%7D; 30sec_ap=9; 60sec_ap=9; 90sec_ap=9; _ga=GA1.2.2024849459.1677284142; 120sec_ap=9; _gat_UA-68177416-1=1; _ga_4NR71M7G48=GS1.1.1677284142.1.1.1677284503.0.0.0',
    'referer': 'https://domovita.by/minsk/flats/sale/2-komnatnaa-kvartira-ul-mihaila-savickogo-3-86',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'x-csrf-token': 'ekS6TvkRpV_cH2Bt1thekOMmAksah9VXgKA0BqtbiSc5ae09r3TOE5BJBjuJvwj0oXxYOkvKnCGy1lhM3xHHbg==',
    'x-requested-with': 'XMLHttpRequest',
}


class ParserDomovita(DbPostgres):
    url = ''
    headers = {}
    cookies = {}

    def __init__(self, url=None, headers=None, cookies=None, dbname=None, user=None, password=None, host=None):
        DbPostgres.__init__(self, dbname, user, password, host)
        self.url = url
        self.headers = headers
        self.cookies = cookies

    def get_all_flat_links(self, first_page, last_page):
        links = []
        bar = PixelBar('Progress', max=last_page)
        while first_page <= last_page:
            response = requests.get(self.url+f'?page={first_page}', self.headers)
            soup = BeautifulSoup(response.content, 'lxml')
            for i in soup.find_all('a', class_='found_item'):
                links.append(i['href'])
            bar.next()
            first_page += 1
        return links

    def get_data(self, links):
        flats = []
        for link in tqdm(links):
            response = requests.get(link, headers=self.headers)
            soup = BeautifulSoup(response.content, 'lxml')
            title = soup.find('h1').text.strip()
            try:
                description_raw = soup.find('div', class_='text-block').find_all('p')
            except Exception as e:
                description_raw = []
            description = ''
            for i in description_raw:
                if i is not None:
                    description += i.text + ' '
            raw_price = soup.find('div', class_='dropdown-pricechange_price-block').find('div')
            if raw_price is not None:
                price = int(re.sub('[^0-9]', '', raw_price.text.strip()))
            else:
                price = 0
            try:
                date = datetime.strptime(
                    soup.find('span', class_='publication-info__item publication-info__publication-date').text.strip(),
                    '%d.%m.%Y')
            except Exception as e:
                date = datetime.now()
            params_raw = [i.find_all('span') for i in soup.find_all('div', class_="object-info__cell")]
            keys = []
            values = []
            for k in params_raw:
                for e, i in enumerate(k):
                    if e % 2 == 0:
                        keys.append(i.text.replace('\n', ''))
                    else:
                        values.append(i.text.replace('\n', ''))
            params = dict(zip(keys, values))
            try:
                area = float(re.sub('[^0-9/.]', '', params['Площадь общая']))
            except Exception as e:
                area = 0
            try:
                year = params['Год постройки']
            except Exception as e:
                year = 'Не указано'
            try:
                rooms = params['Комнат']
            except Exception as e:
                rooms = 'Не указано'
            try:
                house_type = params['Материал стен']
            except Exception as e:
                house_type = 'Не указано'
            try:
                floor = params['Этаж']
            except Exception as e:
                floor = 'Не указано'
            try:
                city = params['Город']
            except Exception as e:
                city = 'Не указано'
            try:
                street = params['Адрес']
            except Exception as e:
                street = 'Не указано'
            try:
                house_number = params['Номер дома']
            except Exception as e:
                house_number = ''
            try:
                district = params['Район']
            except Exception as e:
                district = 'Не указано'
            try:
                neighborhood = params['Микрорайон']
            except Exception as e:
                neighborhood = 'Не указано'


            flats.append(Flat(
                link=link,
                title=title,
                price=price,
                description=description,
                posted_date=date,
                reference=PARSER_NAME,
                address=street + ' ' + house_number,
                district=district,
                house_type=house_type,
                neighborhood=neighborhood,
                floor=floor,
                rooms=rooms,
                area=area,
                city=city,
                year_of_construction=year,

            ))
        return flats

    def insert_flat(self, flat):
        self.query_update("""
         INSERT INTO realt (link, reference, posted_date, title,description, city, rooms, district, neighborhood, 
        address, price, telephone, area, year_of_construction, house_type, floor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (link) DO UPDATE
        SET
        link = EXCLUDED.link,
        posted_date = EXCLUDED.posted_date,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        city = EXCLUDED.city,
        rooms = EXCLUDED.rooms,
        district = EXCLUDED.district,
        neighborhood = EXCLUDED.neighborhood,
        address = EXCLUDED.address,
        price = EXCLUDED.price,
        telephone = EXCLUDED.telephone,
        area = EXCLUDED.area,
        year_of_construction = EXCLUDED.year_of_construction,
        house_type = EXCLUDED.house_type,
        floor = EXCLUDED.floor""", (
            flat.link, flat.reference, flat.posted_date, flat.title, flat.description, flat.city, flat.rooms,
            flat.district,
            flat.neighborhood,
            flat.address, flat.price, flat.telephone, flat.area, flat.year_of_construction, flat.house_type,
            flat.floor))

    def save(self, flats):
        for flat in tqdm(flats):
            try:
                self.insert_flat(flat)
            except Exception as e:
                continue
        self.close()

    def run(self):
        links = self.get_all_flat_links(1,2)
        data = self.get_data(links)
        self.save(data)


a = ParserDomovita(url=URL, headers=HEADERS, dbname=DBNAME, user=USER, password=PASSWORD, host=HOST)
a.run()