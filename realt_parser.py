import os
import re
from datetime import datetime

from progress.bar import PixelBar
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from data import Flat
from db_client import DbPostgres

PARSER_NAME = 'realt.by'

URL = 'https://realt.by/sale/flats/'
HEADERS = headers = {
    'authority': 'realt.by',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ru;q=0.8',
    'cache-control': 'max-age=0',
    # 'cookie': '_gcl_au=1.1.956878246.1676635258; currency-notice=1; adtech_uid=6dd22072-0a58-4dfa-b5cc-bfea66554e4f%3Arealt.by; top100_id=t1.808435.651698127.1676635258614; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; _ym_uid=16766352591058509637; _ym_d=1676635259; tmr_lvid=72fee35715cda63b3dc988606cfde3a9; tmr_lvidTS=1676635259004; _gid=GA1.2.1415878234.1676807228; gdprConfirmed=1; _ym_isad=1; realt-cu=840; realt_user=2246dfa26f7b0d280aba425db8614984; last_visit=1677144674694%3A%3A1677155474694; t3_sid_808435=s1.1780419181.1677154708612.1677155474701.18.6; tmr_detect=1%7C1677155476606; _dc_gtm_UA-1011858-1=1; _ga=GA1.1.1438964484.1676635259; _ga_3RLZ0E8JHQ=GS1.1.1677154708.32.1.1677155892.60.0.0; _ym_visorc=b; mindboxDeviceUUID=4dbf44df-f4ec-4844-9ffc-c13bf3b34d35; directCrm-session=%7B%22deviceGuid%22%3A%224dbf44df-f4ec-4844-9ffc-c13bf3b34d35%22%7D',
    'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
}

COOKIES = {
    '_gcl_au': '1.1.956878246.1676635258',
    'currency-notice': '1',
    'adtech_uid': '6dd22072-0a58-4dfa-b5cc-bfea66554e4f%3Arealt.by',
    'top100_id': 't1.808435.651698127.1676635258614',
    'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
    '_ym_uid': '16766352591058509637',
    '_ym_d': '1676635259',
    'tmr_lvid': '72fee35715cda63b3dc988606cfde3a9',
    'tmr_lvidTS': '1676635259004',
    '_gid': 'GA1.2.1415878234.1676807228',
    'gdprConfirmed': '1',
    '_ym_isad': '1',
    'realt-cu': '840',
    'realt_user': '2246dfa26f7b0d280aba425db8614984',
    'last_visit': '1677144674694%3A%3A1677155474694',
    't3_sid_808435': 's1.1780419181.1677154708612.1677155474701.18.6',
    'tmr_detect': '1%7C1677155476606',
    '_dc_gtm_UA-1011858-1': '1',
    '_ga': 'GA1.1.1438964484.1676635259',
    '_ga_3RLZ0E8JHQ': 'GS1.1.1677154708.32.1.1677155892.60.0.0',
    '_ym_visorc': 'b',
    'mindboxDeviceUUID': '4dbf44df-f4ec-4844-9ffc-c13bf3b34d35',
    'directCrm-session': '%7B%22deviceGuid%22%3A%224dbf44df-f4ec-4844-9ffc-c13bf3b34d35%22%7D',
}

class Parser(DbPostgres):
    url = ''
    headers = {}
    cookies = {}

    def __init__(self, url=None, headers=None, cookies=None):
        super().__init__()
        self.url = url
        self.headers = headers
        self.cookies = cookies

    def get_last_page(self):
        response = requests.get(self.url, headers=self.headers, cookies=self.cookies)
        soup = BeautifulSoup(response.content, 'lxml').find('div', class_='paging-list').find_all('a')[-1].get_text()

        return int(soup)

    def get_all_flat_links(self):

        raw_links = []
        current_page = 1
        last_page = 2
        bar = PixelBar('Progress', max=last_page)
        while current_page <= last_page:

            response = requests.get(self.url + f'?page={current_page}', headers=self.headers, cookies=self.cookies)
            soup = BeautifulSoup(response.content, 'lxml')
            for i in soup.find_all('a', href=True, class_='teaser-title'):
                raw_links.append(i['href'])
            bar.next()

            current_page += 1
        bar.finish()
        links = list(filter(lambda el: 'object' in el, raw_links))
        with open('url.txt', 'w', encoding='utf-8') as file:
            for link in links:
                file.write(link + '\n')

        return links

    def get_data(self):
        links = 'https://realt.by/sale-flats/object/2972716/',
        flats = []
        for link in tqdm(links):
            response = requests.get(link, headers=self.headers,cookies=self.cookies)
            soup = BeautifulSoup(response.content, 'lxml')
            title = soup.find('h1', class_='order-1').text.strip()
            description = soup.find('section', class_='bg-white').text.strip()
            raw_price = soup.find('h2', class_='w-full')
            if raw_price is not None:
                price = int(re.sub('[^0-9]', '', raw_price.text.strip()))
            else:
                price = 0
            try:
                date = datetime.strptime(soup.find('span', class_='mr-1.5').text.strip(), '%d.%m.%Y')
            except Exception as e:
                date = datetime.now()
            params = {i.find('span').text: i.find('p').text for i in soup.find('ul', class_='w-full -my-1')}
            place = {}
            for i in soup.find('ul', class_='w-full mb-0.5 -my-1'):
                try:
                    place[i.find('span').text] = re.sub('.(д)|(а)|(аг)|(г)|(гп)|.\xa0', '', i.find('a').text)
                except Exception as e:
                    place[i.find('span').text] = i.find('p').text

            try:
                area = float(re.sub('[^0-9/.]', '', params['Площадь общая']))
            except Exception as e:
                area = 0
            try:
                year = params['Год постройки']
            except Exception as e:
                year = 'Не указано'
            try:
                rooms = params['Количество комнат']
            except Exception as e:
                rooms = 'Не указано'
            try:
                house_type = params['Тип дома']
            except Exception as e:
                house_type = 'Не указано'
            try:
                floor = params['Этаж / этажность']
            except Exception as e:
                floor = 'Не указано'
            try:
                city = place['Населенный пункт']
            except Exception as e:
                city = 'Не указано'
            try:
                street = place['Улица']
            except Exception as e:
                street = 'Не указано'
            try:
                house_number = place['Номер дома']
            except Exception as e:
                house_number = ''
            try:
                district = place['Район города']
            except Exception as e:
                district = 'Не указано'
            try:
                neighborhood = place['Микрорайон']
            except Exception as e:
                neighborhood = 'Не указано'

            imgs = [i['content'] for i in soup.find_all('meta', property='og:image')]
            images = []
            for img in imgs:
                try:
                    res = requests.get(img, headers=self.headers)
                except Exception as e:
                    continue
                if not os.path.isdir("images/" + PARSER_NAME + '/' + re.sub('(https://realt.by|/)', '', link)):
                    os.makedirs("images/" + PARSER_NAME + '/' + re.sub('(https://realt.by|/)', '', link))
                progress = res.iter_content(1024)
                with open("images/" + PARSER_NAME + '/' + re.sub('(https://realt.by|/)', '', link) + '/' + re.sub(
                        '(https://static.realt.by/user|/)', '', img), "wb") as f:
                    for data in progress:
                        f.write(data)
                images.append("images/" + PARSER_NAME + '/' + re.sub('(https://realt.by|/)', '', link) + '/'
                              + re.sub('(https://static.realt.by/user|/)', '', img))

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
                images=images
            ))
            print(images)
        return flats

    def save_flats(self):
        for flat in tqdm(self.get_data()):
            self.create_img_table(flat)
        self.close()


a = Parser(URL,HEADERS,COOKIES)
a.save_flats()
