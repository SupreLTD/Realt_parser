from os import path

import psycopg2
from psycopg2.extras import NamedTupleCursor
from environs import Env

env = Env()
env.read_env()
DBNAME = env.str('DBNAME')
USER = env.str('USER')
PASSWORD = env.str('PASSWORD')
HOST = env.str('HOST')


class DbPostgres:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __del__(self):
        DbPostgres.__instance = None

    env = Env()
    env.read_env()
    DBNAME = env.str('DBNAME')
    USER = env.str('USER')
    PASSWORD = env.str('PASSWORD')
    HOST = env.str('HOST')

    def __init__(self,):
        self.conn = psycopg2.connect(
            dbname=self.DBNAME,
            user=self.USER,
            password=self.PASSWORD,
            host=self.HOST
        )
        self.conn.autocommit = True

    def fetch_one(self, query, arg=None, factory=None, clean=None):
        ''' Получает только одно ЕДИНСТВЕННОЕ значение (не ряд!) из таблицы
        :param query: Запрос
        :param arg: Переменные
        :param factory: dic (возвращает словарь - ключ/значение) или list (возвращает list)
        :param clean: С параметром вернет только значение. Без параметра вернет значение  в кортеже.
        '''
        try:
            cur = self.__connection(factory)
            self.__execute(cur, query, arg)
            return self.__fetch(cur, clean)

        except (Exception, psycopg2.Error) as error:
            self.__error(error)

    def fetch_all(self, query, arg=None, factory=None):
        """ Получает множетсвенные данные из таблицы
        :param query: Запрос
        :param arg: Переменные
        :param factory: dict (возвращает словарь - ключ/значение) или list (возвращает list)
        """
        try:
            with self.__connection(factory) as cur:
                self.__execute(cur, query, arg)
                return cur.fetchall()


        except (Exception, psycopg2.Error) as error:
            self.__error(error)

    def query_update(self, query, arg, message=None):
        """ Обновляет данные в таблице и возвращает сообщение об успешной операции """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, arg)
            return message

        except (Exception, psycopg2.Error) as error:
            self.__error(error)

    def close(self):
        cur = self.conn.cursor()
        cur.close()
        self.conn.close()

    def __connection(self, factory=None):
        # Dic - возвращает словарь - ключ/значение
        if factory == 'dict':
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # List - возвращает list (хотя и называется DictCursor)
        elif factory == 'list':
            cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Tuple
        else:
            cur = self.conn.cursor()

        return cur

    @staticmethod
    def __execute(cur, query, arg=None):
        # Метод 'execute' всегда возвращает None
        if arg:
            cur.execute(query, arg)
        else:
            cur.execute(query)

    @staticmethod
    def __fetch(cur, clean):
        # Если запрос был выполнен успешно, получим данные с помощью 'fetchone'
        if clean == 'no':
            fetch = cur.fetchone()
        else:
            fetch = cur.fetchone()[0]
        return fetch

    @staticmethod
    def __error(error):
        # В том числе, если в БД данных нет, будет ошибка на этапе fetchone
        print(error)
        return None

    def create_flats_table(self):
        self.query_update("""CREATE TABLE IF NOT EXISTS realt(
        id serial PRIMARY KEY,
        link CHARACTER VARYING(300) UNIQUE NOT NULL,
        reference CHARACTER VARYING(30),
        posted_date TIMESTAMP WITH TIME ZONE,
        title CHARACTER VARYING(1000),
        description CHARACTER VARYING(3000),
        city CHARACTER VARYING(100),
        rooms CHARACTER VARYING(15),
        district CHARACTER VARYING(100),
        neighborhood CHARACTER VARYING(100),
        address CHARACTER VARYING(200),
        price INTEGER,
        telephone CHARACTER VARYING(30),
        area DECIMAL(10,2),
        year_of_construction CHARACTER VARYING(6),
        house_type CHARACTER VARYING(30),
        floor CHARACTER VARYING(10)
          )""", arg=None)

    def insert_flat(self, flat: list):
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
        floor = EXCLUDED.floor
        RETURNING id INTO realt_images;
        
        INSERT INTO realt_images (image,realt_id) VALUES (unnest(%s)) 
        ON CONFLICT (image) DO UPDATE 
        SET
        image=image
                """, (
            flat.link, flat.reference, flat.posted_date, flat.title, flat.description, flat.city, flat.rooms,
            flat.district,
            flat.neighborhood,
            flat.address, flat.price, flat.telephone, flat.area, flat.year_of_construction, flat.house_type,
            flat.floor, flat.images))

    def create_img_table(self,flat):
        self.query_update("""WITH father AS(
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
                floor = EXCLUDED.floor
                RETURNING id 
                )
                
                INSERT INTO realt_images (realt_id,image) VALUES ((select id from father), unnest(%s)) 
                ON CONFLICT (image) DO UPDATE 
                SET
                image=EXCLUDED.image
                        """, (
            flat.link, flat.reference, flat.posted_date, flat.title, flat.description, flat.city, flat.rooms,
            flat.district,
            flat.neighborhood,
            flat.address, flat.price, flat.telephone, flat.area, flat.year_of_construction, flat.house_type,
            flat.floor, flat.images))


