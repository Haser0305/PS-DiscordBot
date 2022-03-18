import os
import sys

import psycopg2
import psycopg2.extras
import re
import logging
import requests
from bs4 import BeautifulSoup
import time
import random
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from math import ceil
from datetime import datetime, timedelta


class PsStore:

    home_page = ""  # home page url.
    api_url = ""    # url here.

    # put data here.
    category_ids = {
    }

    page_offset = 0

    # fill extension into it.
    extensions = {
    }

    # fill user-agent, accept-language and x-psn-store-locale-override
    headers = {

    }

    proxies = {
        'http': os.environ['http_proxy'] if 'http_proxy' in os.environ else '',
        'https': os.environ['https_proxy'] if 'https_proxy' in os.environ else ''
    }

    total_ps4_games_number = 0
    total_ps5_games_number = 0

    def __init__(self):
        # self._connect_firestore__()
        self.connect = self._connect_postgresql()
        self.cursor = self.connect.cursor()
        random.seed()

    def __del__(self):
        self.connect.commit()
        self.connect.close()

    def _connect_firestore__(self):
        # put cred here
        cred = credentials.Certificate('')
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()

    def _firestore_set(self, data: dict, collection, document):
        print('write data into sql')
        doc_ref = self.db.collection(collection).document(document)
        doc_ref.set(data)

    def _firestore_set_batch(self, games_info: list, collection: str, merge_bool: bool = True):
        batch = self.db.batch()
        data_length = len(games_info)
        for i in range(ceil(data_length / 500)):
            page = i * 500
            for game_info in games_info[page:page + 500]:
                batch.set(
                    self.db.collection(collection).document(game_info['id']),
                    game_info,
                    merge=merge_bool
                )

    def _firestore_update_batch(self, game_infos: list, collection: str):
        # data in data argument must be game_info
        batch = self.db.batch()
        data_length = len(game_infos)
        for i in range(ceil(data_length / 500)):
            page = i * 500
            for game_info in game_infos[page: page + 500]:
                batch.update(
                    self.db.collection(collection).document(game_info['id']),
                    game_info
                )
            batch.commit()
            time.sleep(1)

    @staticmethod
    def _strQ2B(s):
        # 把字串全形轉半形
        rstring = ""
        for uchar in s:
            u_code = ord(uchar)
            if u_code == 12288:  # 全形空格直接轉換
                u_code = 32
            elif 65281 <= u_code <= 65374:  # 全形字元（除空格）根據關係轉化
                u_code -= 65248
            rstring += chr(u_code)
        return rstring

    @staticmethod
    def _connect_postgresql():
        return psycopg2.connect(
            os.environ['databaseURI']
        )

    def _postgresql_insert_multiple(self, games_info: list):
        connect = self._connect_postgresql()
        cursor = connect.cursor()
        sql = 'insert into games_info ' \
              '("productId", "npTitleId", name, "basePrice", "discountPrice", "discountText", platforms, type) VALUES %s'
        psycopg2.extras.execute_values(
            cursor,
            sql,
            games_info
        )
        connect.commit()
        connect.close()

    def _postgresql_sql_multiple(self, sql: str, games_info: list):
        start_time = time.time()
        print(f'connect to DB')
        # connect = self._connect_postgresql()
        # cursor = connect.cursor()

        try:
            psycopg2.extras.execute_values(
                self.cursor,
                sql,
                games_info
            )
            self.connect.commit()
        except Exception as e:
            print(sql)
            print(e)

        # connect.close()
        print('close the connection, total cost: ', time.time() - start_time)

    def _get_all_ps4_games(self):

        # put ps4 parameter here.
        variables = ''

        parameters = {
            "operationName": "categoryGridRetrieve",
            "variables": variables,
            "extensions": self.extensions
        }

        response = requests.get(self.api_url, parameters, headers=self.headers)
        try:
            data = response.json()
        except:
            logging.error(f'url => {self.api_url}')
            logging.error(f'parameters => {parameters}')
            logging.error(f'headers => {self.headers}')
            logging.error('response data is wrong.')
            logging.error(response.text)
            exit(-1)

        data = data['data']

        self.total_ps4_games_number = data["categoryGridRetrieve"]["pageInfo"]["totalCount"]

        for page in range(0, ceil(self.total_ps4_games_number / 48)):
            games_info = []
            update_list = []
            print('current page: ', page + 1)
            # put if in here.
            variables = '{' \
                        '"id":"",' \
                        '"pageArgs":{' \
                        '"size":48,' \
                        '"offset":'f"{page * 48}"'},' \
                        '"sortBy":null,' \
                        '"facetOptions":[]}'
            parameters = {
                "operationName": "categoryGridRetrieve",
                "variables": variables,
                "extensions": self.extensions
            }

            response = requests.get(self.api_url, parameters, headers=self.headers)
            data = response.json()['data']

            games = data['categoryGridRetrieve']['products']
            update_time = datetime.utcnow() + timedelta(hours=8)
            temp_game_ids = []

            for game in games:

                if game['id'] in temp_game_ids:
                    continue

                base_price = re.sub(r'[^\d]', '', game['price']['basePrice'])
                discounted_price = re.sub(r'[^\d]', '', game['price']['discountedPrice'])
                game_info = (
                    game['id'],
                    game['npTitleId'],
                    self._strQ2B(game['name']),
                    int(base_price) if base_price else -1,
                    int(discounted_price) if discounted_price else 0,
                    game['price']['discountText'],
                    int(discounted_price) if discounted_price else 0,
                    game['platforms'],
                    game['skus'][0]['type'],
                    update_time
                )
                games_info.append(game_info)

                update_list.append((
                    game['id'],
                    int(discounted_price) if discounted_price else -1,
                    game['price']['discountText'],
                    update_time
                ))

                temp_game_ids.append(game['id'])

            self._postgresql_sql_multiple(
                f"""
                insert into games_info
                values %s
                on conflict ("productId")
                do update set "discountPrice" = excluded."discountPrice",
                              "name"          = excluded."name",
                              "discountText"  = excluded."discountText",
                              "updateTime"    = excluded."updateTime";
                """, games_info)

            self._postgresql_sql_multiple(
                """
                update games_info as g_info
                set "lowestPrice" = e.discountPrice, "updateTime" = e.updateTime
                from (values %s) as e(id, discountPrice, discountText, updateTime)
                where g_info."productId" = e.id and (g_info."lowestPrice" > e.discountPrice)
                """,
                update_list
            )
            print('finish page ', page + 1)
            time.sleep(random.uniform(0.1, 1))

    def _get_all_ps5_games(self):
        # put id here.
        variables = '{' \
                    '"id":"",' \
                    '"pageArgs":{' \
                    '"size":48,' \
                    '"offset":'f"{self.page_offset}"'},' \
                    '"sortBy":{"name":"productReleaseDate",' \
                    '"isAscending":false},' \
                    '"facetOptions":[],' \
                    '"filterBY":[],' \
                    '"faceOptions":[]}'

        print(variables)
        parameters = {
            "operationName": "categoryGridRetrieve",
            "variables": variables,
            "extensions": self.extensions
        }

        response = requests.get(self.api_url, parameters, headers=self.headers, proxies=self.proxies)
        try:
            data = response.json()
        except:
            logging.error(f'url => {self.api_url}')
            logging.error(f'parameters => {parameters}')
            logging.error(f'headers => {self.headers}')
            logging.error('response data is wrong.')
            logging.error(response.text)
            exit(-1)

        data = data['data']

        self.total_ps5_games_number = data["categoryGridRetrieve"]["pageInfo"]["totalCount"]

        for page in range(0, ceil(self.total_ps5_games_number / 48)):
            games_info = []
            update_list = []
            temp_game_ids = []
            print('current page: ', page + 1)
            # put id here.
            variables = '{' \
                        '"id":"",' \
                        '"pageArgs":{' \
                        '"size":48,' \
                        '"offset":'f"{self.page_offset}"'},' \
                        '"sortBy":{"name":"productReleaseDate",' \
                        '"isAscending":false},' \
                        '"facetOptions":[],' \
                        '"filterBY":[],' \
                        '"faceOptions":[]}'
            parameters = {
                "operationName": "categoryGridRetrieve",
                "variables": variables,
                "extensions": self.extensions
            }

            response = requests.get(self.api_url, parameters, headers=self.headers, proxies=self.proxies)
            data = response.json()['data']

            games = data['categoryGridRetrieve']['products']
            update_time = datetime.utcnow() + timedelta(hours=8)
            for game in games:

                if game['id'] in temp_game_ids:
                    continue

                base_price = re.sub(r'[^\d]', '', game['price']['basePrice'])
                discounted_price = re.sub(r'[^\d]', '', game['price']['discountedPrice'])
                game_info = (
                    game['id'],
                    game['npTitleId'],
                    self._strQ2B(game['name']),
                    int(base_price) if base_price else -1,
                    int(discounted_price) if discounted_price else 0,
                    game['price']['discountText'],
                    int(discounted_price) if discounted_price else 0,
                    game['platforms'],
                    game['skus'][0]['type'],
                    update_time
                )
                games_info.append(game_info)

                update_list.append((
                    game['id'],
                    int(discounted_price) if discounted_price else -1,
                    game['price']['discountText'],
                    update_time
                ))

                temp_game_ids.append(game['id'])

            self._postgresql_sql_multiple(
                f"""
                insert into games_info
                values %s
                on conflict ("productId")
                do update set "discountPrice" = excluded."discountPrice",
                              "name"          = excluded."name",
                              "discountText"  = excluded."discountText";
                """, games_info)

            self._postgresql_sql_multiple(
                """
                update games_info as g_info
                set "lowestPrice" = e.discountPrice, "updateTime" = e.updateTime
                from (values %s) as e(id, discountPrice, discountText, updateTime)
                where g_info."productId" = e.id and (g_info."lowestPrice" > e.discountPrice)
                """,
                update_list
            )
            print('finish page ', page + 1)
            time.sleep(random.uniform(0.1, 1))

    def _use_op_get_games(self, op_code: str) -> list:
        variables = '{' \
                    '"id":"'f"{op_code}"'",' \
                    '"pageArgs":{' \
                    '"size":48,' \
                    '"offset":0},' \
                    '"sortBy":null,' \
                    '"facetOptions":[]}'

        parameters = {
            "operationName": "categoryGridRetrieve",
            "variables": variables,
            "extensions": self.extensions
        }

        response = requests.get(self.api_url, parameters, headers=self.headers)

        try:
            response_data = response.json()['data']
        except:
            print(response)

        try:
            total_games_number = response_data["categoryGridRetrieve"]["pageInfo"]["totalCount"]
        except TypeError:
            logging.error('response error')

        return_data = []

        for page in range(ceil(total_games_number / 48)):
            print('current page: ', page + 1)

            variables = '{' \
                        '"id":"'f"{op_code}"'",' \
                        '"pageArgs":{' \
                        '"size":48,' \
                        '"offset":'f"{page * 48}"'},' \
                        '"sortBy":null,' \
                        '"facetOptions":[]}'

            parameters = {
                "operationName": "categoryGridRetrieve",
                "variables": variables,
                "extensions": self.extensions
            }

            response_for = requests.get(self.api_url, parameters, headers=self.headers)
            response_data_in_for = response_for.json()['data']

            games = response_data_in_for['categoryGridRetrieve']['products']

            update_time = datetime.utcnow() + timedelta(hours=8)
            for game in games:
                base_price = re.sub(r'[^\d]', '', game['price']['basePrice'])
                discounted_price = re.sub(r'[^\d]', '', game['price']['discountedPrice'])

                game_info = (
                    game['id'],
                    game['npTitleId'],
                    game['name'],
                    int(base_price) if base_price else -1,
                    int(discounted_price) if discounted_price else -1,
                    game['price']['discountText'],
                    game['platforms'],
                    game['skus'][0]['type'],
                    update_time
                )

                return_data.append(game_info)

            time.sleep(random.uniform(0.5, 2))
        return return_data

    @staticmethod
    def _get_ems() -> list:
        # psn deals url.
        url = ''

        response = requests.get(url)

        content = BeautifulSoup(response.text, 'html.parser')

        ems_ul = content.find('ul', class_='ems-sdk-collection__list')
        deal_dis = ems_ul.find_all('a')

        ems_ids = []

        for i in deal_dis:
            href = i['href']
            category_id = re.search(r'category/([^/]*)', href)
            if category_id:
                ems_ids.append(category_id.groups()[0])

        return ems_ids

    def _update_price(self):
        ems = self._get_ems()

        update_dict = {}
        for i in ems:
            update_dict[i] = self._use_op_get_games(i)

        for i in update_dict.values():
            # todo update database

            update_list = []
            for j in range(len(i)):
                update_list.append((
                    i[j][0],  # game's id
                    i[j][4],  # game's discountPrice
                    i[j][5],  # game's discountText
                    i[j][8]
                ))

            self._postgresql_sql_multiple(
                """
                update games_info as g_info
                set "discountPrice" = e.discountPrice, "discountText" = e.discountText, "updateTime" = e.updateTime
                from (values %s) as e(id, discountPrice, discountText, updateTime)
                where g_info."productId" = e.id
                """,
                update_list
            )

            self._postgresql_sql_multiple(
                """
                update games_info as g_info
                set "lowestPrice" = e.discountPrice, "updateTime" = e.updateTime
                from (values %s) as e(id, discountPrice, discountText, updateTime)
                where g_info."productId" = e.id and (g_info."lowestPrice" > e.discountPrice or g_info."lowestPrice" = -1)
                """,
                update_list
            )

            self._postgresql_sql_multiple(
                """
                with data (
                    id, "npTitleId", name, "basePrice", "discountPrice", 
                    "discountText", platforms, type, updateTime
                    )
                as (
                    values %s
                )
                insert into games_info
                (
                "productId", "npTitleId", name, "basePrice", "discountPrice", "discountText", platforms, type, "updateTime"
                )
                select d.id, d."npTitleId", d.name, d."basePrice", d."discountPrice", d."discountText", 
                    d.platforms, d.type, d.updateTime
                from data d
                where not exists(select 1 from games_info g_info where g_info."productId" = d.id)
                """,
                i
            )

    def test_function(self):
        # self.update_price()
        # print(os.environ['ttt'])
        a = self._get_ems()
        # self.get_all_ps4_games()

    def run_command(self, command_line):
        if command_line == 'get all games':
            self._get_all_ps4_games()
            self._get_all_ps5_games()
            exit(0)
        if command_line == 'get all ps4 games':
            self._get_all_ps4_games()
        elif command_line == 'get ems':
            self._update_price()
        elif command_line == 'get all ps5 games':
            self._get_all_ps5_games()
        else:
            print('no such argument')


start_time = time.time()
test = PsStore()
logging.basicConfig(level=logging.INFO)
# test.get_all_ps4_games()
# test.test_function()

if __name__ == '__main__':
    arg = sys.argv[1]
    test.run_command(arg)

print('run time: ', time.time() - start_time)
