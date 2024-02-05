from datetime import datetime
import time
import gspread
from schemas import ChannelDetails, DiscoveredDomains
import json
import time
from typing import TypeVar
from typing import Callable
import traceback
import random
from utube_parser import Parser

class DataLoader:



    def __init__(self):
        try:
            self.gc = gspread.service_account(filename='google_credentials/credentials.json')
            print(self.gc)
            sh = self.gc.open_by_key('12DONCMKhLcjcJ1aNJDBcyVUlp28mrMtOl6Rooj3OAok')
            self.config_page = sh.worksheet('config')
        except gspread.exceptions.APIError as e:
            print(f"Ошибка при подключении к таблице конфига: {e}")


    def get_google_sheet_id(self):
        with open('config.json') as f:
            data = json.load(f)
            google_sheet_id = str(data["service_account"]["config_id"])
            if google_sheet_id != None:
                return google_sheet_id
            else:
                print('error google_sheet_id')

    def conf_sheet(self):
        config_scan_id = self.get_google_sheet_id()
        config_scan_sheet_name = ('config')
        return config_scan_id, config_scan_sheet_name

    def initial_scan_sheet(self):
        initial_scan_id = self.get_google_sheet_id()
        initial_scan_sheet_name = ('initial_scan')
        return initial_scan_id, initial_scan_sheet_name

    def periodic_scan_sheet(self):
        periodic_scan_id = self.get_google_sheet_id()
        periodic_scan_sheet_name = ('periodic_scan')
        return periodic_scan_id, periodic_scan_sheet_name

    def backlog_scan_sheet(self):
        backlog_scan_id = self.get_google_sheet_id()
        backlog_scan_sheet_name = ('backlog_scan')
        return backlog_scan_id, backlog_scan_sheet_name

    def get_constants(self):
        initial_row = self.config_page.cell(11, 2).value
        if initial_row != None:
            return int(initial_row)
        else:
            print('error initial row')

    def update_initial_row(self, new_value):
        config_id, conf_sheet_name = self.conf_sheet()
        sh = self.gc.open_by_key(config_id)
        page = sh.worksheet(conf_sheet_name)
        cell = ('R11C2')
        page.update(cell, new_value)

    def get_er(self):
        er_row_cell = self.config_page.find("er")
        er_row = self.config_page.cell(er_row_cell.row, er_row_cell.col + 1).value
        if er_row != None:
            return float(er_row)
        else:
            print('error ER')

    def read_initial_scan_ids(self) -> str:
        initial_scan_id, initial_scan_sheet_name = self.initial_scan_sheet()
        sh = self.gc.open_by_key(initial_scan_id)
        page = sh.worksheet(initial_scan_sheet_name)
        ids_col_num = 1
        scanDate_col_num = 5
        initial_row = self.get_constants()
        while True:
            idi_cell = page.cell(initial_row, ids_col_num)
            idi = idi_cell.value
            if idi is not None and page.cell(initial_row, scanDate_col_num).value is None:
                initial_row += 1
                self.update_initial_row(initial_row)
                return idi  # Возвращаем первое подходящее значение
            elif idi is None:
                print('Список каналов пуст')
                time.sleep(300)

    def write_initial_scan_data(self, info: ChannelDetails):
        initial_scan_id, initial_scan_sheet_name =self.initial_scan_sheet()
        sh = self.gc.open_by_key(initial_scan_id)
        page = sh.worksheet(initial_scan_sheet_name)

        initial_row = self.get_constants() - 1

        row_values = [info.title, info.custom_url, info.tags, info.scan_date, info.subs,
                     info.published_at, info.avg_views, info.er, info.contacts]
        page.append_row(row_values, table_range="B{0}:J{0}".format(initial_row))
        self.write_periodic_scan_data(info)

    def get_key_index(self):
        api_key = self.config_page.cell(15, 2).value
        if api_key != None:
            return int(api_key)
        else:
            print('error get_apikey')

    def update_key_index(self, new_api_key):
        config_id, conf_sheet_name = self.conf_sheet()
        sh = self.gc.open_by_key(config_id)
        page = sh.worksheet(conf_sheet_name)
        cell = ('R15C2')
        page.update(cell, new_api_key)



    def write_periodic_scan_data(self, info: ChannelDetails):
        er = self.get_er()
        if info.er >= er:
            periodic_scan_id, periodic_scan_sheet_name = self.periodic_scan_sheet()
            sh = self.gc.open_by_key(periodic_scan_id)
            page = sh.worksheet(periodic_scan_sheet_name)
            row_values = [info.id, info.title, info.custom_url, info.tags, info.scan_date, info.subs,
                          info.published_at, info.avg_views, info.er, info.contacts]

            page.append_row(row_values)

        else:
            backlog_scan_id, backlog_scan_sheet_name = self.backlog_scan_sheet()
            sh = self.gc.open_by_key(backlog_scan_id)
            page = sh.worksheet(backlog_scan_sheet_name)
            row_values = [info.id, info.title, info.custom_url, info.tags, info.scan_date, info.subs,
                          info.published_at, info.avg_views, info.er, info.contacts]

            page.append_row(row_values)



