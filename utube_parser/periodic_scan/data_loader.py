from datetime import datetime
import time
import gspread
from schemas import ChannelDetails, DiscoveredDomains
import json
import time
import random
from typing import TypeVar
from typing import Callable
import traceback


class DataLoader:
    def __init__(self, config_id, config_sheet_name):
        try:
            self.gc = gspread.service_account(filename='periodic_credentials/credentials.json')
            sh = self.gc.open_by_key(config_id)
            self.config_page = sh.worksheet(config_sheet_name)
        except gspread.exceptions.APIError as e:
            print(f"Ошибка при подключении к таблице конфига: {e}")

    def retry_with_backoff(self, fn, retries=10, backoff_in_seconds=1):
        x = 0

        while True:
            try:
                print('got it', fn)
                return fn()
            except:
                if x == retries:
                    raise

                sleep = backoff_in_seconds * 2 ** x + random.uniform(0, 1)
                if sleep > 130:
                    sleep = 130
                else:
                    sleep
                print('Время сна = ', sleep)
                time.sleep(sleep)
                x += 1

                print('Number of retries = ', x)
                print('fn = ', fn)
                traceback.print_exc()

    def get_google_sheet_id(self):
        with open('config.json') as f:
            data = json.load(f)
            google_sheet_id = str(data["service_account"]["config_id"])
            if google_sheet_id != None:
                return google_sheet_id
            else:
                print('error google_sheet_id')

    def conf_sheet(self):
        config_scan_id = self.retry_with_backoff(lambda:self.get_google_sheet_id())
        config_scan_sheet_name = ('config')
        return config_scan_id, config_scan_sheet_name


    def periodic_scan_sheet(self):
        periodic_scan_id = self.retry_with_backoff(lambda:self.get_google_sheet_id())
        periodic_scan_sheet_name = ('periodic_scan')
        return periodic_scan_id, periodic_scan_sheet_name


    def get_periodic_const(self):
        periodic_row_cell = self.retry_with_backoff(lambda:self.config_page.find("periodic_row"))
        periodic_row = self.retry_with_backoff(lambda:self.config_page.cell(periodic_row_cell.row, periodic_row_cell.col + 1).value)
        if periodic_row != None:
            return int(periodic_row)
        else:
            print('error in periodic row')


    def update_periodic_row(self, new_value):
        config_id, config_sheet_name = self.retry_with_backoff(lambda:self.conf_sheet())
        sh = self.retry_with_backoff(lambda: self.gc.open_by_key(config_id))
        page = self.retry_with_backoff(lambda: sh.worksheet(config_sheet_name))
        cell = ('R12C2')
        page.update(cell, new_value)



    def read_periodic_scan_ids(self) -> str:
        periodic_scan_id, periodic_scan_sheet_name = self.retry_with_backoff(lambda: self.periodic_scan_sheet())
        sh = self.retry_with_backoff(lambda: self.gc.open_by_key(periodic_scan_id))
        page = self.retry_with_backoff(lambda: sh.worksheet(periodic_scan_sheet_name))
        ids_col_num = 1
        periodic_row = self.retry_with_backoff(lambda:self.get_periodic_const())
        while True:
            idi_cell = self.retry_with_backoff(lambda: page.cell(periodic_row, ids_col_num))
            idi = self.retry_with_backoff(lambda: idi_cell.value)
            if idi is not None:
                periodic_row += 1
                self.update_periodic_row(periodic_row)
                return idi  # Возвращаем первое подходящее значение
            elif idi is None:
                print('Кончился список чтения read_periodic_scan_ids, засыпаю на 86400 секунд')
                break
                time.sleep(86400)

    def write_periodic_scan_data(self, info: ChannelDetails):
        periodic_scan_id, periodic_scan_sheet_name = self.retry_with_backoff(lambda:self.periodic_scan_sheet())
        sh = self.retry_with_backoff(lambda: self.gc.open_by_key(periodic_scan_id))
        page = self.retry_with_backoff(lambda: sh.worksheet(periodic_scan_sheet_name))

        periodic_row = self.retry_with_backoff(lambda:self.get_periodic_const() - 1)

        start_col = 2
        end_col = 9

        column_mapping = {
            'title': 2,
            'custom_url': 3,
            'tags': 4,
            'scan_date': 5,
            'subs': 6,
            'published_at': 7,
            'avg_views': 8,
            'er': 9,
            'contacts': 10,
        }

        for attribute, col_num in column_mapping.items():
            cell_value = getattr(info, attribute)
            self.retry_with_backoff(lambda: page.update_cell(periodic_row, col_num, cell_value))

