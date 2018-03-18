"""Sqlite3DBreader"""

import sqlite3
import urllib.request
import config


class Sqlite3DBreader:
    def __init__(self, db_name):
        self.db_name = db_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def get_table_names(self):
        self.cur.execute("select name from sqlite_master where type = 'table'")
        return self.cur.fetchall()

    def get_column_names(self, table_name):
        self.cur.execute("PRAGMA table_info(" + table_name + ")")
        column_names = [column[1] for column in self.cur.fetchall()]
        return column_names

    def get_table(self, table_name, column_list = []):
        if not column_list:
            column_row = tuple(self.get_column_names(table_name))
        else:
            column_row = tuple(column_list)
        self.cur.execute("select " + ', '.join(column_row) + " from " + table_name)
        table = self.cur.fetchall()
        table.insert(0, column_row)
        return table

    def save_to_file(self, filename, table, delimiter):
        with open(filename, "w+") as file:
            line = '\n'.join([delimiter.join([str(item) for item in row]) for row in table])
            file.write(line + '\n')

    def download_photos(self, table, url_column_name):
        new_table = [list(row) for row in table]
        url_index = None
        for i, row in enumerate(new_table):
            if not i:
                for j, item in enumerate(row):
                    if item == url_column_name:
                        url_index = j
                        break
            else:
                if url_index and row[url_index]:
                    filename = str(row[0]) + config.photo_type
                    destination = config.photos_path + filename
                    print('Downloading ' + filename)
                    urllib.request.urlretrieve(row[url_index], destination)
