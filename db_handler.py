# -*- coding:utf-8 -*-
# Database handler class for the World Universities database

import sys
import sqlite3
import os.path
from pprint import pprint

class DbHandler(object):
    __TABLE_UNIVERSITIES = {
        'name': 'universities',
        'fields': {
            'id':   {'name': 'id', 'type': 'INTEGER'},
            'url':  {'name': 'url', 'type': 'TEXT'},
            'university': {'name': 'name', 'type': 'TEXT'},
            'country_code': {'name': 'c_code', 'type': 'TEXT'},
            'country': {'name': 'country', 'type': 'TEXT'},
        },
        'index': {
            'unique_url': 'IDX_U_URL'
        }
    }

    __TABLE_ACHECKER = {
        'name': 'achecker',
        'fields': {
            'id': {'name': 'id_uni', 'type': 'INTEGER'},
            'errors': {'name': 'NumOfErrors', 'type': 'INTEGER'}, # NumOfErrors
            'likely': {'name': 'NumOfLikelyProblems', 'type': 'INTEGER'}, # NumOfLikelyProblems
            'potential': {'name': 'NumOfPotentialProblems', 'type': 'INTEGER'}, # NumOfPotentialProblems
        }
    }

    # Table for transaction locking URL ID in multiprocessing env.
    __TABLE_LOCK = {
        'name': 'lock',
        'fields': {
            'id': {'name': 'id_uni', 'type': 'INTEGER'},
        }
    }

    def __init__(self,  db_file):
        self.__DB_FILE = db_file
        self.__createDatabase()

    def createLockSchema(self):
        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS {tbl}".format(tbl = self.__TABLE_LOCK['name']))
                cursor.execute("VACUUM")
                cursor.execute(
                    '''CREATE TABLE IF NOT EXISTS {tn} (
                        {f1} {t1} PRIMARY KEY
                        )
                    '''.format(
                        tn = self.__TABLE_LOCK['name'],
                        f1 = self.__TABLE_LOCK['fields']['id']['name'],
                        t1 = self.__TABLE_LOCK['fields']['id']['type']
                    )
                )
                conn.commit()
                conn.close()
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise

    def __initNewSchema(self):
        try:
            conn = sqlite3.connect(self.__DB_FILE)
            cursor = conn.cursor()
            cursor.execute(
                '''CREATE TABLE {tn} (
                    {f1} {t1} PRIMARY KEY AUTOINCREMENT,
                    {f2} {t2},
                    {f3} {t3},
                    {f4} {t4},
                    {f5} {t5})
                '''.format(
                    tn = self.__TABLE_UNIVERSITIES['name'],
                    f1 = self.__TABLE_UNIVERSITIES['fields']['id']['name'],
                    t1 = self.__TABLE_UNIVERSITIES['fields']['id']['type'],
                    f2 = self.__TABLE_UNIVERSITIES['fields']['url']['name'],
                    t2 = self.__TABLE_UNIVERSITIES['fields']['url']['type'],
                    f3 = self.__TABLE_UNIVERSITIES['fields']['university']['name'],
                    t3 = self.__TABLE_UNIVERSITIES['fields']['university']['type'],
                    f4 = self.__TABLE_UNIVERSITIES['fields']['country_code']['name'],
                    t4 = self.__TABLE_UNIVERSITIES['fields']['country_code']['type'],
                    f5 = self.__TABLE_UNIVERSITIES['fields']['country']['name'],
                    t5 = self.__TABLE_UNIVERSITIES['fields']['country']['type']
                )
            )
            cursor.execute('CREATE UNIQUE INDEX {index} on {tn} ({cn})'.format(
                index = self.__TABLE_UNIVERSITIES['index']['unique_url'],
                tn = self.__TABLE_UNIVERSITIES['name'],
                cn = self.__TABLE_UNIVERSITIES['fields']['url']['name']
            ))
            conn.commit()
            conn.close()
            print("New Database Initialised.")
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def __createDatabase(self):
        # Check if user is sure to recreate db schema
        if os.path.isfile(self.__DB_FILE):
            choice = raw_input("File \"" + self.__DB_FILE + "\" already exists. Do you want to recreate SQLITE database in it?(yes/no) : ")
            if choice == "yes":
                os.remove(self.__DB_FILE)
                self.__initNewSchema()
        else:
            self.__initNewSchema()
        self.createLockSchema()

    def createACheckerSchema(self):
        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='{tbl}'".\
                    format(tbl = self.__TABLE_ACHECKER['name'])
                )

                if len(cursor.fetchall()) <> 0:
                    choice = raw_input("The table '{tbl}' already exists. Do you to want to recreate table {tbl}? (yes/no) : ".format(tbl = self.__TABLE_ACHECKER['name']))
                    if choice == "yes":
                        cursor.execute("drop table if exists {tbl}".\
                            format(tbl = self.__TABLE_ACHECKER['name'])
                        )

                cursor.execute(
                    '''CREATE TABLE IF NOT EXISTS {tn} (
                        {f1} {t1} PRIMARY KEY,
                        {f2} {t2},
                        {f3} {t3},
                        {f4} {t4})
                    '''.format(
                        tn = self.__TABLE_ACHECKER['name'],
                        f1 = self.__TABLE_ACHECKER['fields']['id']['name'],
                        t1 = self.__TABLE_ACHECKER['fields']['id']['type'],
                        f2 = self.__TABLE_ACHECKER['fields']['errors']['name'],
                        t2 = self.__TABLE_ACHECKER['fields']['errors']['type'],
                        f3 = self.__TABLE_ACHECKER['fields']['likely']['name'],
                        t3 = self.__TABLE_ACHECKER['fields']['likely']['type'],
                        f4 = self.__TABLE_ACHECKER['fields']['potential']['name'],
                        t4 = self.__TABLE_ACHECKER['fields']['potential']['type']
                    )
                )
                print("Table {tbl} created successfully.".format(tbl=self.__TABLE_ACHECKER['name']))
                conn.commit()
                conn.close()
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
        else:
            print("Database does not exist.")

    def sql_escape_str(self, some_var):
        some_var = str(some_var)
        return ''.join(char for char in some_var if char.isalnum())

    def sql_escape_num(self, some_var):
        some_var = str(some_var)
        return ''.join(char for char in some_var if char.isdigit())

    def createURL(self, new_url):
        # Create new URL in DB
        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                sql = "INSERT OR IGNORE INTO {tn} ({url}, {uni}, {cc}, {cty}) VALUES (?, ?, ?, ?)".\
                    format(
                        tn = self.__TABLE_UNIVERSITIES['name'],
                        url = self.__TABLE_UNIVERSITIES['fields']['url']['name'],
                        uni = self.__TABLE_UNIVERSITIES['fields']['university']['name'],
                        cc = self.__TABLE_UNIVERSITIES['fields']['country_code']['name'],
                        cty = self.__TABLE_UNIVERSITIES['fields']['country']['name'],
                    )
                cursor.executemany(sql, new_url)
                conn.commit()
                conn.close()
            except sqlite3.IntegrityError:
                print("Duplicate error:", sys.exc_info()[0])
                pass
            except:
                print("Unexpected error:", sys.exc_info()[0])
        else:
            print("Database does not exist.")

    def insertAcheckerReport(self, result):
        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                sql = "INSERT OR IGNORE INTO {tn} ({id}, {errors}, {likely}, {potential}) VALUES (?, ?, ?, ?)".\
                    format(
                        tn = self.__TABLE_ACHECKER['name'],
                        id = self.__TABLE_ACHECKER['fields']['id']['name'],
                        errors = self.__TABLE_ACHECKER['fields']['errors']['name'],
                        likely = self.__TABLE_ACHECKER['fields']['likely']['name'],
                        potential = self.__TABLE_ACHECKER['fields']['potential']['name'],
                    )
                cursor.execute(sql, tuple(map(self.sql_escape_num, result)))
                conn.commit()
                conn.close()
            except sqlite3.IntegrityError:
                print("Duplicate error:", sys.exc_info()[0])
                pass
            except:
                print("Unexpected error:", sys.exc_info()[0])
        else:
            print("Database does not exist.")

    def releaseLock(self, id):
        sql_lock_release = '''
           DELETE FROM {tn} WHERE {id} = ?
        '''.format(
            tn = self.__TABLE_LOCK['name'],
            id = self.__TABLE_LOCK['fields']['id']['name']
        )
        conn = sqlite3.connect(self.__DB_FILE)
        cursor = conn.cursor()
        cursor.execute(sql_lock_release, (self.sql_escape_str(id), ))
        # cursor.execute("VACUUM")
        conn.commit()
        conn.close()


    def getURLSNotAnalyzedByAcheckerWithLock(self, limit = 10):
        sql = '''
            SELECT * FROM {tbl_univ}
            WHERE
            {tbl_uni_id} NOT IN (
                SELECT {tbl_ac_id} FROM {tbl_ac} UNION
                SELECT {tbl_lck_id} FROM {tpl_lck}
            )
            ORDER BY {tbl_uni_id}
            LIMIT ?
        '''.format(
            tbl_univ = self.__TABLE_UNIVERSITIES['name'],
            tbl_uni_id = self.__TABLE_UNIVERSITIES['fields']['id']['name'],
            tbl_ac = self.__TABLE_ACHECKER['name'],
            tbl_ac_id = self.__TABLE_ACHECKER['fields']['id']['name'],
            tpl_lck = self.__TABLE_LOCK['name'],
            tbl_lck_id = self.__TABLE_LOCK['fields']['id']['name']
        )

        sql_lock = '''
           INSERT INTO {tn} ({id}) VALUES (?)
        '''.format(
            tn = self.__TABLE_LOCK['name'],
            id = self.__TABLE_LOCK['fields']['id']['name']
        )

        result = None

        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                cursor.execute(sql, (self.sql_escape_str(limit), ))
                result = cursor.fetchall()
                if len(result) > 0:
                    for url in result:
                        cursor.execute(sql_lock, (self.sql_escape_str(url[0]), ))
                conn.commit()
                conn.close()
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
        else:
            print("Database does not exist.")

        return result
