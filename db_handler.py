# -*- coding:utf-8 -*-
# Database handler class for the World Universities database

import sys
import sqlite3
import os.path

class DbHandler(object):
    __TABLE_UNIVERSITIES = 'universities'
    __FIELD_ID = {'name': 'id', 'type': 'INTEGER', }
    __FIELD_URL = {'name': 'url', 'type': 'TEXT'}
    __FIELD_UNIVERSITY = {'name': 'name', 'type': 'TEXT'}
    __FIELD_COUNTRY_CODE = {'name': 'c_code', 'type': 'TEXT'}
    __FIELD_COUNTRY = {'name': 'country', 'type': 'TEXT'}
    # __FIELD_ERRORS = {'name': 'NumOfErrors', 'type': 'TEXT'} # NumOfErrors
    # __FIELD_LIKELY = {'name': 'NumOfLikelyProblems', 'type': 'TEXT'} # NumOfLikelyProblems
    # __FIELD_POTENTIAL = {'name': 'NumOfPotentialProblems', 'type': 'TEXT'} # NumOfPotentialProblems
    __INDEX_UNIQUE_URL = 'IDX_U_URL'

    def __init__(self,  db_file):
        self.__DB_FILE = db_file
        self.__createDatabase()

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
                    tn = self.__TABLE_UNIVERSITIES,
                    f1 = self.__FIELD_ID['name'], t1 = self.__FIELD_ID['type'],
                    f2 = self.__FIELD_URL['name'], t2 = self.__FIELD_URL['type'],
                    f3 = self.__FIELD_UNIVERSITY['name'], t3 = self.__FIELD_UNIVERSITY['type'],
                    f4 = self.__FIELD_COUNTRY_CODE['name'], t4 = self.__FIELD_COUNTRY_CODE['type'],
                    f5 = self.__FIELD_COUNTRY['name'], t5 = self.__FIELD_COUNTRY['type']
                    # f6 = self.__FIELD_ERRORS['name'], t6 = self.__FIELD_ERRORS['type'],
                    # f7 = self.__FIELD_LIKELY['name'], t7 = self.__FIELD_LIKELY['type'],
                    # f8 = self.__FIELD_POTENTIAL['name'], t8 = self.__FIELD_POTENTIAL['type']
                )
            )
            cursor.execute('CREATE UNIQUE INDEX {index} on {tn} ({cn})'.format(
                index = self.__INDEX_UNIQUE_URL,
                tn = self.__TABLE_UNIVERSITIES,
                cn = self.__FIELD_URL['name']
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

    def createURL(self, new_url):
        # Create new URL in DB
        if self.__DB_FILE <> None:
            try:
                conn = sqlite3.connect(self.__DB_FILE)
                cursor = conn.cursor()
                sql = "INSERT OR IGNORE INTO {tn} ({url}, {uni}, {cc}, {cty}) VALUES (?, ?, ?, ?)".\
                    format(
                        tn = self.__TABLE_UNIVERSITIES,
                        url = self.__FIELD_URL['name'],
                        uni = self.__FIELD_UNIVERSITY['name'],
                        cc = self.__FIELD_COUNTRY_CODE['name'],
                        cty = self.__FIELD_COUNTRY['name'],
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
