# -*- coding: utf-8 -*-
"""
The SQLite manager.
Use for run easily mysql requests
"""

import sqlite3
from kb_package.database.basedb import BaseDB
from kb_package.tools import INFINITE


class SQLiteDB(BaseDB):

    @staticmethod
    def connect(file_name="database.db", **kwargs) -> sqlite3.Connection:
        """
        Making the connexion to the mysql database
        Args:
            file_name: str, file name path
        Returns: the connexion object reach

        """
        try:
            return sqlite3.connect(file_name)
        except Exception as ex:
            raise Exception(
                "Une erreur lors que la connexion à la base de donnée: "
                + str(ex)
            )

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def _execute(cursor, script, params=None, ignore_error=False, **kwargs):
        """
        use to make preparing requests
        Args:
            cursor: mysql.connector.cursor.MySQLCursor
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool

        Returns: mysql.connector.cursor.MySQLCursor, the cu

        """
        args = [script]
        if params is None:
            pass
        elif isinstance(params, (tuple, list)):
            params = tuple(params)
            args.append(params)

        else:
            params = (params,)
            args.append(params)
        try:
            cursor.execute(*args)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise Exception(ex)

    @staticmethod
    def get_all_data_from_cursor(cursor, limit=INFINITE):

        data = []
        try:
            data = cursor.fetchall()
            if limit.__class__ == INFINITE.__class__:
                pass
            else:
                data = data[:limit]
        except (Exception, sqlite3.ProgrammingError):
            pass
        if limit == 1 and len(data):
            return data[0]
        return data

    @staticmethod
    def prepare_insert_data(data: list):
        return ["?" for _ in data]


if __name__ == '__main__':
    import os
    from kb_package.tools import read_json_file
    import json
    DATABASE_FOLDER = r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\CVM\Push SMS\Databases"
    RECENT_DATA_FOLDER = os.path.join(DATABASE_FOLDER, "recent")
    PATH = r"C:\Users\FBYZ6263\Documents\WORK_FOLDER\CVM\Push SMS\pywork"
    CONFIG_PATH = os.path.join(PATH, "config")
    DB_PATH = os.path.join(CONFIG_PATH, "db.json")
    PLANNING_PATH = os.path.join(CONFIG_PATH, "planning.json")
    SQLITE_DB = os.path.join(CONFIG_PATH, "database.db")

    def _get_database_files():
        db_files = read_json_file(DB_PATH, [])

        last_file_in_database = [d["name"].lower() for d in db_files
                                 if d.get("folder", "").lower() == DATABASE_FOLDER.lower()]

        try:
            max_id = max([d["id"] for d in db_files])
        except ValueError:
            max_id = 0

        for index, file in enumerate(os.listdir(DATABASE_FOLDER)):
            if os.path.isdir(os.path.join(DATABASE_FOLDER, file)):
                continue
            if file.lower() in last_file_in_database:
                continue
            db_files.append(
                {
                    "id": max_id + index,
                    "source": os.path.join(DATABASE_FOLDER, file),
                    "folder": DATABASE_FOLDER,
                    "name": file,
                    "type": os.path.splitext(file)[1][1:],
                    "operations": []
                })
        with open(DB_PATH, "w") as config:
            config.writelines(json.dumps(db_files, indent=4))
        return db_files
    db_object = SQLiteDB(file_name=SQLITE_DB)
    db_object.run_script("""
            create table if not exists file_object(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source text,
                folder text,
                filename varchar(1000),
                `type` varchar(10),
                modification_date date,
                operations text
            );""")
    db_object.run_script("""
            create table if not exists campaign(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name text,
                max_size int
            );
""")
    db_object.run_script("""
            create table if not exists day_plan(
                day date PRIMARY KEY,
                day_nb int,
                weekday_str varchar(15)            
            );
            """)
    db_object.run_script("""
            create table if not exists plan(
                day_date date REFERENCES day_plan(day) ON DELETE CASCADE,
                file_id INT REFERENCES file_object(id) ON DELETE CASCADE,
                filename varchar(1000),
                `select` text,
                names text         
            );    
        """)
    print("*"*100)
    data = []
    for f in _get_database_files():
        f.pop("id")
        f["filename"] = f.pop("name")
        f["operations"] = json.dumps(f["operations"])
        data.append(f)
    print(data)
    db_object.insert_many(data, "file_object")