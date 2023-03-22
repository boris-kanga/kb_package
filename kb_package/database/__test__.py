from kb_package.database import SQLiteDB


def main_test():
    db = SQLiteDB()
    db.run_script("""create table test (a integer)""")
    db.insert_many([{"a": i} for i in range(100)], "test")
    assert db.run_script("""select * from test""") == db.get_all_data_from_cursor(db._execute(db.get_cursor(), """select * from test"""))

    db.create_table(r"C:\Users\FBYZ6263\Downloads\categorie_inter.csv", "temp")
    print(db.run_script("""select * from temp""", export=True))

if __name__ == '__main__':
    main_test()