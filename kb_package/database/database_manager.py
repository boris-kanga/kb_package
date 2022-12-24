import importlib


class DatabaseManager:
    MYSQL = "mysql"
    MONGO = "mongo"
    POSTGRES = "postgres"

    def __new__(cls, db_type: str = "mysql", uri=None, **kwargs):
        """
            kwargs:
                user: username
                pwd | password: the connexion password
                port: default 3306 (the MySQL default port)
                host: default localhost
                db_name


        """
        name = ("." if __package__ else "") + db_type.lower() + "db"

        module = importlib.import_module(name,
                                         package=__package__)
        db_class = getattr(module, db_type.capitalize() + "DB")

        assert db_class, "Don't find"
        return db_class(uri, **kwargs)

    def set_logger(self, logger):
        self.db_object.set_logger(logger)

    def __getattr__(self, item):
        return getattr(self.db_object, item)


if __name__ == '__main__':
    uri = "postgres://lzzczmkaldakxe:cec000295de8a66be768571" \
          "c3226e38077f4f996ecb4b8f94fc30be727d23241@ec2-54-172-175-" \
          "251.compute-1.amazonaws.com:5432/d5cl4si7lr3ucn"

    model = DatabaseManager(DatabaseManager.POSTGRES, host="78.138.45.195",
                            db_name="portfolio_db",
                            user="admin",
                            password="B@ris1996")
    print(model.get_cursor())
