import logging
import os
import pymysql


class Store:
    database: str

    def __init__(self, db_path):
        # self.database = os.path.join(db_path, "be.db")
        self.init_tables()

    def init_tables(self):
        try:
            cur = self.get_db_conn()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS `user`(
                    user_id VARCHAR(100) PRIMARY KEY,
                    password VARCHAR(100) NOT NULL,
                    balance INT NOT NULL,
                    token TEXT,
                    terminal TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS store(
                    store_id VARCHAR(100),
                    book_id VARCHAR(100),
                    book_info LONGTEXT,
                    stock_level INT,
                    PRIMARY KEY(store_id, book_id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_store(
                    user_id VARCHAR(100),
                    store_id VARCHAR(100),
                    PRIMARY KEY(user_id, store_id)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS new_order(
                    order_id VARCHAR(200) PRIMARY KEY,
                    user_id VARCHAR(100),
                    store_id VARCHAR(100),
                    status VARCHAR(9),
                    completion_time DATE,
                    TTL DATETIME
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS new_order_detail(
                    order_id VARCHAR(200),
                    book_id VARCHAR(100),
                    count INT,
                    price INT,
                    PRIMARY KEY(order_id, book_id)
                )
            """)

            cur.connection.commit()
        except pymysql.Error as e:
            logging.error(e)
            cur.connection.rollback()

    def get_db_conn(self):
        # return sqlite.connect(self.database)
        db = pymysql.connect(host='localhost', user='root', passwd='root', port=3306, database='bookstore')
        return db.cursor()


database_instance: Store = None


def init_database(db_path):
    global database_instance
    database_instance = Store(db_path)


def get_db_conn():
    global database_instance
    return database_instance.get_db_conn()

