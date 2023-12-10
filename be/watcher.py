import pymysql
import schedule
import time
from datetime import datetime


def delete_expired_data():
    db = pymysql.connect(host='localhost', user='root', passwd='root', port=3306, database='bookstore')
    cursor = db.cursor()

    search_query = """
        SELECT order_id, store_id FROM new_order
        WHERE TTL IS NOT NULL 
        AND TTL < %s
    """
    cursor.execute(
        search_query, (datetime.now(),),
    )

    for order_id, store_id in cursor.fetchall():
        cursor.execute(
            "SELECT book_id, count FROM new_order_detail WHERE order_id = %s",
            (order_id,),
        )
        for row in cursor.fetchall():
            book_id = row[0]
            count = row[1]
            update_query = """
                UPDATE store SET stock_level = stock_level + %s
                WHERE store_id = %s 
                AND book_id = %s 
            """
            cursor.execute(
                update_query, (count, store_id, book_id),
            )

        cursor.execute(
            "DELETE FROM new_order_detail WHERE order_id=%s;",
            (order_id,),
        )
        cursor.execute(
            "DELETE FROM new_order WHERE order_id=%s;",
            (order_id,),
        )
    cursor.connection.commit()

    cursor.close()
    db.close()


# 每小时定时执行
schedule.every().hour.do(delete_expired_data)

# 保持程序运行
while True:
    schedule.run_pending()
    time.sleep(1)
