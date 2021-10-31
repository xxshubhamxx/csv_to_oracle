import cx_Oracle
import csv,os
import config as cfg


def create_table():
    with cx_Oracle.connect(cfg.username, cfg.password, cfg.dsn, encoding=cfg.encoding) as con:
        with con.cursor() as cursor:
            try:
                cursor.execute("CREATE TABLE TEST (id INTEGER, name VARCHAR(255))")
            except Exception as e:
                print(e)


def csv_to_oracle():
    with cx_Oracle.connect(cfg.username, cfg.password, cfg.dsn, encoding=cfg.encoding) as con:
        # create a cursor
        with con.cursor() as cursor:
            # Predefine the memory areas to match the table definition
            # cursor.setinputsizes(None, 25)
            # Adjust the batch size to meet your memory and performance requirements
            batch_size = 10000
            n = os.path.dirname(__file__)
            with open( f'{n}/testsp.csv', 'r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                sql = "insert into test (id,name) values (:1, :2)"
                data = []
                for line in csv_reader:
                    # print(line)
                    data.append((line[0], line[1]))
                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data)
                        data = []
                if data:
                    cursor.executemany(sql, data)
                con.commit()


def oracle_output():
    with cx_Oracle.connect(cfg.username, cfg.password, cfg.dsn, encoding=cfg.encoding) as con:
        # create a cursor
        with con.cursor() as cursor:
            cursor.execute("SELECT * FROM TEST")
            a = cursor.fetchall()
            for row in a:
                print(row)

def delete_table_if_exists():
    with cx_Oracle.connect(cfg.username, cfg.password, cfg.dsn, encoding=cfg.encoding) as con:
        # create a cursor
        with con.cursor() as cursor:
            try:
                cursor.execute("Drop table test")
            except Exception as e:
                print(e)


if __name__ == '__main__':
    delete_table_if_exists()
    create_table()
    csv_to_oracle()
    oracle_output()
    delete_table_if_exists()
