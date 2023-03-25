import pandas as pd
import psycopg2 as psycopg2
import sqlalchemy as sa

#Lay doi tuong connection de thuc hien cac cau lenh SQL
def sql_conn():
   conn = psycopg2.connect(
       database="test",
       user="postgres",
       password="admin",
       host="localhost",
       port="5432")
   return conn

# Import du lieu tu file csv vao CSDL PostgreSQL
def save_csv_database(filename, table_name, if_exists='replace'):
    # Co the su dung if_exists='append' de them du lieu vao cuoi bang da co
    engine = sa.create_engine('postgresql://postgres:admin@localhost:5432/test')
    df = pd.read_csv(filename)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print('Done', df.shape)

#Ham nay dung de lien ket 2 bang table1 va table2 de tao ra bang moi table3
def join_tables(table1, table2, key, table3):
    conn = sql_conn()
    mycursor = conn.cursor()
    # SQL for PostgreSQL
    sql = "CREATE TABLE "+table3+" AS SELECT * FROM "+table1+" INNER JOIN " + table2 +\
          " USING ("+key+")"
    # print(sql)
    mycursor.execute(sql)
    conn.commit()
    print('Done')

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # save_csv_database('product_id_ncds.csv','product_id_ncds')
    # save_csv_database('crawled_data_ncds.csv', 'crawled_data_ncds')
    # save_csv_database('comments_data_ncds.csv', 'comments_data_ncds')
    # join_tables("crawled_data_ncds", "product_id_ncds", "product_id", "crawled_data_products_id")
    # join_tables("comments_data_ncds", "product_id_ncds", "product_id", "comments_data_products_id")
    # join_tables("comments_data_ncds", "crawled_data_ncds", "product_id", "comments_data_crawled_data")
    # Truong hop neu them du lieu moi vao bang da co thi su dung them if_exists='append'
    # save_csv_database('crawled_data_ncds1.csv', 'crawled_data_ncds', if_exists='append')
    # save_csv_database('product_id_ncds1.csv', 'product_id_ncds', if_exists='append')
    # save_csv_database('comments_data_ncds1.csv', 'comments_data_ncds', if_exists='append')
