from config.connection import test_engine_mysql
import mysql.connector as mysql
from mysql.connector import Error

def upsert_mysql(df, table_name, col_to_update: list[str], schema=None):

    """
    Untuk insert dan update data di database mysql

    params:
    -------

    df : Nama DataFrame
    table_name : nama table target
    col_to_update : nama nama kolom di table yang ingin diupdate datanya
    schema : nama schema target

    """


    try:
        conn = test_engine_mysql()

        if conn.is_connected():
            print("Connected to MySQL database.")

            with conn.cursor() as cursor:

                table_spec = ""

                if schema:
                    table_spec += schema + "."
                
                table_spec += table_name

                df_columns = list(df.columns)

                # if col_to_update is None:
                #     col_to_update = [col for col in df_columns if col not in]

                insert_col_list = ", ".join([f"{col_name}" for col_name in df_columns])
                n_vals_insert = ", ".join(['%s'] * len(df_columns))
                update_stmt = ", ".join([f"{col} = VALUES({col})" for col in col_to_update])
                values = [tuple(row) for row in df.values]


                stmt = f"""
                        INSERT INTO {table_spec} \n
                        ({insert_col_list})\n
                        VALUES ({n_vals_insert}) \n
                        ON DUPLICATE KEY UPDATE \n
                        {update_stmt};
                        """
                
                cursor.executemany(stmt, values)

                print(f'Inserted or update {cursor.rowcount} rows into {table_name}')
                conn.commit()

                conn.close()
                print("MySQL connection is closed.")
                print("\n")

                return True
    except Error as e:
        print(f'Error when loading data to {table_name}: {e}')

        return False
