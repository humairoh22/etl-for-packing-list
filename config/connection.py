import psycopg2
from config import setup
from sqlalchemy import create_engine
from dotenv import load_dotenv
import mysql.connector as mysql
from mysql.connector import Error
import os


load_dotenv()

def test_engine_mysql():

    DB_HOSTNAME_DEMO = os.getenv("DB_HOSTNAME_TEST")
    DB_NAME_DEMO = os.getenv("DB_NAME")
    DB_USERNAME_DEMO = os.getenv("DB_USERNAME_TEST")
    DB_PASSWORD_DEMO = os.getenv("DB_PASSWORD_TEST")
    
    conn = mysql.connect(host = DB_HOSTNAME_DEMO,
                         user = DB_USERNAME_DEMO,
                         password = DB_PASSWORD_DEMO,
                         db = DB_NAME_DEMO)
    

    return conn


