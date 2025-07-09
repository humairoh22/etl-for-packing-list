import psycopg2
from config import setup
from sqlalchemy import create_engine
from dotenv import load_dotenv
import mysql.connector as mysql
from mysql.connector import Error
import os


load_dotenv()

def dwh_engine():
    db = setup.config_postgre['dbname']
    username = setup.config_postgre['user']
    password = setup.config_postgre['password']
    host = setup.config_postgre['host']
    port = setup.config_postgre['port']
    
    engine_load = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db}")

    return engine_load

def demo_engine():

    DB_HOSTNAME_DEMO = os.getenv("DB_HOSTNAME_DEMO")
    DB_NAME_DEMO = os.getenv("DB_NAME_DEMO")
    DB_USERNAME_DEMO = os.getenv("DB_USERNAME_DEMO")
    DB_PASSWORD_DEMO = os.getenv("DB_PASSWORD_DEMO")
    
    conn = mysql.connect(host = DB_HOSTNAME_DEMO,
                         user = DB_USERNAME_DEMO,
                         password = DB_PASSWORD_DEMO,
                         db = DB_NAME_DEMO)
    

    return conn

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

def test_engine():
    DB_TEST_HOSTNAME = os.getenv("DB_TEST_HOSTNAME")
    DB_TEST_NAME = os.getenv("DB_TEST_NAME")
    DB_TEST_USERNAME = os.getenv("DB_TEST_USERNAME")
    DB_TEST_PASSWORD = os.getenv("DB_TEST_PASSWORD")
    DB_TEST_PORT = os.getenv("DB_TEST_PORT")

    test_engine = create_engine(f"postgresql://{DB_TEST_USERNAME}:{DB_TEST_PASSWORD}@{DB_TEST_HOSTNAME}:{DB_TEST_PORT}")

    return test_engine
