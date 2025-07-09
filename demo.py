import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv
from roman import toRoman
from utils.upsert import upsert_mysql
from datetime import datetime


def extract(path, dtype=None, skip_rows=None):

    """
    Untuk mengekstrak data dari Excel dan Google Sheet

    params:
    -------
    path : lokasi file atau URL data source
    dtype : dictionary, opsional
            menentukan tipe data suatu kolom saat di ekstrak
    skip_rows : jumlah rows atas yang pengen di skip
    
    """
    

    # ini untuk baca data dari google sheet
    if path.startswith('https://docs.google.com/'):
        df = pd.read_csv(path, dtype=dtype)

        return df
    
    # ini untuk baca data excel yang tipe filenya xls atau xlsx
    elif path.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(path, skiprows=skip_rows)
        
        return df
    
def transform_logbook(logbook):

    """
    Untuk mentransformasi file logbook MT

    params:
    ------
    logbook: DataFrame logbook

    """

    logbook = logbook.copy()
    logbook.columns = logbook.columns.str.replace(' ', '_')
    logbook.columns = logbook.columns.str.lower()

    date_col = ['po_expired', 'so_date']
    for col in date_col:
        logbook[col] = pd.to_datetime(logbook[col], format= '%m/%d/%Y', errors='coerce')

    # ekstrak bulan dan tahun dari so_date
    logbook["year"] = logbook["so_date"].dt.year.astype(str).str[-2:].astype('str')
    logbook["month"] = logbook["so_date"].dt.month 

    # buat penamaan bulan jadi bilangan romawi
    logbook["month_roman"] = logbook["month"].apply(lambda x: toRoman(x))

    # buat pattern nomor SO dari gabungan tahun, bulan romawi, dan 4 digit nomor SO
    logbook['no_so'] = logbook.apply(lambda x: '/'.join(['SO/SMR/', x['segment'], x['year'], x['month_roman'], x['so_num']]), axis=1)

    logbook = logbook[['no_so', 'po_expired']]

    # drop duplicate values berdasarkan no_so
    logbook = logbook.drop_duplicates(subset='no_so')
    
    return logbook

def transform_accurate(df, logbook):

    """
    Untuk mentransformasi data Accurate. Hasil akhir berupa dataframe gabungan data Accurate dan Logbook yang sudah di transform

    params:
    -------

    df : DataFrame Accurate
    logbook : DataFrame logbook yang udah ditransform
    """

    drop_unnamed_col = [col for col in df.columns if 'Unnamed' in col]
    df = df.drop(drop_unnamed_col, axis=1)
    df.columns = df.columns.str.replace(' ', '_')

    # ubah kolom tanggal SO dan tanggal SJ menjadi tipedf date
    date_col = ['tgl_pesan', 'tgl_faktur']
    for col in date_col:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%y', errors='coerce')

    # ubah value di kolom alamat, kota, provinsi menjadi huruf besar
    address_col = ['alamat', 'kota', 'propinsi']
    for col in address_col:
        df[col] = df[col].str.upper()


    df['kota'] = df['kota'].str.replace(r'^(KABUPATEN|KAB\.)\s*', 'KAB ', regex=True)
    df['term_payment'] = df['term_payment'].replace({'C.O.D':'COD', 'Net':'NET', 'net':'NET'}, regex=True)
    
    MAPPINGNAME = {', TBK':' Tbk', ', Tbk':' Tbk', 'TBK':'Tbk'}
    df['nama_customer'] = df['nama_customer'].replace(MAPPINGNAME, regex=True)
    df['nama_customer'] = df['nama_customer'].str.replace(',', '.')

    #mengeluarkan df yang menggunakan apotek panel
    condition = ((df["nama_customer"].str.contains("SUMBER SARI") & df["nama_penjual"].isnull()) |
            (df["nama_customer"].str.contains("AIMAR") & df["nama_penjual"].isnull()) |
            (df["nama_customer"].str.contains("SWADAYA SEHAT") & df["nama_penjual"].isnull())
            )                
    df = df[~condition]

    #mengeluarkan df sw yang terdapat keterangan DIY
    df = df[~df['keterangan'].str.contains('DIY', na=False)]

    #buat kolom baru untuk mengisi nilai dari gabungan no faktur, nama customer, dan kota
    df['concat_invoice'] = df.apply(lambda x: '_'.join([x['no_faktur'], x['nama_customer'], x['kota']]) if pd.notnull(x['no_faktur']) 
                                            else None, axis=1)
    
    df = pd.merge(df, logbook, how='left', left_on='no_pesan', right_on='no_so')
    
    df = df.drop_duplicates(subset='no_pesan')
    


    return df


def load_to_web(df):
    df = df.copy()

    group_col_preorder = ['no_faktur', 'no_po', 'id_customer', 'tgl_faktur', 'po_expired', 'nama_penjual', 'term_payment', 'concat_invoice']

    df_preorder = df.groupby(group_col_preorder, as_index=False, dropna=False).agg({'total_harga':'sum'})

    df_preorder = df_preorder.rename(columns={'no_faktur':'no_invoice', 'id_customer':'customer_id', 'tgl_faktur':'order_time', 'nama_penjual':'sales_name', 'total_harga':'value_invoice'})
    
    
    last_col = ['concat_invoice']
    ordered_cols = [col for col in df_preorder.columns if col not in last_col]
    ordered_cols += last_col

    df_preorder = df_preorder.reindex(ordered_cols, axis= 1)

    df_preorder = df_preorder.dropna(subset=['no_invoice'])
    df_preorder = df_preorder.replace({pd.NaT:None, np.nan:None})

    preorder_cols_to_update = ['concat_invoice', 'customer_id', 'order_time']

    upsert_mysql(df_preorder, 'preorder', preorder_cols_to_update)


if __name__ == "__main__":

    DIR_SMR_JKT = os.getenv("DIR_SMR_JKT")
    DIR_SMR_DIY = os.getenv("DIR_SMR_DIY")
    DIR_PANEL = os.getenv("DIR_SW")
    PATH_LOGBOOK = os.getenv("PATH_LOGBOOK")

    smr_jkt = extract(DIR_SMR_JKT, skip_rows=4)
    logbook = extract(PATH_LOGBOOK, dtype={'SO NUM':str})
    logbook = transform_logbook(logbook)
    smr_jkt_cleaned = transform_accurate(smr_jkt, logbook)

    load_to_web(smr_jkt_cleaned)