from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import glob
import sqlite3

def clean_df(df):
    # date
    df['issue_d'] = pd.to_datetime(df['issue_d'], dayfirst= True)

    # category
    cat_column = ['home_ownership', 'income_category','grade', 'application_type']
    df[cat_column] = df[cat_column].astype('category')
    return df

def fetch_clean():
    database = 'db/loan.db'
    conn = sqlite3.connect(database)
    files = glob.glob('loan_2014/loan_2014/*.csv')

    df_list = []

    for file in files:
        # membaca dataframe
        loan = pd.read_csv(file)
        # membersihkan dataframe
        loan_clean = clean_df(loan)

        # mengambil transaksi terkini dari database
        last_update = pd.read_sql_query("""
                    SELECT issue_d 
                    FROM loan
                    ORDER BY issue_d DESC
                    LIMIT 1
                    """, con = conn)
        last_timestamp = last_update['issue_d'].to_string(index=False)

        # filtering dataframe
        loan_clean_update = loan_clean[loan_clean['issue_d'] > last_timestamp]
        
        # jika dataframe terisi, maka masukkan ke dalam df_list
        if loan_clean_update.shape[0] != 0:
            df_list.append(loan_clean_update)
    
    return df_list

def df_to_db(ti):
    database = 'db/loan.db'
    conn = sqlite3.connect(database)
    df_list = ti.xcom_pull(task_ids = 'fetch_clean_task')

    for df in df_list:
        df.to_sql(name = 'loan',
              con = conn,
              if_exists = 'append',
              index =  False)
        print("Done Created DB")

def report_generator(ti):

    df_list = ti.xcom_pulll(task_ids = 'fetch_clean_task')

    for df in df_list:
        # Mengambil periode bulan, untuk nama file
        periode = df['issue_d'].dt.to_period('M').unique()[0]

        # menghitung jumlah pinjaman berdasar kondisi
        condtion_on_loan = pd.crosstab(index = df['loan_condition'],
            columns = 'Jumlah Pinjaman').sort_values(by = 'Jumlah Pinjaman')
        
        # menghitung total pinjaman
        total_loan = pd.crosstab(index = df['loan_condition'],
            columns = 'Total Loan',
            values = df['loan_amount'],
            aggfunc = 'sum'
            ).sort_values(by = 'Total Loan')
        
        # menyimpan ke dalam file excel
        with pd.ExcelWriter(f'report/{periode}.xlsx') as writer:
            condtion_on_loan.to_excel(writer, sheet_name = 'Frekuensi Kondisi Pinjaman')
            total_loan.to_excel(writer, sheet_name = 'Total Pinjaman')
            print(f"Berhasil membuat report: report/{periode}.xlsx")

with DAG("lbb_dag", # Dag id
         start_date = datetime(2024, 9, 3), # Berjalan mulai 29
        #  schedule_interval = '*/5 * * * *', # setiap 5 menit
        schedule_interval = '@monthly', # setiap bulan
        catchup = False):
    
    fetch_clean_task = PythonOperator(
        task_id = 'fetch_clean_task',
        python_callable = fetch_clean
    )

    df_to_db_task = PythonOperator(
        task_id = 'df_to_db_task',
        python_callable = df_to_db
    )

    report_generator_task = PythonOperator(
        task_id = 'report_generator_task',
        python_callable = report_generator
    )
    fetch_clean_task >> [df_to_db_task, report_generator_task]

# baru nontonsampe 1:58