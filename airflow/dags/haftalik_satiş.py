from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import \
    SQLExecuteQueryOperator  # type: ignore

from airflow import DAG

default_args = {
    'owner': 'veri_muhendisi',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='magaza_haftalik_performans',
    default_args=default_args,
    description='Mağaza bazlı haftalık ciro ve işlem sayısı performansını hesaplar',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    tablo_olustur = SQLExecuteQueryOperator(
        task_id='magaza_performans_tablosu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS magaza_haftalik_satis (
                islem_tarihi DATE,
                magaza_id INT,
                personel VARCHAR(100),
                toplam_ciro NUMERIC(10,2),
                toplam_islem_sayisi INT,
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (islem_tarihi, magaza_id)
            );
        """,
    )

    veri_isle_ve_yukle = SQLExecuteQueryOperator(
        task_id='magaza_performans_verilerini_yukle',
        conn_id='postgres_pagila',
        sql="""
            DELETE FROM magaza_haftalik_satis 
            WHERE islem_tarihi IN (SELECT DISTINCT DATE(payment_date) FROM payment);

            INSERT INTO magaza_haftalik_satis (
                islem_tarihi, 
                magaza_id, 
                personel, 
                toplam_ciro, 
                toplam_islem_sayisi
            )
            SELECT 
                DATE(p.payment_date) AS islem_tarihi,
                s.store_id AS magaza_id,
                CONCAT(st.first_name, ' ', st.last_name) AS personel,
                ROUND(SUM(p.amount), 2) AS toplam_ciro,
                COUNT(p.payment_id) AS toplam_islem_sayisi
            FROM payment p
            JOIN staff st ON p.staff_id = st.staff_id
            JOIN store s ON st.store_id = s.store_id
            GROUP BY DATE(p.payment_date), s.store_id, st.first_name, st.last_name;
        """,
    )

    tablo_olustur >> veri_isle_ve_yukle