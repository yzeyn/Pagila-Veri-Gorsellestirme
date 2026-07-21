from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import \
    SQLExecuteQueryOperator  # type: ignore

from airflow import DAG

varsayilan_ayarlar = {
    'owner': 'veri_muhendisi',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='cografi_satis_dagilimi',
    default_args=varsayilan_ayarlar,
    description='Ülke ve şehir bazlı coğrafi satış ve müşteri dağılımını analiz eder',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    tablo_olusturma_gorevi = SQLExecuteQueryOperator(
        task_id='cografi_satis_tablosu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS cografi_satis_tablosu (
                ulke_adi VARCHAR(50),
                sehir_adi VARCHAR(50),
                toplam_musteri_sayisi INT,
                toplam_hasilat NUMERIC(10,2),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ulke_adi, sehir_adi)
            );
        """,
    )
    
    veri_isle_ve_yukle_gorevi = SQLExecuteQueryOperator(
        task_id='cografi_verileri_yukle',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE cografi_satis_tablosu;
            
            INSERT INTO cografi_satis_tablosu (
                ulke_adi, 
                sehir_adi, 
                toplam_musteri_sayisi, 
                toplam_hasilat
            )
            SELECT 
                co.country AS ulke_adi,
                ci.city AS sehir_adi,
                COUNT(DISTINCT cu.customer_id) AS toplam_musteri_sayisi,
                ROUND(SUM(p.amount), 2) AS toplam_hasilat
            FROM customer cu
            JOIN address a ON cu.address_id = a.address_id
            JOIN city ci ON a.city_id = ci.city_id
            JOIN country co ON ci.country_id = co.country_id
            JOIN payment p ON cu.customer_id = p.customer_id
            GROUP BY co.country, ci.city;
        """,
    )

    tablo_olusturma_gorevi >> veri_isle_ve_yukle_gorevi