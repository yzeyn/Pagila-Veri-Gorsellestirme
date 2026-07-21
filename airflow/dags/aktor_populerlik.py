from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import \
    SQLExecuteQueryOperator  # type: ignore

from airflow import DAG

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='aktor_populerlik',
    default_args=default_args,
    description='Aktörlerin oynadığı filmlerden elde edilen toplam hasılatı ve başarı sıralamasını hesaplar',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Aşama: Hedef Data Mart Tablosunu Oluştur
    tablo_olustur = SQLExecuteQueryOperator(
        task_id='create_aktor_populerlik',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS dm_actor_rankings (
                actor_id INT PRIMARY KEY,
                aktor_adi VARCHAR(150),
                toplam_film_sayisi INT,
                toplam_hasilat NUMERIC(10,2),
                aktor_siralamasi INT,
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # 2. Aşama: E-T-L (Extract - Transform - Load) İşlemi
    aktor_hasilat_yukle = SQLExecuteQueryOperator(
        task_id='calculate_actor_rankings_and_load',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE dm_actor_rankings;
            
            INSERT INTO dm_actor_rankings (
                actor_id, 
                aktor_adi, 
                toplam_film_sayisi, 
                toplam_hasilat, 
                aktor_siralamasi
            )
            WITH aktor_hasilat AS (
                SELECT 
                    a.actor_id,
                    CONCAT(a.first_name, ' ', a.last_name) AS aktor_adi,
                    COUNT(DISTINCT fa.film_id) AS toplam_film_sayisi,
                    ROUND(SUM(p.amount), 2) AS toplam_hasilat
                FROM actor a
                JOIN film_actor fa ON a.actor_id = fa.actor_id
                JOIN film f ON fa.film_id = f.film_id
                JOIN inventory i ON f.film_id = i.film_id
                JOIN rental r ON i.inventory_id = r.inventory_id
                JOIN payment p ON r.rental_id = p.rental_id
                GROUP BY a.actor_id, a.first_name, a.last_name
            )
            SELECT 
                actor_id,
                aktor_adi,
                toplam_film_sayisi,
                toplam_hasilat,
                -- Hasılat miktarına göre aktörleri 1'den başlayarak sıralıyoruz (Transform)
                ROW_NUMBER() OVER (ORDER BY toplam_hasilat DESC) AS aktor_siralamasi
            FROM aktor_hasilat;
        """,
    )

    tablo_olustur >> aktor_hasilat_yukle