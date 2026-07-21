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
    dag_id='populer_kategoriler',
    default_args=default_args,
    description='Film kategorilerinin popülerliğini ve güncel stok durumlarını analiz eder',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    tablo_olustur = SQLExecuteQueryOperator(
        task_id='populer_kategoriler_tablosunu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS populer_kategoriler (
                kategori_adi VARCHAR(50) PRIMARY KEY,
                toplam_film_sayisi INT,
                toplam_kiralama_sayisi INT,
                toplam_stok_adet INT,
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    populerlik_ve_stok_yukle = SQLExecuteQueryOperator(
        task_id='calculate_popularity_and_stock_load',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE populer_kategoriler;
            
            INSERT INTO populer_kategoriler (
                kategori_adi, 
                toplam_film_sayisi, 
                toplam_kiralama_sayisi, 
                toplam_stok_adet
            )
            SELECT 
                c.name AS kategori_adi,
                COUNT(DISTINCT fc.film_id) AS toplam_film_sayisi,
                COUNT(r.rental_id) AS toplam_kiralama_sayisi,
                COUNT(DISTINCT i.inventory_id) AS toplam_stok_adet
            FROM category c
            JOIN film_category fc ON c.category_id = fc.category_id
            JOIN film f ON fc.film_id = f.film_id
            LEFT JOIN inventory i ON f.film_id = i.film_id
            LEFT JOIN rental r ON i.inventory_id = r.inventory_id
            GROUP BY c.name;
        """,
    )

    tablo_olustur >> populerlik_ve_stok_yukle