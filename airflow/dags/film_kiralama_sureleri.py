from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow import DAG

varsayilan_ayarlar = {
    'owner': 'veri_ekibi',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='dm_film_sure_trendleri_etl',
    default_args=varsayilan_ayarlar,
    description='Filmlerin süre gruplarına göre kiralama oranlarını analiz eder.',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    tablo_olustur_task = SQLExecuteQueryOperator(
        task_id='film_kiralama_sureleri_tablo_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS dm_film_length_trends (
                sure_grubu VARCHAR(20) PRIMARY KEY,
                film_sayisi INT,
                toplam_kiralama_sayisi INT,
                kiralama_yuzdesi NUMERIC(5,2),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    etl_yukle_task = SQLExecuteQueryOperator(
        task_id='film_sure_analizi_yukle',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE dm_film_length_trends;

            INSERT INTO dm_film_length_trends (
                sure_grubu, 
                film_sayisi, 
                toplam_kiralama_sayisi, 
                kiralama_yuzdesi
            )
            WITH film_gruplari AS (
                SELECT 
                    f.film_id,
                    CASE 
                        WHEN f.length < 90 THEN 'Kisa (<90 dk)'
                        WHEN f.length BETWEEN 90 AND 120 THEN 'Orta (90-120 dk)'
                        ELSE 'Uzun (>120 dk)'
                    END AS sure_grubu,
                    r.rental_id
                FROM film f
                LEFT JOIN inventory i ON f.film_id = i.film_id
                LEFT JOIN rental r ON i.inventory_id = r.inventory_id
            ),
            agregasyon AS (
                SELECT 
                    sure_grubu,
                    COUNT(DISTINCT film_id) AS film_sayisi,
                    COUNT(rental_id) AS toplam_kiralama_sayisi
                FROM film_gruplari
                GROUP BY sure_grubu
            )
            SELECT 
                sure_grubu,
                film_sayisi,
                toplam_kiralama_sayisi,
                ROUND(
                    (toplam_kiralama_sayisi::NUMERIC / NULLIF(SUM(toplam_kiralama_sayisi) OVER (), 0)) * 100, 
                    2
                ) AS kiralama_yuzdesi
            FROM agregasyon;
        """
    )

    tablo_olustur_task >> etl_yukle_task