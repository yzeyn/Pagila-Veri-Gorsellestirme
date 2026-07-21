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
    dag_id='geciken_iadeler',
    default_args=varsayilan_ayarlar,
    description='İadesi geciken filmleri ve bunların ceza tutarlarını analiz eder',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:


    tablo_olusturma_gorevi = SQLExecuteQueryOperator(
        task_id='geciken_iadeler_tablosu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS geciken_iadeler (
                kiralama_id INT PRIMARY KEY,
                musteri_adi VARCHAR(150),
                film_adi VARCHAR(255),
                kiralama_tarihi TIMESTAMP,
                beklenen_iade_tarihi TIMESTAMP,
                gercek_iade_tarihi TIMESTAMP,
                gecikme_gun_sayisi INT,
                tahmini_ceza_tutari NUMERIC(10,2),
                durum VARCHAR(50),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )
    
    veri_isle_ve_yukle_gorevi = SQLExecuteQueryOperator(
        task_id='gecikten_iadeleri_hesapla_ve_yukle',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE geciken_iadeler;
            
            INSERT INTO geciken_iadeler (
                kiralama_id, 
                musteri_adi, 
                film_adi, 
                kiralama_tarihi, 
                beklenen_iade_tarihi, 
                gercek_iade_tarihi, 
                gecikme_gun_sayisi, 
                tahmini_ceza_tutari, 
                durum
            )
            WITH kiralama_detaylari AS (
                SELECT 
                    r.rental_id AS kiralama_id,
                    CONCAT(c.first_name, ' ', c.last_name) AS musteri_adi,
                    f.title AS film_adi,
                    r.rental_date AS kiralama_tarihi,
                    r.rental_date + (f.rental_duration || ' days')::INTERVAL AS beklenen_iade_tarihi,
                    r.return_date AS gercek_iade_tarihi,
                    f.replacement_cost AS filmin_degeri
                FROM rental r
                JOIN customer c ON r.customer_id = c.customer_id
                JOIN inventory i ON r.inventory_id = i.inventory_id
                JOIN film f ON i.film_id = f.film_id
            ),
            gecikme_hesaplama AS (
                SELECT *,
                    EXTRACT(DAY FROM (COALESCE(gercek_iade_tarihi, NOW()) - beklenen_iade_tarihi))::INT AS gun_farki
                FROM kiralama_detaylari
            )
            SELECT 
                kiralama_id,
                musteri_adi,
                film_adi,
                kiralama_tarihi,
                beklenen_iade_tarihi,
                gercek_iade_tarihi,
                CASE WHEN gun_farki > 0 THEN gun_farki ELSE 0 END AS gecikme_gun_sayisi,
                CASE WHEN gun_farki > 0 THEN LEAST(ROUND(gun_farki * 1.50, 2), filmin_degeri) ELSE 0.00 END AS tahmini_ceza_tutari,
                CASE 
                    WHEN gercek_iade_tarihi IS NULL AND gun_farki > 0 THEN 'Hala Gecikmede'
                    WHEN gercek_iade_tarihi IS NOT NULL AND gun_farki > 0 THEN 'Gecikmeli İade Edildi'
                    ELSE 'Zamanında İade Edildi'
                END AS durum
            FROM gecikme_hesaplama
            WHERE gun_farki > 0;
        """,
    )

    tablo_olusturma_gorevi >> veri_isle_ve_yukle_gorevi