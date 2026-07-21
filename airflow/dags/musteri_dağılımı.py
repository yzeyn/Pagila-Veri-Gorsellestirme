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
    dag_id='musteri_dagilimi',
    default_args=default_args,
    description='Müşteriler için satın alma alışkanlıklarını hesaplar',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    tablo_olustur = SQLExecuteQueryOperator(
        task_id='musteri_segmentleri_tablosunu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS musteri_segmentleri (
                musteri_id INT PRIMARY KEY,
                musteri_adi VARCHAR(150),
                recency_gun INT,
                frequency_adet INT,
                tutar NUMERIC(10,2),
                rfm_segment VARCHAR(50),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )
    
    rfm_hesapla_ve_yukle = SQLExecuteQueryOperator(
        task_id='rfm_hesapla_ve_yukle',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE musteri_segmentleri;
            
            INSERT INTO musteri_segmentleri (
                musteri_id, 
                musteri_adi, 
                recency_gun, 
                frequency_adet, 
                tutar, 
                rfm_segment
            )
            WITH rfm_ham_veri AS (
                SELECT 
                    c.customer_id AS musteri_id,
                    CONCAT(c.first_name, ' ', c.last_name) AS musteri_adi,
                    EXTRACT(DAY FROM ((SELECT MAX(rental_date) FROM rental) - MAX(r.rental_date)))::INT AS recency_gun,
                    COUNT(DISTINCT r.rental_id) AS frequency_adet,
                    ROUND(SUM(p.amount), 2) AS tutar
                FROM customer c
                JOIN rental r ON c.customer_id = r.customer_id
                JOIN payment p ON r.rental_id = p.rental_id
                GROUP BY c.customer_id, c.first_name, c.last_name
            ),
            rfm_skorlar AS (
                SELECT *,
                    -- Recency: Son ziyareti en yakın olan (recency_gun küçük olan) 3 puan alır
                    NTILE(3) OVER (ORDER BY recency_gun DESC) AS r_score,
                    NTILE(3) OVER (ORDER BY frequency_adet ASC) AS f_score,
                    NTILE(3) OVER (ORDER BY tutar ASC) AS m_score
                FROM rfm_ham_veri
            )
            SELECT 
                musteri_id,
                musteri_adi,
                recency_gun,
                frequency_adet,
                tutar,
                CASE 
                    WHEN r_score = 3 AND f_score = 3 THEN 'Sadık Müşteri (Champion)'
                    WHEN r_score = 3 AND f_score < 3 THEN 'Yeni Müşteri'
                    WHEN r_score = 1 AND f_score = 1 THEN 'Kayıp / Riskli Müşteri'
                    WHEN r_score = 1 AND f_score > 1 THEN 'Uykuda / Geri Kazanılabilir'
                    ELSE 'Normal Aktif Müşteri'
                END AS rfm_segment
            FROM rfm_skorlar;
        """,
    )

    tablo_olustur >> rfm_hesapla_ve_yukle