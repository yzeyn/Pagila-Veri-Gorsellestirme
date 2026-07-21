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
    dag_id='musteri_dagilimi',
    default_args=default_args,
    description='Müşteriler için RFM skorlarını ve segmentlerini hesaplar',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Aşama: Hedef Data Mart Tablosunu Oluştur
    tablo_olustur = SQLExecuteQueryOperator(
        task_id='create_dm_customer_segments',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS dm_customer_segments (
                customer_id INT PRIMARY KEY,
                musteri_adi VARCHAR(150),
                recency_gun INT,
                frequency_adet INT,
                monetary_tutar NUMERIC(10,2),
                rfm_segment VARCHAR(50),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # 2. Aşama: RFM Hesaplama ve Segmentasyon (Extract - Transform - Load)
    # Pagila verisindeki tarihler eski olduğu için 'en son kiralama yapılan tarihi' 
    # baz alarak gün hesaplaması (Recency) yapıyoruz.
    rfm_hesapla_ve_yukle = SQLExecuteQueryOperator(
        task_id='calculate_rfm_and_load',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE dm_customer_segments;
            
            INSERT INTO dm_customer_segments (
                customer_id, 
                musteri_adi, 
                recency_gun, 
                frequency_adet, 
                monetary_tutar, 
                rfm_segment
            )
            WITH rfm_ham_veri AS (
                SELECT 
                    c.customer_id,
                    CONCAT(c.first_name, ' ', c.last_name) AS musteri_adi,
                    -- Recency: Müşterinin son kiralama tarihi ile veri setindeki en son kiralama tarihi arasındaki gün farkı
                    (SELECT EXTRACT(DAY FROM (MAX(rental_date) - MAX(r.rental_date))) FROM rental) AS recency_gun,
                    -- Frequency: Toplam kiralama sayısı
                    COUNT(DISTINCT r.rental_id) AS frequency_adet,
                    -- Monetary: Toplam ödeme miktarı
                    ROUND(SUM(p.amount), 2) AS monetary_tutar
                FROM customer c
                JOIN rental r ON c.customer_id = r.customer_id
                JOIN payment p ON r.rental_id = p.rental_id
                GROUP BY c.customer_id, c.first_name, c.last_name
            ),
            rfm_skorlar AS (
                SELECT *,
                    -- NTILE(3) ile müşterileri 1, 2, 3 şeklinde metriklere göre puanlıyoruz
                    -- Recency için düşük gün daha iyi olduğundan DESC yerine normal sıralıyoruz
                    NTILE(3) OVER (ORDER BY recency_gun ASC) AS r_score,
                    NTILE(3) OVER (ORDER BY frequency_adet DESC) AS f_score,
                    NTILE(3) OVER (ORDER BY monetary_tutar DESC) AS m_score
                FROM rfm_ham_veri
            )
            SELECT 
                customer_id,
                musteri_adi,
                recency_gun,
                frequency_adet,
                monetary_tutar,
                -- Mantıksal segmentasyon kuralları (Transform)
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