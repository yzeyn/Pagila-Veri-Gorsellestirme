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
    dag_id='magaza_gunluk_performans',
    default_args=default_args,
    description='Mağaza bazlı günlük ciro ve işlem sayısı performansını hesaplar',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # 1. Aşama: Hedef Data Mart Tablosunu Oluştur (Eğer yoksa)
    tablo_olustur = SQLExecuteQueryOperator(
        task_id='create_dm_daily_store_performance',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS dm_daily_store_performance (
                islem_tarihi DATE,
                store_id INT,
                majoz_personel VARCHAR(100),
                toplam_ciro NUMERIC(10,2),
                toplam_islem_sayisi INT,
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (islem_tarihi, store_id)
            );
        """,
    )

    # 2. Aşama: E-T-L (Extract - Transform - Load) İşlemi
    # Ödemeleri çekip, mağaza bazında gruplayarak Data Mart tablosuna yazar
    veri_isle_ve_yukle = SQLExecuteQueryOperator(
        task_id='extract_transform_load_store_data',
        conn_id='postgres_pagila',
        sql="""
            -- Tekilleştirme veya mükerrer kaydı önlemek için (Idempotency)
            -- Çalıştırılan güne ait eski veriler varsa önce temizlenir.
            DELETE FROM dm_daily_store_performance 
            WHERE islem_tarihi IN (SELECT DISTINCT DATE(payment_date) FROM payment);
            
            -- EXTRACT & TRANSFORM & LOAD
            INSERT INTO dm_daily_store_performance (
                islem_tarihi, 
                store_id, 
                majoz_personel, 
                toplam_ciro, 
                toplam_islem_sayisi
            )
            SELECT 
                DATE(p.payment_date) AS islem_tarihi,
                s.store_id,
                CONCAT(st.first_name, ' ', st.last_name) AS majoz_personel,
                ROUND(SUM(p.amount), 2) AS toplam_ciro,
                COUNT(p.payment_id) AS toplam_islem_sayisi
            FROM payment p
            JOIN staff st ON p.staff_id = st.staff_id
            JOIN store s ON st.store_id = s.store_id
            GROUP BY DATE(p.payment_date), s.store_id, st.first_name, st.last_name;
        """,
    )

    tablo_olustur >> veri_isle_ve_yukle