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
    dag_id='personel_verimlilik_akis',
    default_args=varsayilan_ayarlar,
    description='Personel bazlı haftalık kiralama, ciro ve sepet tutarı verimliliğini analiz eder',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    tablo_olusturma_gorevi = SQLExecuteQueryOperator(
        task_id='personel_verimlilik_tablosu_olustur',
        conn_id='postgres_pagila',
        sql="""
            CREATE TABLE IF NOT EXISTS personel_verimlilik (
                personel_id INT,
                personel_adi VARCHAR(150),
                yil_hafta VARCHAR(10),
                toplam_islem_sayisi INT,
                toplam_tahsilat NUMERIC(10,2),
                ortalama_sepet_tutari NUMERIC(10,2),
                guncellenme_tarihi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (personel_id, yil_hafta)
            );
        """,
    )
    
    veri_isle_ve_yukle_gorevi = SQLExecuteQueryOperator(
        task_id='personel_verilerini_hesapla_ve_yukle',
        conn_id='postgres_pagila',
        sql="""
            TRUNCATE TABLE personel_verimlilik;
            
            INSERT INTO personel_verimlilik (
                personel_id, 
                personel_adi, 
                yil_hafta, 
                toplam_islem_sayisi, 
                toplam_tahsilat, 
                ortalama_sepet_tutari
            )
            SELECT 
                s.staff_id AS personel_id,
                CONCAT(s.first_name, ' ', s.last_name) AS personel_adi,
                -- Ödeme tarihini Yıl-Hafta formatına çeviriyoruz (Örn: 2005-22) (Transform)
                TO_CHAR(p.payment_date, 'YYYY-WW') AS yil_hafta,
                -- Personelin gerçekleştirdiği kiralama/işlem sayısı
                COUNT(p.payment_id) AS toplam_islem_sayisi,
                -- Personelin kasaya soktuğu toplam ciro
                ROUND(SUM(p.amount), 2) AS toplam_tahsilat,
                -- İşlem başına düşen ortalama sepet/tahsilat tutarı (Transform)
                ROUND(AVG(p.amount), 2) AS ortalama_sepet_tutari
            FROM staff s
            JOIN payment p ON s.staff_id = p.staff_id
            GROUP BY s.staff_id, s.first_name, s.last_name, TO_CHAR(p.payment_date, 'YYYY-WW');
        """,
    )

    tablo_olusturma_gorevi >> veri_isle_ve_yukle_gorevi