import json
import os

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic   # 🆕
from dotenv import load_dotenv

load_dotenv()

VERITABANI_ADRESI = os.environ["VERITABANI_ADRESI"]
KAFKA_ADRESI = "localhost:9092"                             # 🆕
TIKLAMA_TOPIC = "tiklamalar"                                # 🆕


def topic_hazirla() -> None:                                # 🆕
    """Pano yoksa oluşturur."""
    admin = AdminClient({
    "bootstrap.servers": KAFKA_ADRESI,
    "broker.address.family": "v4",
    })
    mevcut_panolar = admin.list_topics(timeout=10).topics

    if TIKLAMA_TOPIC in mevcut_panolar:
        print(f"📋 '{TIKLAMA_TOPIC}' panosu zaten var")
        return

    print(f"🛠️  '{TIKLAMA_TOPIC}' panosu yok, oluşturuluyor...")
    sonuclar = admin.create_topics(
        [NewTopic(TIKLAMA_TOPIC, num_partitions=1, replication_factor=1)]
    )
    for ad, sonuc in sonuclar.items():
        try:
            sonuc.result()
            print(f"✅ '{ad}' panosu oluşturuldu")
        except Exception as hata:
            print(f"⚠️  '{ad}' oluşturulamadı: {hata}")


topic_hazirla()                                             # 🆕 işe başlamadan önce

consumer = Consumer({
    "bootstrap.servers": KAFKA_ADRESI,
    "broker.address.family": "v4",
    "group.id": "tiklama-iscisi",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TIKLAMA_TOPIC])                         # 🆕 değişkenden

print("👷 İşçi başladı, tıklamaları bekliyorum...")

try:
    with psycopg.connect(VERITABANI_ADRESI, autocommit=True) as baglanti:
        while True:
            mesaj = consumer.poll(1.0)

            if mesaj is None:
                continue
            if mesaj.error():
                print(f"⚠️  Kafka hatası: {mesaj.error()}")
                continue

            try:
                olay = json.loads(mesaj.value())
                kisa_kod = olay["kisa_kod"]
                zaman = olay["zaman"]
            except (json.JSONDecodeError, TypeError, KeyError):
                print(f"🗑️  Anlaşılmayan mesaj atlandı: {mesaj.value()}")
                continue

            baglanti.execute(
                "INSERT INTO tiklamalar (kisa_kod, zaman) VALUES (%s, %s)",
                (kisa_kod, zaman),
            )
            print(f"✅ Kaydedildi → {kisa_kod} @ {zaman}")

except KeyboardInterrupt:
    print("\n👋 İşçi durduruluyor...")
finally:
    consumer.close()