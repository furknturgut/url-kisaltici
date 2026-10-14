# URL Kısaltıcı

![CI](https://github.com/furknturgut/url-kisaltici/actions/workflows/ci.yml/badge.svg)

Uzun linkleri kısaltan ve tıklamaları sayan küçük bir servis. FastAPI ile yazdım; cache için Redis, tıklama kaydı için Kafka, izleme için Prometheus ve Grafana kullanıyor.

Amacım cache, mesaj kuyruğu, kimlik doğrulama ve izleme gibi konuları tek bir projede, gerçekten çalışan bir şekilde denemekti.

## Neler yapabiliyor

- Kayıt olup giriş yapılabiliyor, giriş yapınca bir JWT dönüyor
- Giriş yapan kullanıcı link kısaltabiliyor (dakikada en fazla 5)
- Kısa linke tıklayan herkes asıl adrese yönlendiriliyor
- Her tıklama kaydediliyor, link sahibi kendi linklerinin istatistiğini görebiliyor
- İstek sayısı, hata oranı, cache hit oranı ve p99 gecikme Grafana'dan izlenebiliyor

## Nasıl çalışıyor

```
Kullanıcı ──> API (FastAPI)
               ├── Redis        kısa kod → uzun link (cache), rate limit sayaçları
               ├── PostgreSQL   kullanıcılar, linkler, tıklamalar
               └── Kafka        "tıklandı" olayları
                                   └── worker.py ──> PostgreSQL

Prometheus ── API'nin /metrics adresinden ölçümleri toplar
Grafana    ── Prometheus'tan okuyup grafik çizer
```

Bir linke tıklandığında API önce Redis'e bakıyor, orada yoksa PostgreSQL'den bulup Redis'e yazıyor. Tıklama bilgisi Kafka'ya bırakılıyor ve kullanıcı beklemeden yönlendiriliyor. `worker.py` bu olayları okuyup veritabanına yazıyor, bu yüzden istatistikler birkaç saniye geriden gelebilir.

Redis ya da Kafka kapalı olsa da yönlendirme çalışmaya devam ediyor. Redis yoksa doğrudan veritabanına gidiliyor, Kafka yoksa olay kısa bir süre kuyrukta bekliyor.

## Kullanılanlar

- Python 3.12, FastAPI, Pydantic
- PostgreSQL 16 (psycopg)
- Redis 7
- Apache Kafka 3.9 (confluent-kafka)
- Argon2 (şifre hash), PyJWT
- Prometheus, Grafana
- Docker Compose
- pytest, GitHub Actions

## Kurulum

Gerekenler: Python 3.12+ ve Docker.

**1. Repoyu indir**

```bash
git clone https://github.com/furknturgut/url-kisaltici.git
cd url-kisaltici
```

**2. `.env` dosyasını oluştur**

```bash
cp .env.example .env
```

İçindeki alanları doldur. `GIZLI_ANAHTAR` için rastgele bir değer üretebilirsin:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

**3. Servisleri başlat**

```bash
docker compose up -d
```

**4. Tabloları oluştur**

Kullanıcı ve veritabanı adını `.env`'de ne yazdıysan onunla değiştir:

```bash
docker exec -i url-db psql -U admin -d urldb < schema.sql
```

**5. Python ortamını kur**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**6. API ve worker'ı ayrı terminallerde çalıştır**

```bash
uvicorn main:app --reload --host 0.0.0.0
```

```bash
python worker.py
```

`--host 0.0.0.0` sadece Prometheus'un (Docker içinden) API'ye ulaşabilmesi için gerekli. Yalnızca API'yi deneyeceksen yazmana gerek yok.

## Adresler

| Servis | Adres |
|---|---|
| API dokümantasyonu (Swagger) | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

Veritabanı, Redis, Kafka, Prometheus ve Grafana sadece `127.0.0.1` üzerinden açık, aynı ağdaki başka cihazlar bunlara bağlanamıyor.

## API

| Metot | Adres | Giriş gerekli mi | Açıklama |
|---|---|---|---|
| POST | `/api/v1/kayit` | Hayır | Kayıt olur |
| POST | `/api/v1/giris` | Hayır | Giriş yapar, JWT döner |
| POST | `/api/v1/urls` | Evet | Link kısaltır |
| GET | `/api/v1/urls/{kod}/stats` | Evet | Tıklanma sayısı (sadece link sahibi görebilir) |
| GET | `/{kod}` | Hayır | Asıl adrese yönlendirir (302) |
| GET | `/metrics` | Hayır | Prometheus ölçümleri |

Giriş gerektiren adreslerde token header'da gönderiliyor:

```
Authorization: Bearer <token>
```

Örnek akış:

```bash
# kayıt
curl -X POST http://localhost:8000/api/v1/kayit \
  -H "Content-Type: application/json" \
  -d '{"email": "ali@mail.com", "sifre": "gizli1234"}'

# giriş
curl -X POST http://localhost:8000/api/v1/giris \
  -H "Content-Type: application/json" \
  -d '{"email": "ali@mail.com", "sifre": "gizli1234"}'

# link kısaltma (token'ı girişten aldığın değerle değiştir)
curl -X POST http://localhost:8000/api/v1/urls \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"uzun_url": "https://example.com"}'
```

## Testler

Docker servisleri açıkken:

```bash
pytest -v
```

Aynı testler her push'ta GitHub Actions'ta da çalışıyor (`.github/workflows/ci.yml`). Orada PostgreSQL, Redis ve Kafka her seferinde sıfırdan açılıyor ve tablolar `schema.sql` ile oluşturuluyor.

## Grafana

Dashboard repoda yok, elle oluşturdum. Data source olarak Prometheus eklerken adres `http://prometheus:9090` olmalı (Grafana da Docker içinde çalıştığı için `localhost` değil).

Kullandığım sorgular:

```
# saniyedeki istek sayısı
sum by (handler) (rate(http_requests_total[1m]))

# cache hit oranı
sum(rate(url_cache_istekleri_total{sonuc="hit"}[1m])) / sum(rate(url_cache_istekleri_total[1m]))

# p99 gecikme
histogram_quantile(0.99, sum by (le) (rate(http_request_duration_highr_seconds_bucket[1m])))

# hatalı istekler
sum(rate(http_requests_total{status=~"4xx|5xx"}[1m]))
```

## Dosyalar

```
main.py              API
worker.py            Kafka'dan tıklamaları okuyup veritabanına yazar
schema.sql           tablolar
docker-compose.yml   PostgreSQL, Redis, Kafka, Prometheus, Grafana
prometheus.yml       Prometheus'un nereden veri toplayacağı
conftest.py          test hazırlıkları
test_main.py         testler
.env.example         gerekli ayarların listesi
```

## Eksikler

Bilerek basit bıraktığım ya da henüz yapmadığım şeyler:

- Her istekte yeni bir PostgreSQL bağlantısı açılıyor, connection pool eklenmeli
- Kısa link adresi kodda `localhost:8000` olarak yazılı, ayardan gelmeli
- API ve worker henüz Docker'da çalışmıyor, elle başlatılıyor
- Kafka verisi kalıcı değil. Container yeniden başlayınca topic'i worker tekrar oluşturuyor ama işlenmemiş olaylar kaybolabilir
- Loglar `print` ile yazılıyor, yapılandırılmış loglamaya geçilmeli