from os import access
from anyio.streams import stapled
import json
import secrets
import string
import time
from datetime import datetime, timezone,timedelta
import os
from dotenv import load_dotenv
import jwt                                          # 🆕
from argon2.exceptions import VerifyMismatchError   # 🆕
from argon2 import PasswordHasher
from pydantic import BaseModel, EmailStr, Field, HttpUrl
import psycopg
import redis
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException,Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer    # 🆕
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, HttpUrl
from prometheus_client import Counter
from prometheus_fastapi_instrumentator import Instrumentator
from contextlib import asynccontextmanager

load_dotenv()
VERITABANI_ADRESI = os.environ["VERITABANI_ADRESI"]
GIZLI_ANAHTAR = os.environ["GIZLI_ANAHTAR"]

@asynccontextmanager
async def yasam_dongusu(app):
    yield                      # ← uygulama burada çalışıyor
    print("📮 Kapanıyor, bekleyen Kafka mesajları gönderiliyor...")
    producer.flush(5)          # ← kapanırken: en fazla 5 sn bekle, hepsini gönder


app = FastAPI(title="URL Kısaltıcı", lifespan=yasam_dongusu)

Instrumentator().instrument(app).expose(app)

CACHE_ISTEKLERI = Counter(
    "url_cache_istekleri",
    "Redis cache'e yapılan isteklerin sonucu",
    ["sonuc"],
)

ALFABE = string.ascii_letters + string.digits
KOD_UZUNLUGU = 7


cache = redis.Redis(host="localhost", port=6379, decode_responses=True)
CACHE_SURESI = 3600

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "broker.address.family": "v4",
})
ph = PasswordHasher()
ALGORITMA = "HS256"
TOKEN_SURESI_DK = 30
TIKLAMA_TOPIC = "tiklamalar"


# ---------- Modeller ----------
class UrlIstek(BaseModel):
    uzun_url: HttpUrl


class UrlCevap(BaseModel):
    kisa_kod: str
    kisa_url: str


class Istatistik(BaseModel):
    kisa_kod: str
    uzun_url: str
    toplam_tiklama: int
    son_tiklama: datetime | None

class KayitIstek(BaseModel):
    email: EmailStr
    sifre: str = Field(min_length=8)

class KullaniciCevap(BaseModel):
    id: int
    email: str

class GirisIstek(BaseModel):
    email: EmailStr
    sifre: str

class TokenCevap(BaseModel):
    access_token: str
    token_type: str = "bearer"



# ---------- Yardımcı fonksiyonlar ----------
def baglanti_ac():
    return psycopg.connect(VERITABANI_ADRESI)


def kod_var_mi(kod: str) -> bool:
    with baglanti_ac() as baglanti:
        satir = baglanti.execute(
            "SELECT 1 FROM urls WHERE kisa_kod = %s", (kod,)
        ).fetchone()
    return satir is not None


def kod_uret() -> str:
    while True:
        kod = "".join(secrets.choice(ALFABE) for _ in range(KOD_UZUNLUGU))
        if not kod_var_mi(kod):
            return kod


def kod_gecerli_mi(kod: str) -> bool:
    return len(kod) == KOD_UZUNLUGU and all(harf in ALFABE for harf in kod)


def url_bul(kod: str) -> str | None:
    """Önce Redis'e, yoksa PostgreSQL'e bakar."""
    anahtar = f"url:{kod}"
    baslangic = time.perf_counter()

    try:
        uzun_url = cache.get(anahtar)
    except redis.exceptions.RedisError:
        print("⚠️  Redis'e ulaşılamadı, doğrudan PostgreSQL'e gidiliyor")
        uzun_url = None

    if uzun_url is not None:
        CACHE_ISTEKLERI.labels(sonuc="hit").inc()      # 🆕
        sure = (time.perf_counter() - baslangic) * 1000
        print(f"⚡ CACHE HIT  → {kod}  ({sure:.2f} ms)")
        return uzun_url

    CACHE_ISTEKLERI.labels(sonuc="miss").inc()         # 🆕
    with baglanti_ac() as baglanti:
        ...
        satir = baglanti.execute(
            "SELECT uzun_url FROM urls WHERE kisa_kod = %s", (kod,)
        ).fetchone()
    sure = (time.perf_counter() - baslangic) * 1000
    print(f"🐢 CACHE MISS → {kod}  ({sure:.2f} ms)")

    if satir is None:
        return None
    uzun_url = satir[0]

    try:
        cache.set(anahtar, uzun_url, ex=CACHE_SURESI)
    except redis.exceptions.RedisError:
        pass

    return uzun_url


def tiklama_olayi_gonder(kod: str) -> None:
    """Kafka'ya 'bu linke tıklandı' notu bırakır."""
    olay = {
        "kisa_kod": kod,
        "zaman": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer.produce(TIKLAMA_TOPIC, key=kod, value=json.dumps(olay))
        producer.poll(0)
        print(f"📨 Kafka'ya gönderildi → {olay}")
    except Exception as hata:
        print(f"⚠️  Kafka'ya gönderilemedi: {hata}")

def token_uret(kullanici_id: int) -> str:
    """Kullanıcıya imzalı bir bilet (JWT) üretir."""
    simdi = datetime.now(timezone.utc)
    icerik = {
        "sub": str(kullanici_id),
        "iat": simdi,
        "exp": simdi + timedelta(minutes=TOKEN_SURESI_DK),
    }
    return jwt.encode(icerik, GIZLI_ANAHTAR, algorithm=ALGORITMA)

guvenlik = HTTPBearer()
def aktif_kullanici(
    kimlik: HTTPAuthorizationCredentials = Depends(guvenlik),
) -> int:
    """Bileti kontrol eder, geçerliyse sahibinin id'sini döndürür."""
    try:
        icerik = jwt.decode(kimlik.credentials, GIZLI_ANAHTAR, algorithms=[ALGORITMA])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Oturum süresi doldu, tekrar giriş yap")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Geçersiz token")
    return int(icerik["sub"])

DAKIKADA_LINK_LIMITI = 5


def link_limiti_kontrol(kullanici_id: int = Depends(aktif_kullanici)) -> int:
    """Kullanıcının bu dakikadaki link sayısını kontrol eder."""
    dakika = int(time.time() // 60)
    anahtar = f"limit:{kullanici_id}:{dakika}"

    try:
        sayi = cache.incr(anahtar)
        if sayi == 1:
            cache.expire(anahtar, 60)
    except redis.exceptions.RedisError:
        return kullanici_id   # Redis kapalıysa engelleme

    if sayi > DAKIKADA_LINK_LIMITI:
        kalan_saniye = 60 - int(time.time()) % 60
        raise HTTPException(
            status_code=429,
            detail="Çok fazla istek. Biraz bekleyip tekrar dene.",
            headers={"Retry-After": str(kalan_saniye)},
        )

    return kullanici_id

# ---------- Endpoint'ler ----------
@app.post("/api/v1/urls", response_model=UrlCevap, status_code=201)
def url_kisalt(istek: UrlIstek, kullanici_id: int = Depends(link_limiti_kontrol)):   # 🆕 # 🆕
    kod = kod_uret()
    with baglanti_ac() as baglanti:
        baglanti.execute(
            "INSERT INTO urls (kisa_kod, uzun_url, kullanici_id) VALUES (%s, %s, %s)",  # 🆕
            (kod, str(istek.uzun_url), kullanici_id),                                    # 🆕
        )
    return UrlCevap(kisa_kod=kod, kisa_url=f"http://localhost:8000/{kod}")


@app.get("/api/v1/urls/{kod}/stats", response_model=Istatistik)
def istatistik(kod: str, kullanici_id: int = Depends(aktif_kullanici)):   # 🆕
    with baglanti_ac() as baglanti:
        satir = baglanti.execute(
            "SELECT uzun_url, kullanici_id FROM urls WHERE kisa_kod = %s", (kod,)   # 🆕
        ).fetchone()
        if satir is None or satir[1] != kullanici_id:                               # 🆕
            raise HTTPException(status_code=404, detail="Link bulunamadı")

        toplam, son = baglanti.execute(
            "SELECT COUNT(*), MAX(zaman) FROM tiklamalar WHERE kisa_kod = %s",
            (kod,),
        ).fetchone()

    return Istatistik(
        kisa_kod=kod,
        uzun_url=satir[0],
        toplam_tiklama=toplam,
        son_tiklama=son,
    )


@app.get("/{kod}")
def yonlendir(kod: str):
    if not kod_gecerli_mi(kod):
        raise HTTPException(status_code=404, detail="Link bulunamadı")

    uzun_url = url_bul(kod)
    if uzun_url is None:
        raise HTTPException(status_code=404, detail="Link bulunamadı")

    tiklama_olayi_gonder(kod)
    return RedirectResponse(url=uzun_url, status_code=302)
    

@app.post("/api/v1/kayit", response_model=KullaniciCevap, status_code=201)
def kayit_ol(istek: KayitIstek):
    email = istek.email.lower()
    sifre_hash = ph.hash(istek.sifre)

    try:
        with baglanti_ac() as baglanti:
            satir = baglanti.execute(
                "INSERT INTO kullanicilar (email, sifre_hash) VALUES (%s, %s) RETURNING id",
                (email, sifre_hash),
            ).fetchone()
    except psycopg.errors.UniqueViolation:
        raise HTTPException(status_code=409, detail="bu e-posta zaten kayıtlı")

    return KullaniciCevap(id=satir[0], email=email)
    
@app.post("/api/v1/giris", response_model=TokenCevap)
def giris_yap(istek: GirisIstek):
    email = istek.email.lower()

    with baglanti_ac() as baglanti:
        satir = baglanti.execute(
            "SELECT id, sifre_hash FROM kullanicilar WHERE email = %s", (email,)
        ).fetchone()

    if satir is None:
        raise HTTPException(status_code=401, detail="E-posta veya şifre hatalı")

    kullanici_id, sifre_hash = satir

    try:
        ph.verify(sifre_hash, istek.sifre)
    except VerifyMismatchError:
        raise HTTPException(status_code=401, detail="E-posta veya şifre hatalı")

    return TokenCevap(access_token=token_uret(kullanici_id))