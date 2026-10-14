-- URL Kısaltıcı veritabanı şeması

CREATE TABLE IF NOT EXISTS kullanicilar (
    id          SERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    sifre_hash  TEXT NOT NULL,
    olusturma   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS urls (
    kisa_kod      VARCHAR(7) PRIMARY KEY,
    uzun_url      TEXT NOT NULL,
    olusturma     TIMESTAMP DEFAULT NOW(),
    kullanici_id  INTEGER REFERENCES kullanicilar(id)
);

CREATE TABLE IF NOT EXISTS tiklamalar (
    id        SERIAL PRIMARY KEY,
    kisa_kod  VARCHAR(7) NOT NULL,
    zaman     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS tiklamalar_kisa_kod_idx ON tiklamalar (kisa_kod);