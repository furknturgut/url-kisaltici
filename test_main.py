UZUN_URL = "https://example.com/"


def test_link_olusturma_ve_yonlendirme(client, kullanici_olustur):
    bilet = kullanici_olustur()

    # Link oluştur
    cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL}, headers=bilet)
    assert cevap.status_code == 201
    kod = cevap.json()["kisa_kod"]
    assert len(kod) == 7

    # Kısa linke git, yönlendirmeyi kontrol et
    cevap = client.get(f"/{kod}", follow_redirects=False)
    assert cevap.status_code == 302
    assert cevap.headers["location"] == UZUN_URL


def test_biletsiz_link_olusturulamaz(client):
    cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL})
    assert cevap.status_code in (401, 403)


def test_sahte_bilet_reddedilir(client):
    sahte = {"Authorization": "Bearer bu.sahte.bilet"}
    cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL}, headers=sahte)
    assert cevap.status_code == 401


def test_yanlis_sifre_reddedilir(client, kullanici_olustur):
    kullanici_olustur()
    cevap = client.post(
        "/api/v1/giris", json={"email": "yok@test.com", "sifre": "yanlis123"}
    )
    assert cevap.status_code == 401


def test_olmayan_link_404(client):
    assert client.get("/zzzzzzz", follow_redirects=False).status_code == 404
    assert client.get("/favicon.ico", follow_redirects=False).status_code == 404


def test_baskasinin_istatistigi_gorulemez(client, kullanici_olustur):
    ahmet = kullanici_olustur()
    mehmet = kullanici_olustur()

    cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL}, headers=ahmet)
    kod = cevap.json()["kisa_kod"]

    # Ahmet kendi istatistiğini görebilir
    assert client.get(f"/api/v1/urls/{kod}/stats", headers=ahmet).status_code == 200
    # Mehmet göremez
    assert client.get(f"/api/v1/urls/{kod}/stats", headers=mehmet).status_code == 404


def test_rate_limit(client, kullanici_olustur):
    bilet = kullanici_olustur()

    for _ in range(5):
        cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL}, headers=bilet)
        assert cevap.status_code == 201

    cevap = client.post("/api/v1/urls", json={"uzun_url": UZUN_URL}, headers=bilet)
    assert cevap.status_code == 429
    assert "retry-after" in cevap.headers