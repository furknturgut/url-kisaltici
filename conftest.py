import uuid

import pytest
from fastapi.testclient import TestClient

from main import app


@pytest.fixture
def client():
    """API'ye istek atan sahte bir tarayıcı."""
    with TestClient(app) as c:
        yield c


@pytest.fixture
def kullanici_olustur(client):
    """Yeni bir kullanıcı oluşturup giriş yapar, bilet header'ını döndürür."""
    def _olustur():
        email = f"test-{uuid.uuid4().hex[:8]}@test.com"
        sifre = "test12345"

        cevap = client.post("/api/v1/kayit", json={"email": email, "sifre": sifre})
        assert cevap.status_code == 201

        cevap = client.post("/api/v1/giris", json={"email": email, "sifre": sifre})
        assert cevap.status_code == 200

        token = cevap.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    return _olustur