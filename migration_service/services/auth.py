import httpx
import jwt

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers

from migration_service.settings import settings


__PUBLIC_KEYS = {}


async def load_jwks():
    __PUBLIC_KEYS.clear()

    async with httpx.AsyncClient() as requests:
        response = await requests.get(f'{settings.api_iam}/auth/.well-known/jwks.json')
        data = response.json()

        for jwk in data:
            numbers = RSAPublicNumbers(e=jwk['e'], n=jwk['n'])
            __PUBLIC_KEYS[jwk['kid']] = numbers.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )


async def decode_jwt(token: str):
    headers = jwt.get_unverified_header(token)
    key = __PUBLIC_KEYS[headers['kid']]
    return jwt.decode(token, key, algorithms=['RS256'])
