from hashlib import blake2b
from hmac import compare_digest
import os


class CAuth:
    AUTH_SIZE = os.environ.get("CAUTH-AUTH_SIZE", 16)
    SECRET_KEY = os.environ.get("CAUTH-SECRET_KEY", "your-secret-key")

    def __init__(self, message):
        self.message = message

    @staticmethod
    def to_bytes(text):
        if not isinstance(text, bytes):
            return str(text).encode('ascii')
        return text

    @staticmethod
    def _verify(message, hash):
        good_sig = CAuth.to_bytes(CAuth.get_hash(message))
        hash = CAuth.to_bytes(hash)
        return compare_digest(good_sig, hash)

    def verify(self, hash):
        return CAuth._verify(self.message, hash)

    @staticmethod
    def get_hash(message):
        message = CAuth.to_bytes(message)
        h = blake2b(digest_size=CAuth.AUTH_SIZE,
                    key=str(CAuth.SECRET_KEY).encode('ascii'))
        h.update(message)
        return h.hexdigest().encode('utf-8').decode("utf-8")


if __name__ == '__main__':

    hash = CAuth.get_hash("bienvenu")
    print(hash)
    print(CAuth("bienvenu").verify(hash))
