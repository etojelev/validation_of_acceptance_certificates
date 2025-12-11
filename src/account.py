from src.response import AsyncHttpClient


class Account:
    def __init__(self, account: str, token: str):
        self.account = account
        self.token = token
        self.async_client = AsyncHttpClient()
        self.headers = {"Authorization": token, "Content-Type": "application/json"}
