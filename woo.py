"""
WooCommerce REST API client com paginação automática.
"""

import requests
import time
from datetime import datetime


class WooClient:
    def __init__(self, url: str, key: str, secret: str):
        self.base = url.rstrip("/") + "/wp-json/wc/v3"
        self.auth = (key, secret)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"User-Agent": "CrushCRM/1.0"})

    def _get(self, endpoint: str, params: dict = None) -> list:
        url = f"{self.base}/{endpoint}"
        params = params or {}
        params.setdefault("per_page", 100)
        results = []
        page = 1

        while True:
            params["page"] = page
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            results.extend(data)
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            print(f"  [{endpoint}] página {page}/{total_pages} — {len(results)} registros", end="\r")
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.1)  # respeita rate limit

        print()
        return results

    def get_customers(self, modified_after: str = None) -> list:
        params = {}
        if modified_after:
            params["modified_after"] = modified_after
        return self._get("customers", params)

    def get_products(self) -> list:
        return self._get("products", {
            "_fields": "id,name,categories",
            "status": "publish",
        })

    def get_orders(self, modified_after: str = None) -> list:
        params = {
            "status": "completed,processing",
            "_fields": "id,customer_id,customer_email,date_created,total,status,line_items",
        }
        if modified_after:
            params["modified_after"] = modified_after
        return self._get("orders", params)
