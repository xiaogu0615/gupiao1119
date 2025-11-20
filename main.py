import os
import json
import time
import requests
import pandas as pd
import yfinance as yf

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
BASE_TOKEN = os.getenv("FEISHU_BASE_TOKEN")

ASSETS_TABLE_ID = "tblTFq4Cqsz0SSa1"

FIELD_CODE = "Code"
FIELD_PRICE = "fldbbaX8bo"

FEISHU_API_BASE = "https://open.feishu.cn/open-apis/bitable/v1/apps"
AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"


# ============================
# 飞书 API 客户端
# ============================
class FeishuClient:

    def __init__(self):
        self.token = self._get_token()
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _get_token(self):
        payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
        r = requests.post(AUTH_URL, json=payload)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise Exception(f"获取 token 失败: {d}")
        return d["app_access_token"]

    def get_records(self):
        url = f"{FEISHU_API_BASE}/{BASE_TOKEN}/tables/{ASSETS_TABLE_ID}/records"
        params = {"page_size": 100}
        r = requests.get(url, headers=self.headers, params=params)
        r.raise_for_status()
        d = r.json()
        if d.get("code") != 0:
            raise Exception(f"读取记录失败: {d}")
        return d["data"]["items"]

    def update_one_record(self, record_id, fields):
        """正确的飞书更新方式：逐条 PATCH"""
        url = f"{FEISHU_API_BASE}/{BASE_TOKEN}/tables/{ASSETS_TABLE_ID}/records/{record_id}"
        payload = {"fields": fields}

        r = requests.patch(url, headers=self.headers, json=payload)

        if r.status_code != 200:
            raise Exception(f"PATCH HTTP 错误 {r.status_code}: {r.text}")

        d = r.json()
        if d.get("code") != 0:
            raise Exception(f"飞书业务错误: {d}")

        return True


# ============================
# yfinance 获取价格
# ============================
def fetch_prices(symbols):
    symbols = list(set(symbols))
    print(f"正在获取 {len(symbols)} 个资产价格...")

    for retry in range(3):
        try:
            df = yf.download(symbols, period="1d", auto_adjust=True, progress=False)
            break
        except Exception as e:
            print(f"⚠ yfinance 第 {retry+1} 次失败: {e}")
            time.sleep(2)
    else:
        print("❌ yfinance 获取失败")
        return {}

    prices = {}
    for s in symbols:
        try:
            if len(symbols) == 1:
                price = df["Close"].iloc[-1]
            else:
                price = df["Close"][s].iloc[-1]

            prices[s] = round(float(price), 5)
            print(f"  ✔ {s}: {prices[s]}")

        except:
            print(f"  ✖ {s}: 无价格数据")
            prices[s] = None

    return prices


# ============================
# 工具
# ============================
def get_symbol(v):
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, list) and v and "text" in v[0]:
        return v[0]["text"].strip()
    if isinstance(v, (float, int)):
        return str(v)
    return None


# ============================
# 主流程
# ============================
def main():

    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("❌ GitHub Secrets 未配置完整")
        return

    client = FeishuClient()

    rows = client.get_records()
    print(f"读取到 {len(rows)} 条记录")

    symbols = []
    for r in rows:
        s = get_symbol(r["fields"].get(FIELD_CODE))
        if s:
            symbols.append(s)

    prices = fetch_prices(symbols)

    print("\n开始更新飞书记录...\n")

    updated = 0
    for r in rows:
        rid = r["record_id"]
        s = get_symbol(r["fields"].get(FIELD_CODE))

        if s and prices.get(s) is not None:
            fields = {FIELD_PRICE: prices[s]}
            client.update_one_record(rid, fields)
            print(f"  ✔ 已更新 {s} → {prices[s]}")
            updated += 1

    print(f"\n🎉 完成：共更新 {updated} 条记录。")


if __name__ == "__main__":
    main()


