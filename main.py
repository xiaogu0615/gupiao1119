import os
import requests
import json
import pandas as pd
import yfinance as yf

# --- 配置区 (CONF_START) ---
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
BASE_TOKEN = os.getenv("FEISHU_BASE_TOKEN")

# 表格 ID 和字段 ID 映射
ASSETS_TABLE_ID = "tblTFq4Cqsz0SSa1"

FIELD_ID_MAP = {
    "Code": "Code",              # 资产代码 (Primary Field)
    "Type": "fldwUSEPXS",        # 资产类型
    "Price": "fldbbaX8bo",       # 价格字段 ID（必须是数字类型）
}
# --- 配置区 (CONF_END) ---

# 飞书 API 终点
FEISHU_AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
FEISHU_API_BASE = "https://open.feishu.cn/open-apis/bitable/v1/apps"

class FeishuClient:
    """飞书 API 客户端"""

    def __init__(self, app_id, app_secret, base_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.base_token = base_token
        self._access_token = self._get_app_access_token()
        self.headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json"
        }

    def _get_app_access_token(self):
        """获取 App Access Token"""
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        headers = {"Content-Type": "application/json"}
        response = requests.post(FEISHU_AUTH_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0:
            return data["app_access_token"]
        else:
            raise Exception(f"获取 App Token 失败: {data.get('msg')}")

    def get_assets_records(self):
        """读取表格记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{ASSETS_TABLE_ID}/records"
        params = {
            "page_size": 100,
            "view_id": "vewiMpomq3"  # 视图 ID
        }
        print(f"正在读取表格数据: {ASSETS_TABLE_ID}...")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0:
            items = data["data"]["items"]
            print(f"读取成功，共 {len(items)} 条记录。")
            return items
        else:
            raise Exception(f"读取表格失败: {data.get('msg')}")

    def update_price_records(self, records_to_update):
        """批量更新记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{ASSETS_TABLE_ID}/records/batch_update"
        payload = {"records": records_to_update}

        print(f"准备批量更新 {len(records_to_update)} 条记录...")
        print("--- 调试：完整 JSON Payload 结构 ---")
        print(json.dumps(payload, indent=4, ensure_ascii=False))
        print("---------------------------------------")

        response = requests.patch(url, headers=self.headers, data=json.dumps(payload))

        if response.status_code != 200:
            try:
                error_data = response.json()
                if 'field validation failed' in error_data.get('msg', ''):
                    print("注意：字段类型必须与飞书表格一致，例如数字字段必须写入数字。")
                raise Exception(
                    f"写入失败，HTTP 状态码: {response.status_code}. "
                    f"飞书错误码: {error_data.get('code')}. 错误信息: {error_data.get('msg')}. 详细数据: {error_data}"
                )
            except json.JSONDecodeError:
                response.raise_for_status()

        data = response.json()
        if data.get("code") == 0:
            print("批量更新成功！✅")
        else:
            raise Exception(f"批量更新失败: {data.get('msg')}. 详细错误信息: {data}")

def fetch_yfinance_price(symbols):
    """从 yfinance 获取价格"""
    if not symbols:
        return {}
    unique_symbols = list(set(symbols))
    print(f"正在从 yfinance 获取 {len(unique_symbols)} 个资产的价格...")
    data = yf.download(unique_symbols, period="1d", progress=False, auto_adjust=True)
    prices = {}
    for symbol in unique_symbols:
        try:
            if len(unique_symbols) == 1:
                price = data['Close'].iloc[-1]
            else:
                price = data['Close'][symbol].iloc[-1]
            if pd.notna(price):
                prices[symbol] = round(float(price), 5)
                print(f"  ✅ {symbol}: {prices[symbol]}")
            else:
                print(f"  ⚠️ {symbol}: 价格数据缺失或无效。")
                prices[symbol] = None
        except Exception as e:
            print(f"  ❌ {symbol}: 获取价格失败 ({e})")
            prices[symbol] = None
    return prices

def get_symbol_string(field_value):
    """解析飞书字段值为字符串"""
    if not field_value:
        return None
    if isinstance(field_value, str):
        return field_value.strip()
    elif isinstance(field_value, list) and field_value and isinstance(field_value[0], dict) and 'text' in field_value[0]:
        return field_value[0]['text'].strip()
    elif isinstance(field_value, (float, int)):
        return str(field_value)
    return None

def main():
    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("错误：请确保在 GitHub Secrets 中配置了 FEISHU_APP_ID, FEISHU_APP_SECRET 和 FEISHU_BASE_TOKEN。")
        return
    try:
        feishu_client = FeishuClient(APP_ID, APP_SECRET, BASE_TOKEN)
        assets_records = feishu_client.get_assets_records()
        if not assets_records:
            print("未找到任何记录。任务结束。")
            return

        symbols_to_fetch = []
        code_field_id = FIELD_ID_MAP["Code"]
        price_field_id = FIELD_ID_MAP["Price"]

        for record in assets_records:
            symbol = get_symbol_string(record['fields'].get(code_field_id))
            if symbol:
                symbols_to_fetch.append(symbol)

        if not symbols_to_fetch:
            print("未找到有效的资产代码。任务结束。")
            return

        price_data = fetch_yfinance_price(symbols_to_fetch)
        feishu_payload = []
        updated_count = 0

        for record in assets_records:
            record_id = record["record_id"]
            symbol = get_symbol_string(record['fields'].get(code_field_id))
            if symbol and symbol in price_data and price_data[symbol] is not None:
                new_price = price_data[symbol]
                update_record = {
                    "record_id": record_id,
                    "fields": {
                        price_field_id: new_price
                    }
                }
                if updated_count == 0:
                    print(f"--- 调试：示例更新记录结构 (ID: {record_id}) ---")
                    print(json.dumps(update_record, indent=4, ensure_ascii=False))
                    print("-----------------------------------------------------")
                feishu_payload.append(update_record)
                updated_count += 1

        if not feishu_payload:
            print("没有找到任何需要更新的有效价格。任务结束。")
            return

        feishu_client.update_price_records(feishu_payload)
        print(f"总计更新了 {updated_count} 条记录的价格。")

    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == '__main__':
    main()
