import os
import requests
import json
import pandas as pd
import yfinance as yf

# --- 配置区 (CONF_START) ---
# 从 GitHub Secrets 中读取
APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
BASE_TOKEN = os.getenv("FEISHU_BASE_TOKEN")

# 表格 ID 和字段 ID 映射
ASSETS_TABLE_ID = "tblTFq4Cqsz0SSa1"

# IMPORTANT: 通过诊断确认，资产代码的字段 ID 实际就是字符串 "Code"
FIELD_ID_MAP = {
    "Code": "Code",              # 资产代码 (已修正为 API 返回的键名 "Code")
    "Type": "fldwUSEPXS",        # 资产类型 (无需修改)
    "Price": "fldycnGfq3",       # 价格 (无需修改)
}
# --- 配置区 (CONF_END) ---

# 飞书 API 终点
FEISHU_AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
FEISHU_API_BASE = "https://open.feishu.cn/open-apis/bitable/v1/apps"

class FeishuClient:
    """处理飞书API认证和数据交互的客户端"""

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
        """发送请求获取 App Access Token"""
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
        """从飞书表格中读取所有记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{ASSETS_TABLE_ID}/records"
        
        # 确保读取所有字段，并指定 view_id 避免视图筛选影响
        params = {
            "page_size": 100, 
            "view_id": "vewiMpomq3" # 使用你之前提供的视图 ID
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
        """更新飞书表格中的记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{ASSETS_TABLE_ID}/records"
        payload = {"records": records_to_update}

        print(f"准备更新 {len(records_to_update)} 条记录...")
        response = requests.post(url, headers=self.headers, data=json.dumps(payload), params={"value_input_option": "custom"})
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0:
            print("数据成功写入飞书！✅")
        else:
            raise Exception(f"写入失败，飞书返回错误: {data.get('msg')}. 详细错误信息: {data}")

def fetch_yfinance_price(symbols):
    """
    使用 yfinance 批量获取股票/ETF/外汇/加密货币的价格。
    返回一个字典，键为代码，值为价格。
    """
    if not symbols:
        return {}
    
    # 移除重复代码并转换为列表
    unique_symbols = list(set(symbols))
    
    print(f"正在从 yfinance 获取 {len(unique_symbols)} 个资产的价格...")
    
    # 获取数据
    data = yf.download(unique_symbols, period="1d", progress=False)

    prices = {}
    
    # yfinance 返回的数据结构
    # 如果只有一个代码，data 是一个 pandas Series/DataFrame
    # 如果有多个代码，data 是一个多级索引的 DataFrame
    
    for symbol in unique_symbols:
        try:
            # 尝试获取 'Adj Close' (调整后收盘价) 或 'Close' (收盘价)
            if len(unique_symbols) == 1:
                # 单个代码返回的是 Series 或只有一列的 DataFrame
                price = data['Adj Close'].iloc[-1] if 'Adj Close' in data.columns else data['Close'].iloc[-1]
            else:
                # 多个代码返回的是多级索引 DataFrame
                price = data['Adj Close'][symbol].iloc[-1] if 'Adj Close' in data.columns else data['Close'][symbol].iloc[-1]

            # 确保价格是有效的数字
            if pd.notna(price):
                prices[symbol] = round(float(price), 2)
                print(f"  ✅ {symbol}: {prices[symbol]}")
            else:
                print(f"  ⚠️ {symbol}: 价格数据缺失或无效。")
                prices[symbol] = None
                
        except Exception as e:
            print(f"  ❌ {symbol}: 获取价格失败 ({e})")
            prices[symbol] = None
            
    return prices

def get_symbol_string(field_value):
    """
    从飞书 API 返回的复杂字段值中提取出股票代码字符串。
    根据诊断，飞书 Primary Field 字段 "Code" 直接返回字符串。
    """
    if not field_value:
        return None
    
    # 针对简单字符串字段 (Primary Field)
    if isinstance(field_value, str):
        return field_value.strip()
    
    # 保留对其他可能格式的兼容性，尽管目前看来不需要
    elif isinstance(field_value, list) and field_value and isinstance(field_value[0], dict) and 'text' in field_value[0]:
        return field_value[0]['text'].strip()
    
    return None


def main():
    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("错误：请确保在 GitHub Secrets 中配置了 FEISHU_APP_ID, FEISHU_APP_SECRET 和 FEISHU_BASE_TOKEN。")
        return

    try:
        feishu_client = FeishuClient(APP_ID, APP_SECRET, BASE_TOKEN)
        # 1. 获取所有记录
        assets_records = feishu_client.get_assets_records()
        
        if not assets_records:
            print("未找到任何记录。任务结束。")
            return
            
        # 2. 提取需要更新的股票代码和记录 ID
        symbols_to_fetch = []
        records_to_update_map = {} # {symbol: [record_id, ...]}
        
        code_field_id = FIELD_ID_MAP["Code"]
        price_field_id = FIELD_ID_MAP["Price"]
        
        for record in assets_records:
            record_id = record["record_id"]
            
            # 使用修正后的解析函数
            symbol = get_symbol_string(record['fields'].get(code_field_id))
            
            if symbol:
                symbols_to_fetch.append(symbol)
                
                # 建立代码到记录 ID 的映射，方便后续更新
                if symbol not in records_to_update_map:
                    records_to_update_map[symbol] = []
                records_to_update_map[symbol].append(record_id)

        if not symbols_to_fetch:
            print("未找到有效的资产代码。任务结束。")
            return

        # 3. 批量获取价格
        price_data = fetch_yfinance_price(symbols_to_fetch)
        
        # 4. 准备飞书更新的 payload
        feishu_payload = []
        updated_count = 0

        # 遍历所有记录，准备更新数据
        for record in assets_records:
            record_id = record["record_id"]
            symbol = get_symbol_string(record['fields'].get(code_field_id))
            
            # 只有当代码有效且成功获取到价格时才进行更新
            if symbol and symbol in price_data and price_data[symbol] is not None:
                new_price = price_data[symbol]
                
                # 飞书 API 期望的更新结构
                update_record = {
                    "record_id": record_id,
                    "fields": {
                        # 注意：飞书 API 在更新时，如果字段 ID 是自定义的，需要用 Field ID；
                        # 如果是 Primary Field (如这里的 "Code" 字段，但我们更新的是 "Price")，
                        # 只需要确保使用正确的 Price 字段 ID
                        price_field_id: new_price 
                    }
                }
                feishu_payload.append(update_record)
                updated_count += 1
                
        if not feishu_payload:
            print("没有找到任何需要更新的有效价格。任务结束。")
            return

        # 5. 批量更新飞书
        feishu_client.update_price_records(feishu_payload)
        print(f"总计更新了 {updated_count} 条记录的价格。")

    except Exception as e:
        print(f"程序运行出错: {e}")
        # 在 GitHub Actions 中，如果出现异常，脚本会以非零退出码退出，导致任务失败

if __name__ == '__main__':
    # 确保 yfinance 缓存路径设置，以防万一
    yf.set_tz_cache_location(os.path.expanduser('~/.yfinance'))
    main()
