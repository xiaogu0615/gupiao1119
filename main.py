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
    "Code": "Code",              # 资产代码 (Primary Field 键名)
    "Type": "fldwUSEPXS",        # 资产类型 (字段 ID)
    # >>>>>> 新创建的、可写入的“单行文本”字段 ID: fldbbaX8bo <<<<<<
    "Price": "fldbbaX8bo",       # 价格 (使用新的文本字段 ID)
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
        
        # *** 调试：打印完整 JSON Payload 结构 ***
        print("--- 调试：完整 JSON Payload 结构 ---")
        print(json.dumps(payload, indent=4, ensure_ascii=False))
        print("---------------------------------------")
        
        # 使用默认写入模式
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        
        # 检查 HTTP 状态码
        if response.status_code != 200:
            try:
                error_data = response.json()
                # 检查是否是富文本结构写入错误
                if 'field validation failed' in error_data.get('msg', ''):
                     print("注意：如果价格字段是单行文本，请确保写入的是纯字符串，而不是富文本数组格式。")
                
                raise Exception(f"写入失败，HTTP 状态码: {response.status_code}. 飞书错误码: {error_data.get('code')}. 错误信息: {error_data.get('msg')}. 详细数据: {error_data}")
            except json.JSONDecodeError:
                response.raise_for_status()
        
        data = response.json()
        
        if data.get("code") == 0:
            print("数据成功写入飞书！✅")
        else:
            raise Exception(f"写入失败，飞书返回业务错误: {data.get('msg')}. 详细错误信息: {data}")

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
    
    # 移除不受支持的 cache=False，使用 auto_adjust=True
    data = yf.download(unique_symbols, period="1d", progress=False, auto_adjust=True)

    prices = {}
    
    for symbol in unique_symbols:
        try:
            # yfinance 在 auto_adjust=True 时，只会返回 'Close' 列
            if len(unique_symbols) == 1:
                price = data['Close'].iloc[-1] 
            else:
                price = data['Close'][symbol].iloc[-1]

            if pd.notna(price):
                # 价格保留 5 位小数的浮点数，以匹配飞书字段设置
                prices[symbol] = round(float(price), 5)
                print(f"  ✅ {symbol}: {prices[symbol]}")
            else:
                print(f"  ⚠️ {symbol}: 价格数据缺失或无效。")
                prices[symbol] = None
                
        except Exception as e:
            # 捕获并打印更清晰的错误信息
            print(f"  ❌ {symbol}: 获取价格失败 ({e})")
            prices[symbol] = None
            
    return prices

def get_symbol_string(field_value):
    """
    从飞书 API 返回的复杂字段值中提取出股票代码字符串 (读取 Primary Field 时可能需要).
    """
    if not field_value:
        return None
    
    # 检查是否是纯字符串 (Primary Field 的默认读取格式)
    if isinstance(field_value, str):
        return field_value.strip()
    
    # 检查是否是富文本数组 (读取其他字段时可能返回的格式)
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
        code_field_id = FIELD_ID_MAP["Code"]
        price_field_id = FIELD_ID_MAP["Price"]
        
        for record in assets_records:
            symbol = get_symbol_string(record['fields'].get(code_field_id))
            if symbol:
                symbols_to_fetch.append(symbol)

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
                
                # *** 关键修复：强制使用纯字符串格式，对应 "单行文本" 字段类型 (Type 1) ***
                price_string = str(new_price) 
                
                # 飞书 API 期望的更新结构，确保字段值是纯字符串
                update_record = {
                    "record_id": record_id,
                    "fields": {
                        price_field_id: price_string # 写入纯字符串
                    }
                }
                
                # *** 调试输出：打印一个示例 payload 元素 ***
                if updated_count == 0:
                    print(f"--- 调试：示例更新记录结构 (ID: {record_id}) ---")
                    # 此时 fldbbaX8bo 的值将是 Simple String 结构
                    print(json.dumps(update_record, indent=4, ensure_ascii=False))
                    print("-----------------------------------------------------")

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
    main()
