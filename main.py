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

# 字段 Field ID 映射 (我们正在尝试定位正确的 Code ID)
FIELD_ID_MAP = {
    "Code": "fldaIfMQC8",        # 资产代码 (此ID可能不正确，我们正在诊断)
    "Type": "fldwUSEPXS",        # 资产类型
    "Price": "fldycnGfq3",       # 价格
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

    def _get_table_data(self, table_id):
        """通用方法：从飞书表格中读取所有记录（强制带 view_id 避免筛选）"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{table_id}/records"
        
        # 确保读取所有字段，并指定 view_id 避免视图筛选影响
        params = {
            "page_size": 100, 
            "view_id": "vewiMpomq3" # 使用你之前提供的视图 ID
        }
        
        print(f"正在读取表格数据: {table_id}...")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") == 0:
            # 打印实际读取到的记录数
            items = data["data"]["items"]
            print(f"读取成功，共 {len(items)} 条记录。")
            return items 
        else:
            raise Exception(f"读取表格失败: {data.get('msg')}")

    def _update_records(self, table_id, records_to_update):
        """更新飞书表格中的记录"""
        url = f"{FEISHU_API_BASE}/{self.base_token}/tables/{table_id}/records"
        payload = {"records": records_to_update}

        response = requests.post(url, headers=self.headers, data=json.dumps(payload), params={"value_input_option": "custom"})
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0:
            print("数据成功写入飞书！✅")
        else:
            raise Exception(f"写入失败，飞书返回错误: {data.get('msg')}")

def fetch_yfinance_price(symbols):
    """使用 yfinance 获取股票/ETF/外汇/加密货币的价格"""
    return {}

def get_symbol_string(field_value):
    """从飞书 API 返回的复杂字段值中提取出股票代码字符串"""
    if not field_value:
        return None
    
    # 尝试解析飞书 Primary Field (主字段) 这种 {type: 'text', text: 'AAPL'} 的格式
    if isinstance(field_value, list) and field_value and isinstance(field_value[0], dict) and 'text' in field_value[0]:
        return field_value[0]['text'].strip()
    # 针对其他简单字符串字段
    elif isinstance(field_value, str):
        return field_value.strip()
    return None


def main():
    if not all([APP_ID, APP_SECRET, BASE_TOKEN]):
        print("错误：请确保在 GitHub Secrets 中配置了 FEISHU_APP_ID, FEISHU_APP_SECRET 和 FEISHU_BASE_TOKEN。")
        return

    try:
        feishu_client = FeishuClient(APP_ID, APP_SECRET, BASE_TOKEN)
        assets_records = feishu_client._get_table_data(ASSETS_TABLE_ID)
        
        if not assets_records:
            print("未找到任何记录。请检查表格是否为空。")
            return
            
        print("--- 诊断模式：打印第一个记录的所有字段原始数据 ---")
        first_record = assets_records[0]
        
        print(f"记录 ID: {first_record.get('record_id', 'N/A')}")
        print("所有字段 (Field ID: Value) 原始数据:")
        # Pretty print the fields dictionary
        # 这里打印了整个 fields 字典，以查找正确的 Field ID
        print(json.dumps(first_record.get('fields'), indent=4, ensure_ascii=False))
        
        print("--- 诊断总结：请在上方输出中找到包含股票代码的字段，并提供其对应的 Field ID ---")

        # 诊断完成后，脚本退出
        return 

    except Exception as e:
        print(f"程序运行出错: {e}")

if __name__ == '__main__':
    # 确保 yfinance 缓存路径设置
    yf.set_tz_cache_location(os.path.expanduser('~/.yfinance'))
    main()
