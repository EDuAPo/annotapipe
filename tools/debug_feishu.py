#!/usr/bin/env python3
"""调试飞书表格连接"""
import os
import sys
import yaml
import requests

def load_env():
    """加载环境变量"""
    env_file = "configs/.env"
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    os.environ[key.strip()] = value.strip()

def main():
    print("=" * 60)
    print("飞书表格调试")
    print("=" * 60)
    
    # 加载配置
    load_env()
    
    with open("configs/feishu.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    app_id = os.environ.get('FEISHU_APP_ID', '')
    app_secret = os.environ.get('FEISHU_APP_SECRET', '')
    app_token = config.get('app_token', '')
    table_id = config.get('table_id', '')
    
    print(f"\n配置信息:")
    print(f"  app_id: {app_id[:10]}..." if app_id else "  app_id: 未设置")
    print(f"  app_secret: {app_secret[:10]}..." if app_secret else "  app_secret: 未设置")
    print(f"  app_token: {app_token}")
    print(f"  table_id: {table_id}")
    
    # 获取 token
    print(f"\n获取 tenant_access_token...")
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    r = requests.post(url, json={"app_id": app_id, "app_secret": app_secret})
    data = r.json()
    
    if data.get('code') != 0:
        print(f"  ❌ 获取 token 失败: {data}")
        return 1
    
    token = data.get('tenant_access_token')
    print(f"  ✅ token: {token[:20]}...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # 获取表格信息
    print(f"\n获取多维表格信息...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}"
    r = requests.get(url, headers=headers)
    data = r.json()
    
    if data.get('code') != 0:
        print(f"  ❌ 获取表格信息失败: code={data.get('code')}, msg={data.get('msg')}")
    else:
        app_info = data.get('data', {}).get('app', {})
        print(f"  ✅ 表格名称: {app_info.get('name')}")
        print(f"  ✅ 表格 URL: {app_info.get('url')}")
    
    # 获取数据表列表
    print(f"\n获取数据表列表...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    r = requests.get(url, headers=headers)
    data = r.json()
    
    if data.get('code') != 0:
        print(f"  ❌ 获取数据表列表失败: code={data.get('code')}, msg={data.get('msg')}")
    else:
        tables = data.get('data', {}).get('items', [])
        print(f"  ✅ 共 {len(tables)} 个数据表:")
        for t in tables:
            marker = "👉" if t.get('table_id') == table_id else "  "
            print(f"    {marker} {t.get('name')} (table_id={t.get('table_id')})")
    
    # 获取当前表格的记录数
    print(f"\n获取表格 {table_id} 的记录...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    r = requests.get(url, params={"page_size": 5}, headers=headers)
    data = r.json()
    
    if data.get('code') != 0:
        print(f"  ❌ 获取记录失败: code={data.get('code')}, msg={data.get('msg')}")
    else:
        total = data.get('data', {}).get('total', 0)
        items = data.get('data', {}).get('items', [])
        print(f"  ✅ 共 {total} 条记录")
        print(f"  前5条记录:")
        for i, item in enumerate(items[:5]):
            fields = item.get('fields', {})
            name = fields.get('数据包名称', 'N/A')
            # 处理复杂格式
            if isinstance(name, list) and name:
                name = name[0].get('text', 'N/A') if isinstance(name[0], dict) else name[0]
            print(f"    {i+1}. {name}")
    
    # 测试创建一条记录（包含关键帧数、标注情况、更新时间）
    # 先获取字段配置，找到标注情况的选项ID
    print(f"\n获取字段配置...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    r = requests.get(url, headers=headers)
    field_data = r.json()
    status_option_id = None
    if field_data.get('code') == 0:
        for f in field_data.get('data', {}).get('items', []):
            if f.get('field_name') == '标注情况':
                options = f.get('property', {}).get('options', [])
                for opt in options:
                    if opt.get('name') == '已完成':
                        status_option_id = opt.get('id')
                        print(f"  找到 '已完成' 选项 ID: {status_option_id}")
                        break
    
    print(f"\n测试创建记录（含关键帧数、标注情况、更新时间）...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    test_name = "DEBUG_TEST_20260111_191300"
    import time as time_module
    # 更新时间是 type=5 (日期时间)，需要毫秒时间戳
    # 关键帧数是 type=1 (文本)，需要字符串
    # 标注情况是多选类型，需要传数组格式
    payload = {
        "fields": {
            "数据包名称": test_name,
            "关键帧数": "123",  # 文本类型
            "标注情况": ["已完成"],  # 多选类型，传数组
            "更新时间": int(time_module.time() * 1000),  # 毫秒时间戳
            "拉框属性": True
        }
    }
    print(f"  请求 payload: {payload}")
    r = requests.post(url, json=payload, headers=headers)
    data = r.json()
    
    if data.get('code') != 0:
        print(f"  ❌ 创建失败: code={data.get('code')}, msg={data.get('msg')}")
    else:
        record = data.get('data', {}).get('record', {})
        print(f"  ✅ 创建成功!")
        print(f"     record_id: {record.get('record_id')}")
        print(f"     fields: {record.get('fields')}")
    
    # 测试更新刚创建的记录
    if data.get('code') == 0:
        record_id = record.get('record_id')
        print(f"\n测试更新记录 {record_id}...")
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        update_payload = {
            "records": [{
                "record_id": record_id,
                "fields": {
                    "关键帧数": "456",
                    "更新时间": int(time_module.time() * 1000),
                    "线段属性": True
                }
            }]
        }
        print(f"  更新 payload: {update_payload}")
        r = requests.post(url, json=update_payload, headers=headers)
        data = r.json()
        if data.get('code') != 0:
            print(f"  ❌ 更新失败: code={data.get('code')}, msg={data.get('msg')}")
        else:
            print(f"  ✅ 更新成功!")
            print(f"     响应: {data.get('data', {})}")
    
    # 获取字段列表，查看字段ID和详细配置
    print(f"\n获取字段列表（含详细配置）...")
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    r = requests.get(url, headers=headers)
    data = r.json()
    if data.get('code') == 0:
        fields = data.get('data', {}).get('items', [])
        print(f"  ✅ 共 {len(fields)} 个字段:")
        for f in fields:
            field_name = f.get('field_name')
            field_type = f.get('type')
            print(f"    - {field_name}: field_id={f.get('field_id')}, type={field_type}")
            # 打印单选字段的选项
            if field_type == 4:  # 单选类型
                property_info = f.get('property', {})
                options = property_info.get('options', [])
                print(f"      单选选项: {options}")
    else:
        print(f"  ❌ 获取字段失败: {data}")
    
    print("\n" + "=" * 60)
    print("请检查飞书表格是否有新记录并已更新")
    print("=" * 60)
    return 0

if __name__ == "__main__":
    sys.exit(main())
