"""
追踪模块
负责飞书表格追踪和本地统计
"""
import os
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime

import yaml
import requests

logger = logging.getLogger(__name__)


@dataclass
class TrackingRecord:
    """追踪记录"""
    name: str
    keyframe_count: int = 0
    annotation_status: str = "已完成"
    uploaded: bool = False
    attributes: List[str] = None
    
    def __post_init__(self):
        if self.attributes is None:
            self.attributes = []


class BaseTracker:
    """追踪器基类"""
    
    def track(self, records: List[TrackingRecord]) -> Dict[str, Any]:
        """追踪记录"""
        raise NotImplementedError
    
    def detect_attributes(self, json_dir: str) -> List[str]:
        """检测数据属性"""
        return []


class LocalTracker(BaseTracker):
    """本地 TXT 追踪器"""
    
    def __init__(self, output_path: str = "local_report.txt"):
        self.output_path = Path(output_path)
    
    def track(self, records: List[TrackingRecord]) -> Dict[str, Any]:
        """写入本地 TXT 报告"""
        with open(self.output_path, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("标注数据处理统计报告\n")
            f.write("=" * 60 + "\n\n")
            
            total_keyframes = 0
            for rec in records:
                f.write(f"数据包: {rec.name}\n")
                f.write(f"  关键帧数: {rec.keyframe_count}\n")
                f.write(f"  标注情况: {rec.annotation_status}\n")
                f.write(f"  已上传: {'是' if rec.uploaded else '否'}\n")
                if rec.attributes:
                    f.write(f"  属性: {', '.join(rec.attributes)}\n")
                f.write("\n")
                total_keyframes += rec.keyframe_count
            
            f.write("-" * 60 + "\n")
            f.write(f"总计: {len(records)} 个数据包, {total_keyframes} 个关键帧\n")
        
        logger.info(f"✅ 本地报告已保存: {self.output_path}")
        
        return {
            "created": len(records),
            "updated": 0,
            "total_keyframes": total_keyframes,
        }


class FeishuTracker(BaseTracker):
    """飞书多维表格追踪器（直接调用飞书 API）"""
    
    def __init__(self, config_path: str = "configs/feishu.yaml"):
        self.config_path = Path(config_path)
        self.config: Dict = {}
        self._token: Optional[str] = None
        self._token_time: Optional[float] = None
        self._available = False
        self._init_config()
    
    def _init_config(self):
        """加载配置"""
        try:
            if not self.config_path.exists():
                logger.warning(f"飞书配置文件不存在: {self.config_path}")
                return
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f) or {}
            
            # 从环境变量获取敏感信息
            self.config['app_id'] = os.environ.get('FEISHU_APP_ID', '')
            self.config['app_secret'] = os.environ.get('FEISHU_APP_SECRET', '')
            
            if self.config.get('app_id') and self.config.get('app_secret'):
                self._available = True
                logger.info("🔗 飞书追踪器初始化成功")
            else:
                logger.warning("飞书凭证未配置 (FEISHU_APP_ID/FEISHU_APP_SECRET)")
        except Exception as e:
            logger.warning(f"飞书配置加载失败: {e}")
    
    @property
    def is_available(self) -> bool:
        return self._available and self.config.get('enabled', True)
    
    def _get_token(self) -> str:
        """获取 tenant_access_token"""
        if self._token and self._token_time:
            if time.time() - self._token_time < 7000:
                return self._token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.config.get('app_id', ''),
            "app_secret": self.config.get('app_secret', '')
        }
        
        try:
            r = requests.post(url, json=payload, timeout=10)
            data = r.json()
            if data.get('code') == 0:
                self._token = data.get('tenant_access_token')
                self._token_time = time.time()
                return self._token
        except Exception as e:
            logger.error(f"获取飞书 Token 失败: {e}")
        return ""
    
    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json"
        }
    
    def _search_record(self, name: str) -> Optional[str]:
        """根据数据包名称搜索记录，返回 record_id"""
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        payload = {
            "filter": {
                "conjunction": "and",
                "conditions": [{
                    "field_name": "数据包名称",
                    "operator": "is",
                    "value": [name]
                }]
            },
            "page_size": 1
        }
        
        try:
            r = requests.post(url, json=payload, headers=self._get_headers(), timeout=15)
            data = r.json()
            if data.get('code') == 0:
                items = data.get('data', {}).get('items', [])
                if items:
                    return items[0].get('record_id')
        except Exception:
            pass
        return None
    
    def _batch_create_records(self, records_fields: List[Dict]) -> int:
        """批量创建记录"""
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        
        payload = {"records": [{"fields": f} for f in records_fields]}
        try:
            r = requests.post(url, json=payload, headers=self._get_headers(), timeout=30)
            data = r.json()
            if data.get('code') == 0:
                return len(data.get('data', {}).get('records', []))
        except Exception as e:
            logger.error(f"批量创建失败: {e}")
        return 0
    
    def _batch_update_records(self, records: List[Dict]) -> int:
        """批量更新记录"""
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        
        payload = {"records": records}
        try:
            r = requests.post(url, json=payload, headers=self._get_headers(), timeout=30)
            data = r.json()
            if data.get('code') == 0:
                return len(records)
        except Exception as e:
            logger.error(f"批量更新失败: {e}")
        return 0
    
    def detect_attributes(self, json_dir: str) -> List[str]:
        """从路径中检测数据属性"""
        attributes = []
        keywords = self.config.get('attribute_keywords', {})
        path_str = str(json_dir).lower()
        
        for attr_name, keywords_list in keywords.items():
            for keyword in keywords_list:
                if keyword.lower() in path_str:
                    attributes.append(attr_name)
                    break
        return attributes
    
    def track(self, records: List[TrackingRecord], json_dir: str = None) -> Dict[str, Any]:
        """追踪到飞书表格"""
        if not self.is_available:
            logger.warning("飞书追踪器不可用，跳过")
            return {}
        
        attributes = self.detect_attributes(json_dir) if json_dir else []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        to_create = []
        to_update = []
        created_names = []
        updated_names = []
        total_keyframes = 0
        
        for rec in records:
            total_keyframes += rec.keyframe_count
            
            fields = {
                "数据包名称": rec.name,
                "标注情况": rec.annotation_status,
                "关键帧数": rec.keyframe_count,
                "更新时间": now,
            }
            
            # 添加属性
            for attr in attributes:
                attr_field = f"{attr}属性"
                if attr_field in self.config.get('field_mapping', {}):
                    fields[attr_field] = True
            
            # 上传状态
            if rec.uploaded:
                fields['上传data02/dataset/scenesnew'] = True
            
            # 查找是否已存在
            record_id = self._search_record(rec.name)
            
            if record_id:
                to_update.append({"record_id": record_id, "fields": fields})
                updated_names.append(rec.name)
            else:
                to_create.append(fields)
                created_names.append(rec.name)
        
        # 执行批量操作（飞书限制每批 500 条）
        created_count = 0
        updated_count = 0
        
        for i in range(0, len(to_create), 500):
            batch = to_create[i:i+500]
            created_count += self._batch_create_records(batch)
            if i + 500 < len(to_create):
                time.sleep(0.5)
        
        for i in range(0, len(to_update), 500):
            batch = to_update[i:i+500]
            updated_count += self._batch_update_records(batch)
            if i + 500 < len(to_update):
                time.sleep(0.5)
        
        logger.info(f"✅ 飞书更新: 新增 {created_count}, 更新 {updated_count}, 关键帧 {total_keyframes}")
        
        return {
            'created': created_names,
            'updated': updated_names,
            'total_keyframes': total_keyframes
        }


class Tracker:
    """统一追踪器，自动选择飞书或本地"""
    
    def __init__(self, feishu_config: str = "configs/feishu.yaml"):
        self.feishu = FeishuTracker(feishu_config)
        self.local = LocalTracker()
        self._use_feishu = self.feishu.is_available
    
    def track(self, records: List[TrackingRecord], json_dir: str = None) -> Dict[str, Any]:
        """追踪记录"""
        if self._use_feishu:
            return self.feishu.track(records, json_dir)
        else:
            return self.local.track(records)
    
    def detect_attributes(self, json_dir: str) -> List[str]:
        """检测数据属性"""
        if self._use_feishu:
            return self.feishu.detect_attributes(json_dir)
        return []


def create_tracking_records(result, keyframe_counts: Dict[str, int]) -> List[TrackingRecord]:
    """从 PipelineResult 创建追踪记录"""
    records = []
    
    # 收集所有处理过的数据名称
    all_names = set()
    all_names.update(result.downloaded)
    all_names.update(result.uploaded)
    all_names.update(result.processed)
    all_names.update(result.check_passed)
    all_names.update(result.check_failed)
    all_names.update(result.moved_to_final)
    all_names.update(result.skipped_server_exists)
    
    for name in sorted(all_names):
        # 确定标注状态
        if name in result.check_passed or name in result.skipped_server_exists:
            status = "已完成"
        elif name in result.check_failed:
            status = "检查不通过"
        else:
            status = "已完成"
        
        # 是否已上传
        uploaded = name in result.moved_to_final or name in result.skipped_server_exists
        
        records.append(TrackingRecord(
            name=name,
            keyframe_count=keyframe_counts.get(name, 0),
            annotation_status=status,
            uploaded=uploaded,
        ))
    
    return records
