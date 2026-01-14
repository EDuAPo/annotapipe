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

import re

import yaml
import requests

logger = logging.getLogger(__name__)


def _extract_text_value(value) -> str:
    """从飞书字段值中提取纯文本（处理复杂对象格式）"""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and len(value) > 0:
        first = value[0]
        if isinstance(first, dict):
            return first.get('text', '')
        return str(first)
    return str(value) if value else ''


def extract_time_key(name: str) -> str:
    """
    从数据包名称中提取时间段作为模糊匹配键
    
    支持的命名格式:
    - 20251226_165741-165910_rere_0 -> 20251226_165741-165910 (去掉后缀)
    - 20251124_132834_to_20251124_133029 -> 20251124_132834_to_20251124_133029 (保持不变)
    - 1209_134548_134748 -> 1209_134548_134748 (保持不变)
    """
    # 格式: YYYYMMDD_HHMMSS-HHMMSS_xxx_n -> 提取 YYYYMMDD_HHMMSS-HHMMSS
    match = re.match(r'^(\d{8}_\d{6}-\d{6})', name)
    if match:
        return match.group(1)
    
    # 其他格式保持不变
    return name


def _load_env_file(env_path: str = "configs/.env"):
    """手动加载 .env 文件到环境变量"""
    env_file = Path(env_path)
    if not env_file.exists():
        return
    try:
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                key, value = key.strip(), value.strip()
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


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
        self._records_cache: Optional[Dict[str, Dict]] = None  # 缓存所有记录
        self._cache_time: Optional[float] = None
        self._init_config()
    
    def _init_config(self):
        """加载配置"""
        try:
            # 先加载 .env 文件
            _load_env_file()
            
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
    
    def _get_token(self, force_refresh: bool = False) -> str:
        """获取 tenant_access_token"""
        # 如果有缓存的token且未过期，直接返回
        if not force_refresh and self._token and self._token_time:
            if time.time() - self._token_time < 6900:  # 提前100秒刷新，避免边界情况
                return self._token
        
        # 获取新token
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
                logger.debug(f"✓ 飞书token获取成功")
                return self._token
            else:
                logger.error(f"获取飞书token失败: code={data.get('code')}, msg={data.get('msg')}")
                # 清除缓存的token
                self._token = None
                self._token_time = None
        except Exception as e:
            logger.error(f"获取飞书token异常: {e}")
            # 清除缓存的token
            self._token = None
            self._token_time = None
        return ""
    
    def _get_headers(self, force_refresh: bool = False) -> Dict[str, str]:
        """获取请求头，如果token无效会自动刷新"""
        token = self._get_token(force_refresh=force_refresh)
        if not token:
            raise Exception("无法获取有效的飞书token")
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _load_all_records(self, force_reload: bool = False) -> Dict[str, Dict]:
        """加载所有记录到缓存，返回 {数据包名称: {record_id, fields}}"""
        # 禁用缓存，每次都重新加载
        if not force_reload and self._records_cache is not None:
            return self._records_cache
        
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        
        all_records = {}
        page_token = None
        page_count = 0
        
        logger.debug(f"📥 加载飞书表格所有记录... (app_token={app_token}, table_id={table_id})")
        
        while True:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            try:
                # 尝试获取数据，如果token失效则刷新后重试
                for attempt in range(2):
                    try:
                        headers = self._get_headers(force_refresh=(attempt > 0))
                        r = requests.get(url, params=params, headers=headers, timeout=30)
                        data = r.json()
                        
                        # 检查是否是token失效错误
                        if data.get('code') == 99991663 and attempt == 0:
                            logger.warning("飞书token失效，刷新后重试...")
                            continue
                        
                        if data.get('code') != 0:
                            logger.error(f"加载记录失败: code={data.get('code')}, msg={data.get('msg')}")
                            break
                        
                        # 成功，跳出重试循环
                        break
                    except Exception as e:
                        if attempt == 0:
                            logger.warning(f"请求失败，重试中: {e}")
                            continue
                        raise
                
                items = data.get('data', {}).get('items', [])
                for item in items:
                    record_id = item.get('record_id')
                    fields = item.get('fields', {})
                    name = _extract_text_value(fields.get('数据包名称', ''))
                    if name and record_id:
                        all_records[name] = {
                            'record_id': record_id,
                            'fields': fields
                        }
                
                page_count += 1
                page_token = data.get('data', {}).get('page_token')
                if not page_token or not data.get('data', {}).get('has_more'):
                    break
                    
            except Exception as e:
                logger.error(f"加载记录异常: {e}")
                break
        
        logger.debug(f"📥 已加载 {len(all_records)} 条记录 ({page_count} 页)")
        
        self._records_cache = all_records
        self._cache_time = time.time()
        return all_records
    
    def _search_record(self, name: str) -> Optional[Dict]:
        """根据数据包名称搜索记录，返回 {record_id, fields} 或 None
        
        匹配策略：
        1. 先从缓存中精确匹配
        2. 如果没找到，再从缓存中模糊匹配（time_key）
        """
        # 加载所有记录到缓存
        all_records = self._load_all_records()
        
        # 提取时间段作为匹配键
        time_key = extract_time_key(name)
        
        # 1. 精确匹配
        if name in all_records:
            logger.debug(f"  ✓ 精确匹配: {name}")
            return all_records[name]
        
        # 2. 模糊匹配：查找包含 time_key 的记录
        for existing_name, record in all_records.items():
            if time_key in existing_name or existing_name in name:
                logger.debug(f"  ✓ 模糊匹配: {name} -> {existing_name}")
                return record
        
        logger.debug(f"  ✗ 未找到: {name} (将新增)")
        return None
    
    def _batch_create_records(self, records_fields: List[Dict]) -> tuple:
        """批量创建记录，返回 (创建数量, 创建的记录列表)"""
        if not records_fields:
            return 0, []
        
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        
        payload = {"records": [{"fields": f} for f in records_fields]}
        
        # 尝试创建，如果token失效则刷新后重试
        for attempt in range(2):
            try:
                headers = self._get_headers(force_refresh=(attempt > 0))
                r = requests.post(url, json=payload, headers=headers, timeout=30)
                data = r.json()
                
                # 检查是否是token失效错误
                if data.get('code') == 99991663 and attempt == 0:
                    logger.warning("飞书token失效，刷新后重试...")
                    continue
                
                if data.get('code') == 0:
                    created_records = data.get('data', {}).get('records', [])
                    created = len(created_records)
                    # 打印创建的记录详情
                    for rec in created_records:
                        rec_id = rec.get('record_id', 'N/A')
                        name = rec.get('fields', {}).get('数据包名称', 'N/A')
                        logger.debug(f"  ✓ 已创建: {name} (record_id={rec_id})")
                    return created, created_records
                else:
                    logger.error(f"批量创建失败: code={data.get('code')}, msg={data.get('msg')}")
                    return 0, []
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"创建请求失败，重试中: {e}")
                    continue
                logger.error(f"批量创建异常: {e}")
                return 0, []
        
        return 0, []
    
    def _batch_update_records(self, records: List[Dict]) -> int:
        """批量更新记录"""
        if not records:
            return 0
        
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        
        # 直接使用字段名称
        payload = {"records": records}
        logger.debug(f"📝 更新请求: record_id={records[0]['record_id'] if records else 'N/A'}, fields={records[0]['fields'] if records else {}}")
        
        # 尝试更新，如果token失效则刷新后重试
        for attempt in range(2):
            try:
                headers = self._get_headers(force_refresh=(attempt > 0))
                r = requests.post(url, json=payload, headers=headers, timeout=30)
                data = r.json()
                logger.debug(f"📝 更新响应: code={data.get('code')}, msg={data.get('msg', 'OK')}")
                
                # 检查是否是token失效错误
                if data.get('code') == 99991663 and attempt == 0:
                    logger.warning("飞书token失效，刷新后重试...")
                    continue
                
                if data.get('code') == 0:
                    updated_records = data.get('data', {}).get('records', [])
                    for rec in updated_records:
                        rec_id = rec.get('record_id', 'N/A')
                        name = rec.get('fields', {}).get('数据包名称', 'N/A')
                        logger.debug(f"  ✓ 已更新: {name} (record_id={rec_id})")
                    return len(updated_records)
                else:
                    logger.error(f"批量更新失败: code={data.get('code')}, msg={data.get('msg')}")
                    if records:
                        logger.error(f"更新记录示例: {records[0]}")
                    return 0
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"更新请求失败，重试中: {e}")
                    continue
                logger.error(f"批量更新异常: {e}")
                return 0
        
        return 0
    
    def _get_path_field_from_pipeline(self, pipeline_config_path: str) -> str:
        """从 pipeline.yaml 读取 final_dir 并转换为飞书列名
        
        例如: /data02/dataset/scenesnew -> 上传data02/dataset/scenesnew
        """
        try:
            config_path = Path(pipeline_config_path)
            if not config_path.exists():
                logger.warning(f"Pipeline配置文件不存在: {pipeline_config_path}，使用默认路径")
                return "上传data02/dataset/scenesnew"
            
            with open(config_path, 'r', encoding='utf-8') as f:
                pipeline_config = yaml.safe_load(f) or {}
            
            # 获取第一个启用的服务器的 final_dir
            servers = pipeline_config.get('servers', [])
            for server in servers:
                if server.get('enabled', True):
                    final_dir = server.get('final_dir', '')
                    if final_dir:
                        # 去掉开头的 '/'，然后加上 '上传' 前缀
                        path_without_slash = final_dir.lstrip('/')
                        path_field = f"上传{path_without_slash}"
                        logger.debug(f"从 pipeline.yaml 读取路径: {final_dir} -> {path_field}")
                        return path_field
            
            logger.warning("未找到启用的服务器配置，使用默认路径")
            return "上传data02/dataset/scenesnew"
        except Exception as e:
            logger.warning(f"读取 pipeline.yaml 失败: {e}，使用默认路径")
            return "上传data02/dataset/scenesnew"
    
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
    
    def track(self, records: List[TrackingRecord], json_dir: str = None, pipeline_config_path: str = "configs/pipeline.yaml") -> Dict[str, Any]:
        """追踪到飞书表格"""
        if not self.is_available:
            logger.warning("飞书追踪器不可用，跳过")
            return {}
        
        # 强制重新加载记录，确保缓存是最新的
        self._load_all_records(force_reload=True)
        
        attributes = self.detect_attributes(json_dir) if json_dir else []
        
        # 从 pipeline.yaml 动态读取 final_dir 并转换为飞书列名
        path_field = self._get_path_field_from_pipeline(pipeline_config_path)
        field_mapping = self.config.get('field_mapping', {})
        
        to_create = []
        to_update = []
        created_names = []
        updated_names = []
        total_keyframes = 0
        
        logger.debug(f"📋 开始处理 {len(records)} 条记录...")
        
        for rec in records:
            total_keyframes += rec.keyframe_count
            
            # 查找是否已存在
            logger.debug(f"🔍 搜索记录: {rec.name}")
            existing = self._search_record(rec.name)
            
            # 注意：飞书多维表格的字段类型
            # 只更新复选框类型的属性/路径字段
            fields = {}
            
            if existing:
                # 更新模式：更新关键帧数、标注情况、更新时间和属性/路径
                existing_fields = existing.get('fields', {})
                
                # 更新关键帧数、标注情况、更新时间（注意字段类型）
                # 关键帧数是文本类型，需要字符串
                # 标注情况是多选类型，需要数组
                # 更新时间是日期时间类型，需要毫秒时间戳
                fields["关键帧数"] = str(rec.keyframe_count)
                fields["标注情况"] = [rec.annotation_status]
                fields["更新时间"] = int(time.time() * 1000)
                
                # 属性列：保留已有的 True 值 + 新增当前属性
                for attr_name in field_mapping.keys():
                    if attr_name.endswith('属性'):
                        if existing_fields.get(attr_name):
                            fields[attr_name] = True
                
                # 新增当前检测到的属性
                for attr in attributes:
                    attr_field = f"{attr}属性"
                    if attr_field in field_mapping:
                        fields[attr_field] = True
                
                # 路径列：保留已有的 True 值 + 新增当前路径
                for field_name in field_mapping.keys():
                    if field_name.startswith('上传'):
                        if existing_fields.get(field_name):
                            fields[field_name] = True
                
                # 新增当前路径
                if rec.uploaded and path_field in field_mapping:
                    fields[path_field] = True
                
                to_update.append({"record_id": existing['record_id'], "fields": fields})
                updated_names.append(rec.name)
            else:
                # 创建模式：设置名称、关键帧数、标注情况、更新时间和复选框字段
                # 注意字段类型：关键帧数是文本，标注情况是多选数组，更新时间是毫秒时间戳
                fields["数据包名称"] = rec.name
                fields["关键帧数"] = str(rec.keyframe_count)
                fields["标注情况"] = [rec.annotation_status]
                fields["更新时间"] = int(time.time() * 1000)
                
                for attr in attributes:
                    attr_field = f"{attr}属性"
                    if attr_field in field_mapping:
                        fields[attr_field] = True
                
                if rec.uploaded and path_field in field_mapping:
                    fields[path_field] = True
                
                to_create.append(fields)
                created_names.append(rec.name)
        
        # 执行批量操作（飞书限制每批 500 条）
        created_count = 0
        updated_count = 0
        
        for i in range(0, len(to_create), 500):
            batch = to_create[i:i+500]
            count, created_records = self._batch_create_records(batch)
            created_count += count
            # 更新缓存，避免后续重复创建
            for rec in created_records:
                name = rec.get('fields', {}).get('数据包名称')
                if name and self._records_cache is not None:
                    self._records_cache[name] = {
                        'record_id': rec.get('record_id'),
                        'fields': rec.get('fields', {})
                    }
            if i + 500 < len(to_create):
                time.sleep(0.5)
        
        for i in range(0, len(to_update), 500):
            batch = to_update[i:i+500]
            updated_count += self._batch_update_records(batch)
            if i + 500 < len(to_update):
                time.sleep(0.5)
        
        logger.info(f"✅ 飞书更新: 新增 {created_count}, 更新 {updated_count}")
        
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
    
    def track(self, records: List[TrackingRecord], json_dir: str = None, pipeline_config_path: str = "configs/pipeline.yaml") -> Dict[str, Any]:
        """追踪记录"""
        if self._use_feishu:
            return self.feishu.track(records, json_dir, pipeline_config_path)
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
