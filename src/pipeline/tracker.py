"""
追踪模块
负责飞书表格追踪和本地统计
"""
import os
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
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
    final_dir: str = None
    # 标注统计（自动提取）
    total_annotations: int = 0
    total_frames: int = 0
    box_count: int = 0
    line_count: int = 0
    annotation_type: str = ""
    categories: dict = None          # 原始类别字典 {class_name: total_count}
    frame_categories: dict = None    # 帧级类别字典 {class_name: frame_count}
    # 批次元数据（手动配置）
    scene: str = ""
    weather: str = ""
    lighting: str = ""
    area: str = ""

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
                if rec.total_annotations:
                    f.write(f"  总标注数: {rec.total_annotations}\n")
                    f.write(f"  拉框数: {rec.box_count}\n")
                    f.write(f"  线段数: {rec.line_count}\n")
                    f.write(f"  标注类型: {rec.annotation_type}\n")
                if rec.categories:
                    cat_str = ", ".join(f"{k}:{v}" for k, v in rec.categories.items())
                    f.write(f"  类别分布: {cat_str}\n")
                if rec.scene:
                    f.write(f"  场景: {rec.scene}\n")
                if rec.weather:
                    f.write(f"  天气: {rec.weather}\n")
                if rec.lighting:
                    f.write(f"  光照: {rec.lighting}\n")
                if rec.area:
                    f.write(f"  区域: {rec.area}\n")
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
                r = requests.get(url, params=params, headers=self._get_headers(), timeout=30)
                data = r.json()
                
                if data.get('code') != 0:
                    logger.error(f"加载记录失败: code={data.get('code')}, msg={data.get('msg')}")
                    break
                
                items = data.get('data', {}).get('items', [])
                for item in items:
                    record_id = item.get('record_id')
                    fields = item.get('fields', {})
                    
                    # 将字段ID转换回字段名，以便后续逻辑使用
                    field_mapping = self.config.get('field_mapping', {})
                    id_to_name = {v: k for k, v in field_mapping.items()}
                    converted_fields = {}
                    for field_id, field_value in fields.items():
                        field_name = id_to_name.get(field_id, field_id)  # 如果找不到映射，保持原样
                        converted_fields[field_name] = field_value
                    
                    name = _extract_text_value(converted_fields.get('数据包名称', ''))
                    if name and record_id:
                        all_records[name] = {
                            'record_id': record_id,
                            'fields': converted_fields
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
    
    def get_completed_names(self) -> Set[str]:
        """获取飞书中标注情况为"已完成"的数据包名称集合"""
        if not self.is_available:
            return set()
        if not self._get_token():
            return set()

        all_records = self._load_all_records(force_reload=True)
        completed = set()
        for name, record in all_records.items():
            fields = record.get('fields', {})
            status_val = fields.get('标注情况', [])
            # 多选字段可能是字符串列表或对象列表
            if isinstance(status_val, list):
                status_texts = []
                for item in status_val:
                    if isinstance(item, dict):
                        status_texts.append(item.get('text', ''))
                    else:
                        status_texts.append(str(item))
                if '已完成' in status_texts:
                    completed.add(name)
            elif isinstance(status_val, str) and '已完成' in status_val:
                completed.add(name)
        return completed

    def _batch_create_records(self, records_fields: List[Dict]) -> tuple:
        """批量创建记录，返回 (创建数量, 创建的记录列表)"""
        if not records_fields:
            return 0, []
        
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        
        # batch_create API 使用字段名，不需要转换为字段ID
        payload = {"records": [{"fields": f} for f in records_fields]}
        try:
            r = requests.post(url, json=payload, headers=self._get_headers(), timeout=30)
            data = r.json()
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
        except Exception as e:
            logger.error(f"批量创建异常: {e}")
        return 0, []
    
    def _batch_update_records(self, records: List[Dict]) -> int:
        """批量更新记录"""
        if not records:
            return 0
        
        app_token = self.config.get('app_token', '')
        table_id = self.config.get('table_id', '')
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        
        # batch_update API 也使用字段名，不需要转换为字段ID
        payload = {"records": records}
        logger.debug(f"📝 更新请求: record_id={records[0]['record_id'] if records else 'N/A'}, fields={records[0]['fields'] if records else {}}")
        try:
            r = requests.post(url, json=payload, headers=self._get_headers(), timeout=30)
            data = r.json()
            logger.debug(f"📝 更新响应: code={data.get('code')}, msg={data.get('msg', 'OK')}")
            if data.get('code') == 0:
                updated_records = data.get('data', {}).get('records', [])
                updated_count = len(updated_records)
                logger.debug(f"📝 批量更新成功: 更新了 {updated_count} 条记录")
                for rec in updated_records:
                    rec_id = rec.get('record_id', 'N/A')
                    name = rec.get('fields', {}).get('数据包名称', 'N/A')
                    logger.debug(f"  ✓ 已更新: {name} (record_id={rec_id})")
                return updated_count
            else:
                logger.error(f"批量更新失败: code={data.get('code')}, msg={data.get('msg')}")
                if records:
                    logger.error(f"更新记录示例: {records[0]}")
        except Exception as e:
            logger.error(f"批量更新异常: {e}")
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

    def _format_category_distribution(self, frame_categories: dict, total_frames: int = 0) -> str:
        """将帧级类别字典按映射表格式化为占比过滤后的短字母拼接

        例如: frame_categories = {vehicle.car: 80, traffic_cone: 15, unknown: 5}, total_frames=100
        占比阈值 = 10% -> 只保留 car(80%) 和 cone(15%) -> "CK"
        """
        mapping = self.config.get('category_mapping', {})
        ratio_threshold = self.config.get('category_ratio_threshold', 0.1)

        if total_frames <= 0:
            total_frames = max(frame_categories.values()) if frame_categories else 1

        # 按帧数降序排列，过滤低于占比阈值的类别
        items = sorted(frame_categories.items(), key=lambda x: -x[1])
        letters = []
        for cls, frame_count in items:
            ratio = frame_count / total_frames
            if ratio >= ratio_threshold:
                short = mapping.get(cls, cls[0].upper() if cls else '?')
                letters.append(short)

        return "".join(letters)
    
    def track(self, records: List[TrackingRecord]) -> Dict[str, Any]:
        """追踪到飞书表格"""
        if not self.is_available:
            logger.warning("飞书追踪器不可用，跳过")
            return {}

        # 检查token是否有效
        if not self._get_token():
            logger.warning("飞书Token获取失败，回退到本地追踪")
            return {}

        # 强制重新加载记录，确保缓存是最新的
        self._load_all_records(force_reload=True)

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
            
            # 构建字段
            fields = {}
            
            if existing:
                # 更新模式
                fields["关键帧数"] = str(rec.keyframe_count)
                fields["标注情况"] = [rec.annotation_status]
                fields["更新时间"] = int(time.time() * 1000)

                # 类别字段（占比过滤后的短字母拼接，如 "PKSU"）
                if rec.frame_categories:
                    fields["类别"] = self._format_category_distribution(rec.frame_categories, rec.total_frames)

                # 批次元数据字段（多选类型，需要数组格式）
                if rec.scene:
                    fields["场景"] = [rec.scene]
                if rec.weather:
                    fields["天气"] = [rec.weather]
                if rec.lighting:
                    fields["光照"] = [rec.lighting]
                if rec.area:
                    fields["区域"] = [rec.area]

                to_update.append({"record_id": existing['record_id'], "fields": fields})
                updated_names.append(rec.name)
            else:
                # 创建模式
                fields["数据包名称"] = rec.name
                fields["关键帧数"] = str(rec.keyframe_count)
                fields["标注情况"] = [rec.annotation_status]
                fields["更新时间"] = int(time.time() * 1000)

                # 类别字段
                if rec.frame_categories:
                    fields["类别"] = self._format_category_distribution(rec.frame_categories, rec.total_frames)

                # 批次元数据字段（多选类型，需要数组格式）
                if rec.scene:
                    fields["场景"] = [rec.scene]
                if rec.weather:
                    fields["天气"] = [rec.weather]
                if rec.lighting:
                    fields["光照"] = [rec.lighting]
                if rec.area:
                    fields["区域"] = [rec.area]

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
    
    def track(self, records: List[TrackingRecord]) -> Dict[str, Any]:
        """追踪记录"""
        if self._use_feishu:
            result = self.feishu.track(records)
            if result:  # 如果飞书追踪成功，返回结果
                return result
            else:  # 飞书追踪失败，回退到本地
                logger.info("飞书追踪失败，回退到本地追踪")
                return self.local.track(records)
        else:
            return self.local.track(records)
    
    def detect_attributes(self, json_dir: str) -> List[str]:
        """检测数据属性"""
        if self._use_feishu:
            return self.feishu.detect_attributes(json_dir)
        return []

    def get_completed_names(self) -> Set[str]:
        """获取飞书中已完成的数据包名称集合"""
        if self._use_feishu:
            return self.feishu.get_completed_names()
        return set()


def create_tracking_records(result, keyframe_counts: Dict[str, int],
                            annotation_stats: Dict[str, dict] = None,
                            batch_metadata=None) -> List[TrackingRecord]:
    """从 PipelineResult 创建追踪记录"""
    annotation_stats = annotation_stats or {}
    records = []

    # 只处理成功的数据包（检查通过或已在服务器上）
    successful_names = set()
    successful_names.update(result.check_passed)
    successful_names.update(result.skipped_server_exists)

    for name in sorted(successful_names):
        status = "已完成"
        uploaded = name in result.moved_to_final or name in result.skipped_server_exists
        stats = annotation_stats.get(name, {})

        records.append(TrackingRecord(
            name=name,
            keyframe_count=keyframe_counts.get(name, 0),
            annotation_status=status,
            uploaded=uploaded,
            final_dir=result.final_dirs.get(name),
            total_annotations=stats.get('total_annotations', 0),
            total_frames=stats.get('total_frames', 0),
            box_count=stats.get('box_count', 0),
            line_count=stats.get('line_count', 0),
            annotation_type=stats.get('annotation_type', ''),
            categories=stats.get('categories'),
            frame_categories=stats.get('frame_categories'),
            scene=batch_metadata.scene if batch_metadata else '',
            weather=batch_metadata.weather if batch_metadata else '',
            lighting=batch_metadata.lighting if batch_metadata else '',
            area=batch_metadata.area if batch_metadata else '',
        ))

    return records
