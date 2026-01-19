"""
流水线调度器模块
负责控制流水线步骤的执行顺序和开关
"""
import logging
from enum import Enum
from typing import List, Set, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class PipelineStep(Enum):
    """流水线步骤枚举"""
    DOWNLOAD = "download"
    UPLOAD = "upload"
    EXTRACT = "extract"
    CHECK = "check"
    MOVE_TO_FINAL = "move_to_final"
    NAS_BACKUP = "nas_backup"
    FEISHU_SYNC = "feishu_sync"
    
    @classmethod
    def from_string(cls, step_name: str) -> Optional['PipelineStep']:
        """从字符串创建步骤枚举"""
        try:
            return cls(step_name.lower())
        except ValueError:
            return None
    
    @classmethod
    def all_steps(cls) -> List['PipelineStep']:
        """获取所有步骤（按执行顺序）"""
        return [
            cls.DOWNLOAD,
            cls.UPLOAD,
            cls.EXTRACT,
            cls.CHECK,
            cls.MOVE_TO_FINAL,
            cls.NAS_BACKUP,
            cls.FEISHU_SYNC,
        ]


@dataclass
class StepConfig:
    """步骤配置"""
    enabled: bool = True
    skip_on_error: bool = False  # 失败时是否跳过后续步骤
    retry_count: int = 0         # 重试次数（预留）
    
    def __repr__(self):
        return f"StepConfig(enabled={self.enabled})"


class PipelineScheduler:
    """流水线调度器"""
    
    # 步骤依赖关系（某些步骤依赖其他步骤）
    STEP_DEPENDENCIES = {
        PipelineStep.UPLOAD: [PipelineStep.DOWNLOAD],
        PipelineStep.EXTRACT: [PipelineStep.UPLOAD],
        PipelineStep.CHECK: [PipelineStep.EXTRACT],
        PipelineStep.MOVE_TO_FINAL: [PipelineStep.CHECK],
        PipelineStep.NAS_BACKUP: [PipelineStep.MOVE_TO_FINAL],
    }
    
    # 预设模式
    DEFAULT_PRESETS = {
        'full': [
            PipelineStep.DOWNLOAD,
            PipelineStep.UPLOAD,
            PipelineStep.EXTRACT,
            PipelineStep.CHECK,
            PipelineStep.MOVE_TO_FINAL,
            PipelineStep.NAS_BACKUP,
            PipelineStep.FEISHU_SYNC,
        ],
        'download_only': [
            PipelineStep.DOWNLOAD,
        ],
        'check_only': [
            PipelineStep.CHECK,
            PipelineStep.FEISHU_SYNC,
        ],
        'reprocess': [
            PipelineStep.EXTRACT,
            PipelineStep.CHECK,
            PipelineStep.MOVE_TO_FINAL,
            PipelineStep.FEISHU_SYNC,
        ],
        'sync_only': [
            PipelineStep.FEISHU_SYNC,
        ],
    }
    
    def __init__(self, config: Dict = None, preset: str = None, 
                 enabled_steps: List[str] = None, disabled_steps: List[str] = None):
        """
        初始化调度器
        
        Args:
            config: 配置字典（从 pipeline.yaml 加载）
            preset: 预设模式名称
            enabled_steps: 启用的步骤列表（命令行参数）
            disabled_steps: 禁用的步骤列表（命令行参数）
        """
        self.config = config or {}
        self.steps_config: Dict[PipelineStep, StepConfig] = {}
        self.enabled_steps: Set[PipelineStep] = set()
        
        # 加载配置
        self._load_config(preset, enabled_steps, disabled_steps)
        
        # 验证配置
        self._validate_config()
    
    def _load_config(self, preset: str = None, 
                     enabled_steps: List[str] = None, 
                     disabled_steps: List[str] = None):
        """加载步骤配置"""
        # 1. 从配置文件加载默认配置
        steps_config = self.config.get('steps', {})
        
        # 2. 如果指定了预设模式，使用预设
        if preset:
            preset_steps = self._get_preset_steps(preset)
            if preset_steps:
                logger.info(f"使用预设模式: {preset}")
                # 预设模式：只启用预设中的步骤
                for step in PipelineStep.all_steps():
                    self.steps_config[step] = StepConfig(enabled=(step in preset_steps))
            else:
                logger.warning(f"未知的预设模式: {preset}，使用默认配置")
                self._load_default_config(steps_config)
        else:
            self._load_default_config(steps_config)
        
        # 3. 命令行参数覆盖（优先级最高）
        if enabled_steps:
            # 如果指定了 enabled_steps，先禁用所有，再启用指定的
            for step in PipelineStep.all_steps():
                self.steps_config[step] = StepConfig(enabled=False)
            for step_name in enabled_steps:
                step = PipelineStep.from_string(step_name)
                if step:
                    self.steps_config[step] = StepConfig(enabled=True)
                    logger.info(f"命令行启用步骤: {step.value}")
        
        if disabled_steps:
            # 禁用指定的步骤
            for step_name in disabled_steps:
                step = PipelineStep.from_string(step_name)
                if step and step in self.steps_config:
                    self.steps_config[step].enabled = False
                    logger.info(f"命令行禁用步骤: {step.value}")
        
        # 更新启用的步骤集合
        self.enabled_steps = {
            step for step, config in self.steps_config.items() 
            if config.enabled
        }
    
    def _load_default_config(self, steps_config: Dict):
        """加载默认配置"""
        for step in PipelineStep.all_steps():
            # 默认所有步骤都启用（向后兼容）
            enabled = steps_config.get(step.value, True)
            self.steps_config[step] = StepConfig(enabled=enabled)
    
    def _get_preset_steps(self, preset_name: str) -> Optional[List[PipelineStep]]:
        """获取预设模式的步骤列表"""
        # 先从配置文件查找
        presets = self.config.get('step_presets', {})
        if preset_name in presets:
            step_names = presets[preset_name]
            return [PipelineStep.from_string(name) for name in step_names 
                    if PipelineStep.from_string(name)]
        
        # 再从默认预设查找
        if preset_name in self.DEFAULT_PRESETS:
            return self.DEFAULT_PRESETS[preset_name]
        
        return None
    
    def _validate_config(self):
        """验证配置的合理性"""
        warnings = []
        
        # 检查步骤依赖
        for step, dependencies in self.STEP_DEPENDENCIES.items():
            if self.should_run(step):
                for dep in dependencies:
                    if not self.should_run(dep):
                        warnings.append(
                            f"步骤 '{step.value}' 依赖 '{dep.value}'，但 '{dep.value}' 未启用"
                        )
        
        # 输出警告
        if warnings:
            logger.warning("⚠️  步骤配置可能存在问题:")
            for warning in warnings:
                logger.warning(f"  - {warning}")
            logger.warning("  流水线可能无法正常工作，请检查配置")
    
    def should_run(self, step: PipelineStep) -> bool:
        """判断步骤是否应该运行"""
        return step in self.enabled_steps
    
    def get_step_config(self, step: PipelineStep) -> StepConfig:
        """获取步骤配置"""
        return self.steps_config.get(step, StepConfig(enabled=False))
    
    def get_execution_plan(self) -> List[PipelineStep]:
        """获取执行计划（按顺序）"""
        return [step for step in PipelineStep.all_steps() if self.should_run(step)]
    
    def print_execution_plan(self):
        """打印执行计划"""
        plan = self.get_execution_plan()
        if not plan:
            logger.warning("⚠️  没有启用任何步骤")
            return
        
        logger.info("📋 执行计划:")
        step_names = {
            PipelineStep.DOWNLOAD: "下载 ZIP",
            PipelineStep.UPLOAD: "上传到服务器",
            PipelineStep.EXTRACT: "解压处理",
            PipelineStep.CHECK: "质量检查",
            PipelineStep.MOVE_TO_FINAL: "移动到 final_dir",
            PipelineStep.NAS_BACKUP: "NAS 备份",
            PipelineStep.FEISHU_SYNC: "飞书同步",
        }
        
        for i, step in enumerate(plan, 1):
            logger.info(f"  {i}. {step_names.get(step, step.value)}")
        
        # 显示跳过的步骤
        skipped = [step for step in PipelineStep.all_steps() if not self.should_run(step)]
        if skipped:
            logger.info("⏭  跳过的步骤:")
            for step in skipped:
                logger.info(f"  - {step_names.get(step, step.value)}")
