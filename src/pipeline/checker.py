"""
质量检查模块
负责在远程服务器上检查标注质量
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .ssh_client import SSHClient
from .config import get_config

logger = logging.getLogger(__name__)

# 远程检查脚本路径
REMOTE_CHECKER_SCRIPT = "/tmp/annotation_checker.py"
REMOTE_CHECK_CONFIG = "/tmp/check_config.yaml"


class AnnotationChecker:
    """标注质量检查器"""
    
    def __init__(self, ssh: SSHClient):
        self.ssh = ssh
        self.config = get_config()
        self._script_deployed = False
    
    def deploy_script(self):
        """部署检查脚本到服务器"""
        if self._script_deployed:
            return
        
        # 部署检查脚本
        from .processor import _load_script
        self.ssh.write_file(REMOTE_CHECKER_SCRIPT, _load_script("annotation_checker.py"))
        
        # 上传检查配置
        config_path = Path(self.config.check_config_path)
        if config_path.exists():
            import yaml
            with open(config_path, 'r') as f:
                config_content = yaml.dump(yaml.safe_load(f))
            self.ssh.write_file(REMOTE_CHECK_CONFIG, config_content)
        
        self._script_deployed = True
        logger.info("✅ 检查脚本部署完成")
    
    def check(self, data_dir: str, stem: str) -> Tuple[bool, int, str]:
        """
        检查标注质量
        
        Args:
            data_dir: 远程数据目录路径
            stem: 数据名称
        
        Returns:
            (passed, issue_count, report_path)
        """
        self.deploy_script()
        
        report_path = f"/tmp/report_{stem}.txt"
        
        cmd = (
            f"python3 {REMOTE_CHECKER_SCRIPT} "
            f"--data_dir '{data_dir}' "
            f"--config '{REMOTE_CHECK_CONFIG}' "
            f"--report '{report_path}'"
        )
        
        status, out, err = self.ssh.exec_command(cmd, timeout=120)
        
        if status != 0:
            return False, -1, f"检查脚本失败: {err[:200]}"
        
        # 读取报告判断是否通过
        report_content = self.ssh.read_file(report_path) or ""
        issue_count = report_content.count("帧:")
        
        return issue_count == 0, issue_count, report_path
    
    def download_report(self, remote_report: str, local_dir: Path) -> Optional[Path]:
        """下载检查报告到本地"""
        stem = Path(remote_report).stem.replace("report_", "")
        local_report = local_dir / f"report_{stem}.txt"
        
        if self.ssh.download_file(remote_report, str(local_report)):
            return local_report
        return None
    
    def get_keyframe_count(self, data_dir: str) -> int:
        """获取关键帧数量"""
        sample_paths = [
            f"{data_dir}/sample.json",
            f"{data_dir}/undistorted/sample.json",
        ]
        
        for sample_path in sample_paths:
            if self.ssh.file_exists(sample_path):
                cmd = f"python3 -c \"import json; print(len(json.load(open('{sample_path}'))))\""
                status, out, _ = self.ssh.exec_command(cmd)
                if status == 0 and out.strip().isdigit():
                    return int(out.strip())
        
        return 0
    
    def check_batch(self, data_dirs: List[str]) -> Dict[str, Tuple[bool, int]]:
        """
        批量检查多个数据目录
        
        Returns:
            {stem: (passed, issue_count)}
        """
        results = {}
        
        for data_dir in data_dirs:
            stem = Path(data_dir).name
            passed, issue_count, _ = self.check(data_dir, stem)
            results[stem] = (passed, issue_count)
            
            status = "✓" if passed else f"✗ ({issue_count}个问题)"
            logger.info(f"  {stem}: {status}")
        
        return results
