"""
服务器端处理模块
负责在远程服务器上解压 ZIP、替换 JSON、检查质量
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from .config import get_config, PipelineConfig
from .ssh_client import SSHClient

logger = logging.getLogger(__name__)

# 远程脚本路径
REMOTE_WORKER_SCRIPT = "/tmp/zip_worker.py"
REMOTE_CHECKER_SCRIPT = "/tmp/annotation_checker.py"
REMOTE_CHECK_CONFIG = "/tmp/check_config.yaml"

# 本地脚本目录
LOCAL_SCRIPTS_DIR = Path(__file__).parent.parent / "remote_scripts"


def _load_script(name: str) -> str:
    """从 remote_scripts 目录加载脚本内容"""
    script_path = LOCAL_SCRIPTS_DIR / name
    if script_path.exists():
        return script_path.read_text(encoding='utf-8')
    raise FileNotFoundError(f"脚本文件不存在: {script_path}")


class RemoteProcessor:
    """远程服务器处理器"""
    
    def __init__(self, ssh: SSHClient, config: PipelineConfig = None):
        self.ssh = ssh
        self.config = config or get_config()
        self._scripts_deployed = False
    
    def deploy_scripts(self):
        """部署远程处理脚本"""
        if self._scripts_deployed:
            return
        
        # 部署 ZIP 处理脚本
        self.ssh.write_file(REMOTE_WORKER_SCRIPT, _load_script("zip_worker.py"))
        
        # 部署检查脚本
        self.ssh.write_file(REMOTE_CHECKER_SCRIPT, _load_script("annotation_checker.py"))
        
        # 上传检查配置
        config_path = Path(self.config.check_config_path)
        if config_path.exists():
            import yaml
            with open(config_path, 'r') as f:
                config_content = yaml.dump(yaml.safe_load(f))
            self.ssh.write_file(REMOTE_CHECK_CONFIG, config_content)
        
        self._scripts_deployed = True
        logger.info("✅ 远程脚本部署完成")
    
    def get_server_state(self) -> Dict:
        """获取服务器状态"""
        server = self.ssh.server
        
        # 获取已有的 ZIP 文件
        zip_files = set()
        processed_zip_stems = set()  # 已处理完成的 ZIP（通过 processed_ 前缀判断）
        files = self.ssh.list_files(server.zip_dir, "*.zip")
        for name in files:
            if name.startswith("processed_"):
                stem = name[len("processed_"):]
                zip_files.add(stem)
                processed_zip_stems.add(stem.replace('.zip', ''))
            else:
                zip_files.add(name)
        
        # 获取已处理完成的目录（只检查当前 final_dir）
        # 如果数据在其他 final_dir 中，但不在当前 final_dir 中，会重新处理
        # 这样可以支持将数据移动到不同的 final_dir（旧数据会保留）
        processed_dirs = set(self.ssh.list_dirs(server.final_dir))
        
        # 获取处理中的目录（断点续传支持）
        processing_dirs = set(self.ssh.list_dirs(server.process_dir))
        
        return {
            "zip_files": zip_files,
            "processed_dirs": processed_dirs,
            "processing_dirs": processing_dirs,
        }
    
    def process_zip(self, zip_path: str, json_path: str, stem: str) -> Tuple[bool, str]:
        """
        在服务器上处理 ZIP 文件（如果有 ZIP）或仅处理 JSON
        返回 (success, error_message)
        """
        server = self.ssh.server
        
        # 上传 JSON 文件（如果还没上传）
        remote_json = f"/tmp/{Path(json_path).name}"
        if not self.ssh.file_exists(remote_json):
            if not self.ssh.upload_file(json_path, remote_json):
                return False, "上传 JSON 文件失败"
        
        # 检查是否有 ZIP 文件
        has_zip = self.ssh.file_exists(zip_path)
        
        if has_zip:
            # 有 ZIP 文件：执行完整的处理脚本
            cmd = (
                f"python3 {REMOTE_WORKER_SCRIPT} "
                f"--zip '{zip_path}' "
                f"--json '{remote_json}' "
                f"--out '{server.process_dir}' "
                f"--rename_json '{self.config.rename_json}'"
            )
            
            status, out, err = self.ssh.exec_command(cmd, timeout=300)
            
            if status != 0:
                return False, f"处理脚本失败: {err}"
        else:
            # 没有 ZIP 文件：仅处理 JSON（创建目录结构并放置 JSON）
            target_dir = f"{server.process_dir}/{stem}"
            self.ssh.mkdir_p(target_dir)
            
            # 确定 JSON 文件名
            json_filename = "annotations.json" if self.config.rename_json else Path(json_path).name
            target_json = f"{target_dir}/{json_filename}"
            
            # 复制 JSON 到目标位置
            status, _, err = self.ssh.exec_command(f"cp '{remote_json}' '{target_json}'")
            if status != 0:
                return False, f"复制 JSON 失败: {err}"
        
        # 注意：ZIP 处理后操作（rename/delete）已移至 move_to_final 中执行
        # 确保只有在整个流程完成后才处理原始 ZIP，避免重复上传问题
        
        return True, ""
    
    def check_annotations(self, data_dir: str, stem: str) -> Tuple[bool, int, str]:
        """
        检查标注质量
        返回 (passed, issue_count, report_path)
        """
        server = self.ssh.server
        # 报告存放在服务器端 process_dir/reports/ 目录
        reports_dir = f"{server.process_dir}/reports"
        self.ssh.mkdir_p(reports_dir)
        report_path = f"{reports_dir}/report_{stem}.txt"
        
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
    
    def get_keyframe_count(self, data_dir: str) -> int:
        """获取关键帧数量"""
        # 检查多个可能的 JSON 文件位置
        sample_paths = [
            f"{data_dir}/sample.json",
            f"{data_dir}/undistorted/sample.json",
            f"{data_dir}/annotations.json",  # JSON-only 模式
        ]
        
        logger.debug(f"🔍 检查关键帧: {data_dir}")
        
        for sample_path in sample_paths:
            if self.ssh.file_exists(sample_path):
                logger.debug(f"  ✓ 找到: {sample_path}")
                # 尝试多种 JSON 格式
                cmd = (
                    f"python3 -c \""
                    f"import json; "
                    f"data = json.load(open('{sample_path}')); "
                    f"print(len(data['frames']) if isinstance(data, dict) and 'frames' in data else len(data))"
                    f"\""
                )
                status, out, err = self.ssh.exec_command(cmd)
                if status == 0 and out.strip().isdigit():
                    count = int(out.strip())
                    logger.info(f"  ✓ 关键帧数: {count}")
                    return count
                else:
                    logger.warning(f"  ✗ 读取失败 status={status}, out={out.strip()}, err={err.strip()}")
            else:
                logger.debug(f"  ✗ 不存在: {sample_path}")
        
        logger.warning(f"⚠ 未找到关键帧数据: {data_dir}")
        return 0
    
    def get_keyframe_count_from_zip(self, zip_path: str) -> int:
        """从ZIP文件中读取关键帧数量（不解压整个ZIP）"""
        # 创建临时目录
        temp_dir = f"/tmp/kf_extract_{Path(zip_path).stem}"
        self.ssh.exec_command(f"rm -rf '{temp_dir}'")
        self.ssh.mkdir_p(temp_dir)
        
        try:
            # 尝试提取 sample.json 或 undistorted/sample.json
            sample_paths = ["sample.json", "undistorted/sample.json"]
            
            for sample_path in sample_paths:
                # 尝试从ZIP中提取特定文件
                extract_cmd = f"unzip -q -j '{zip_path}' '*/{sample_path}' -d '{temp_dir}' 2>/dev/null || true"
                self.ssh.exec_command(extract_cmd)
                
                # 检查是否提取成功
                extracted_file = f"{temp_dir}/sample.json"
                if self.ssh.file_exists(extracted_file):
                    # 读取关键帧数量
                    cmd = (
                        f"python3 -c \""
                        f"import json; "
                        f"data = json.load(open('{extracted_file}')); "
                        f"print(len(data['frames']) if isinstance(data, dict) and 'frames' in data else len(data))"
                        f"\""
                    )
                    status, out, _ = self.ssh.exec_command(cmd)
                    if status == 0 and out.strip().isdigit():
                        count = int(out.strip())
                        logger.info(f"从ZIP读取关键帧: {Path(zip_path).name} -> {count} 帧")
                        return count
            
            logger.warning(f"无法从ZIP中提取sample.json: {zip_path}")
            return 0
        finally:
            # 清理临时目录
            self.ssh.exec_command(f"rm -rf '{temp_dir}'")
    
    def move_to_final(self, stem: str) -> Tuple[bool, str]:
        """移动到最终目录，并清理原始 ZIP"""
        server = self.ssh.server
        src = f"{server.process_dir}/{stem}"
        dst = f"{server.final_dir}/{stem}"
        zip_path = f"{server.zip_dir}/{stem}.zip"
        
        # 检查源目录
        if not self.ssh.dir_exists(src):
            return False, "源目录不存在"
        
        # 如果目标目录已存在，直接删除（不备份）
        if self.ssh.dir_exists(dst):
            self.ssh.exec_command(f"rm -rf '{dst}'")
        
        # 移动
        status, _, err = self.ssh.exec_command(f"mv '{src}' '{dst}'")
        
        if status != 0:
            return False, f"移动失败: {err}"
        
        # 整个流程完成后，处理原始 ZIP（避免中途失败导致重复上传）
        if self.config.zip_after_process == "rename":
            new_name = f"{server.zip_dir}/processed_{stem}.zip"
            self.ssh.exec_command(f"mv '{zip_path}' '{new_name}'")
        elif self.config.zip_after_process == "delete":
            self.ssh.exec_command(f"rm -f '{zip_path}'")
        
        return True, dst
