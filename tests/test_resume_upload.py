#!/usr/bin/env python3
"""
断点续传功能测试脚本

测试场景：
1. 正常上传 - 验证完整上传流程
2. 模拟中断 - 验证断点续传
3. 数据损坏 - 验证 MD5 校验
4. 本地文件修改 - 验证续传前校验

使用方法：
    python test_resume_upload.py [--size SIZE_MB] [--test TEST_NAME]
    
    --size: 测试文件大小（MB），默认 100MB
    --test: 指定测试（normal/interrupt/corrupt/modify），默认运行所有测试
"""
import os
import sys
import time
import hashlib
import argparse
import tempfile
import logging
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from src.pipeline.ssh_client import SSHClient
from src.pipeline.config import get_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def create_test_file(path: Path, size_mb: int) -> str:
    """创建测试文件并返回 MD5"""
    logger.info(f"创建测试文件: {path} ({size_mb}MB)")
    
    chunk_size = 1024 * 1024  # 1MB
    md5_hash = hashlib.md5()
    
    with open(path, 'wb') as f:
        for i in range(size_mb):
            # 使用可重复的随机数据（基于位置）
            data = bytes([((i * 256 + j) % 256) for j in range(chunk_size)])
            f.write(data)
            md5_hash.update(data)
    
    md5 = md5_hash.hexdigest()
    logger.info(f"文件创建完成, MD5: {md5}")
    return md5


def calc_file_md5(path: Path) -> str:
    """计算文件 MD5"""
    md5_hash = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


class ResumeUploadTester:
    """断点续传测试器"""
    
    def __init__(self, size_mb: int = 100):
        self.size_mb = size_mb
        self.config = get_config()
        self.ssh = SSHClient()
        self.test_dir = Path(tempfile.mkdtemp(prefix="resume_test_"))
        self.results = {}
    
    def setup(self):
        """初始化测试环境"""
        logger.info("=" * 60)
        logger.info("初始化测试环境")
        logger.info("=" * 60)
        
        if not self.ssh.connect():
            raise Exception("无法连接 SSH 服务器")
        
        logger.info(f"已连接服务器: {self.ssh.server.ip}")
        logger.info(f"测试目录: {self.test_dir}")
        logger.info(f"测试文件大小: {self.size_mb}MB")
        
        # 确保远程测试目录存在
        self.remote_test_dir = f"{self.ssh.server.zip_dir}/_test_resume"
        self.ssh.mkdir_p(self.remote_test_dir)
        
        # 清理之前的测试文件
        self.ssh.exec_command(f"rm -f {self.remote_test_dir}/*")
        
        return True
    
    def cleanup(self):
        """清理测试环境"""
        logger.info("清理测试环境...")
        
        # 清理远程文件
        self.ssh.exec_command(f"rm -rf {self.remote_test_dir}")
        
        # 清理本地文件
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        
        self.ssh.close()
        logger.info("清理完成")
    
    def test_normal_upload(self) -> bool:
        """测试1: 正常上传"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("测试1: 正常上传")
        logger.info("=" * 60)
        
        local_file = self.test_dir / "test_normal.bin"
        remote_file = f"{self.remote_test_dir}/test_normal.bin"
        
        # 创建测试文件
        expected_md5 = create_test_file(local_file, self.size_mb)
        
        # 上传
        logger.info("开始上传...")
        start_time = time.time()
        success = self.ssh.upload_file(str(local_file), remote_file, verify=True, resume=True)
        elapsed = time.time() - start_time
        
        if not success:
            logger.error("❌ 上传失败")
            return False
        
        logger.info(f"上传完成, 耗时: {elapsed:.1f}秒")
        
        # 验证远程文件
        logger.info("验证远程文件...")
        status, remote_md5, _ = self.ssh.exec_command(f"md5sum '{remote_file}' | cut -d' ' -f1")
        remote_md5 = remote_md5.strip()
        
        if remote_md5 != expected_md5:
            logger.error(f"❌ MD5 不匹配: 期望 {expected_md5}, 实际 {remote_md5}")
            return False
        
        logger.info(f"✅ 测试通过: MD5 匹配 ({expected_md5})")
        return True
    
    def test_interrupt_resume(self) -> bool:
        """测试2: 模拟中断后续传"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("测试2: 模拟中断后续传")
        logger.info("=" * 60)
        
        local_file = self.test_dir / "test_interrupt.bin"
        remote_file = f"{self.remote_test_dir}/test_interrupt.bin"
        temp_file = f"{remote_file}.uploading"
        
        # 创建测试文件
        expected_md5 = create_test_file(local_file, self.size_mb)
        
        # 模拟部分上传（上传前 50%）
        partial_size = (self.size_mb * 1024 * 1024) // 2
        logger.info(f"模拟部分上传: {partial_size / (1024*1024):.1f}MB")
        
        with open(local_file, 'rb') as f:
            partial_data = f.read(partial_size)
        
        # 直接写入临时文件
        with self.ssh._sftp.file(temp_file, 'wb') as rf:
            rf.write(partial_data)
        
        # 验证临时文件大小
        remote_stat = self.ssh._sftp.stat(temp_file)
        logger.info(f"临时文件大小: {remote_stat.st_size / (1024*1024):.1f}MB")
        
        # 现在执行断点续传
        logger.info("执行断点续传...")
        start_time = time.time()
        success = self.ssh.upload_file(str(local_file), remote_file, verify=True, resume=True)
        elapsed = time.time() - start_time
        
        if not success:
            logger.error("❌ 断点续传失败")
            return False
        
        logger.info(f"断点续传完成, 耗时: {elapsed:.1f}秒")
        
        # 验证远程文件
        logger.info("验证远程文件...")
        status, remote_md5, _ = self.ssh.exec_command(f"md5sum '{remote_file}' | cut -d' ' -f1")
        remote_md5 = remote_md5.strip()
        
        if remote_md5 != expected_md5:
            logger.error(f"❌ MD5 不匹配: 期望 {expected_md5}, 实际 {remote_md5}")
            return False
        
        logger.info(f"✅ 测试通过: 断点续传后 MD5 匹配 ({expected_md5})")
        return True
    
    def test_corrupt_detection(self) -> bool:
        """测试3: 检测数据损坏"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("测试3: 检测数据损坏")
        logger.info("=" * 60)
        
        local_file = self.test_dir / "test_corrupt.bin"
        remote_file = f"{self.remote_test_dir}/test_corrupt.bin"
        temp_file = f"{remote_file}.uploading"
        
        # 创建测试文件
        expected_md5 = create_test_file(local_file, self.size_mb)
        
        # 创建一个损坏的临时文件（内容不匹配）
        partial_size = (self.size_mb * 1024 * 1024) // 2
        logger.info(f"创建损坏的临时文件: {partial_size / (1024*1024):.1f}MB")
        
        # 写入错误数据
        corrupt_data = bytes([0xFF] * partial_size)
        with self.ssh._sftp.file(temp_file, 'wb') as rf:
            rf.write(corrupt_data)
        
        # 执行上传（应该检测到损坏并重新上传）
        logger.info("执行上传（应检测到损坏）...")
        start_time = time.time()
        success = self.ssh.upload_file(str(local_file), remote_file, verify=True, resume=True)
        elapsed = time.time() - start_time
        
        if not success:
            logger.error("❌ 上传失败")
            return False
        
        logger.info(f"上传完成, 耗时: {elapsed:.1f}秒")
        
        # 验证远程文件
        logger.info("验证远程文件...")
        status, remote_md5, _ = self.ssh.exec_command(f"md5sum '{remote_file}' | cut -d' ' -f1")
        remote_md5 = remote_md5.strip()
        
        if remote_md5 != expected_md5:
            logger.error(f"❌ MD5 不匹配: 期望 {expected_md5}, 实际 {remote_md5}")
            return False
        
        logger.info(f"✅ 测试通过: 检测到损坏并重新上传，MD5 匹配 ({expected_md5})")
        return True
    
    def test_local_modify_detection(self) -> bool:
        """测试4: 检测本地文件修改"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("测试4: 检测本地文件修改")
        logger.info("=" * 60)
        
        local_file = self.test_dir / "test_modify.bin"
        remote_file = f"{self.remote_test_dir}/test_modify.bin"
        temp_file = f"{remote_file}.uploading"
        
        # 创建原始测试文件
        original_md5 = create_test_file(local_file, self.size_mb)
        
        # 上传前 50% 到临时文件
        partial_size = (self.size_mb * 1024 * 1024) // 2
        logger.info(f"上传部分数据: {partial_size / (1024*1024):.1f}MB")
        
        with open(local_file, 'rb') as f:
            partial_data = f.read(partial_size)
        
        with self.ssh._sftp.file(temp_file, 'wb') as rf:
            rf.write(partial_data)
        
        # 修改本地文件
        logger.info("修改本地文件...")
        with open(local_file, 'r+b') as f:
            f.seek(0)
            f.write(b'\x00' * 1024)  # 修改前 1KB
        
        new_md5 = calc_file_md5(local_file)
        logger.info(f"修改后 MD5: {new_md5}")
        
        # 执行上传（应该检测到本地文件已修改，重新上传）
        logger.info("执行上传（应检测到本地文件已修改）...")
        start_time = time.time()
        success = self.ssh.upload_file(str(local_file), remote_file, verify=True, resume=True)
        elapsed = time.time() - start_time
        
        if not success:
            logger.error("❌ 上传失败")
            return False
        
        logger.info(f"上传完成, 耗时: {elapsed:.1f}秒")
        
        # 验证远程文件（应该是修改后的版本）
        logger.info("验证远程文件...")
        status, remote_md5, _ = self.ssh.exec_command(f"md5sum '{remote_file}' | cut -d' ' -f1")
        remote_md5 = remote_md5.strip()
        
        if remote_md5 != new_md5:
            logger.error(f"❌ MD5 不匹配: 期望 {new_md5}, 实际 {remote_md5}")
            return False
        
        logger.info(f"✅ 测试通过: 检测到本地修改并重新上传，MD5 匹配 ({new_md5})")
        return True
    
    def run_all_tests(self):
        """运行所有测试"""
        try:
            self.setup()
            
            tests = [
                ("normal", self.test_normal_upload),
                ("interrupt", self.test_interrupt_resume),
                ("corrupt", self.test_corrupt_detection),
                ("modify", self.test_local_modify_detection),
            ]
            
            for name, test_func in tests:
                try:
                    self.results[name] = test_func()
                except Exception as e:
                    logger.error(f"测试 {name} 异常: {e}")
                    self.results[name] = False
            
            # 打印汇总
            logger.info("")
            logger.info("=" * 60)
            logger.info("测试汇总")
            logger.info("=" * 60)
            
            all_passed = True
            for name, passed in self.results.items():
                status = "✅ 通过" if passed else "❌ 失败"
                logger.info(f"  {name}: {status}")
                if not passed:
                    all_passed = False
            
            logger.info("")
            if all_passed:
                logger.info("🎉 所有测试通过！断点续传功能可靠。")
            else:
                logger.info("⚠️ 部分测试失败，请检查日志。")
            
            return all_passed
            
        finally:
            self.cleanup()
    
    def run_single_test(self, test_name: str):
        """运行单个测试"""
        test_map = {
            "normal": self.test_normal_upload,
            "interrupt": self.test_interrupt_resume,
            "corrupt": self.test_corrupt_detection,
            "modify": self.test_local_modify_detection,
        }
        
        if test_name not in test_map:
            logger.error(f"未知测试: {test_name}")
            logger.info(f"可用测试: {list(test_map.keys())}")
            return False
        
        try:
            self.setup()
            result = test_map[test_name]()
            return result
        finally:
            self.cleanup()


def main():
    parser = argparse.ArgumentParser(description="断点续传功能测试")
    parser.add_argument("--size", type=int, default=100, help="测试文件大小（MB）")
    parser.add_argument("--test", type=str, default=None, 
                       help="指定测试（normal/interrupt/corrupt/modify）")
    args = parser.parse_args()
    
    tester = ResumeUploadTester(size_mb=args.size)
    
    if args.test:
        success = tester.run_single_test(args.test)
    else:
        success = tester.run_all_tests()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
