#!/usr/bin/env python3
"""
结构化日志模块 - JSON 格式日志

使用 Python logging 模块，输出 JSON 格式日志
"""
import logging
import json
import sys
from datetime import datetime
from pathlib import Path


class JSONFormatter(logging.Formatter):
    """JSON 格式化器"""

    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # 添加额外字段
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


class StructuredLogger:
    """结构化日志器"""

    def __init__(self, name: str, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 避免重复添加 handler
        if not self.logger.handlers:
            # 控制台输出
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(JSONFormatter())
            self.logger.addHandler(console_handler)

    def _log_with_extra(self, level: int, message: str, **extra_fields):
        """带额外字段的日志"""
        record = self.logger.makeRecord(
            self.logger.name, level, "", "", (), None, ""
        )
        record.message = message
        record.extra_fields = extra_fields
        self.logger.handle(record)

    def debug(self, message: str, **fields):
        self._log_with_extra(logging.DEBUG, message, **fields)

    def info(self, message: str, **fields):
        self._log_with_extra(logging.INFO, message, **fields)

    def warning(self, message: str, **fields):
        self._log_with_extra(logging.WARNING, message, **fields)

    def error(self, message: str, **fields):
        self._log_with_extra(logging.ERROR, message, **fields)

    def critical(self, message: str, **fields):
        self._log_with_extra(logging.CRITICAL, message, **fields)


def get_logger(name: str, level: str = "INFO") -> StructuredLogger:
    """
    获取结构化日志器

    Args:
        name: 日志器名称
        level: 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）

    Returns:
        StructuredLogger 实例
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    return StructuredLogger(name, log_level)
