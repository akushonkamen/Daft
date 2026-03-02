#!/usr/bin/env python3
"""
性能指标收集器

收集查询耗时、成功率、错误率等指标
"""
import time
import threading
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict, field


@dataclass
class MetricPoint:
    """单个指标数据点"""
    timestamp: float
    name: str
    value: float
    unit: str
    tags: Dict[str, str] = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)


class MetricsCollector:
    """指标收集器"""

    def __init__(self):
        self.metrics: List[MetricPoint] = []
        self.counters: Dict[str, int] = {}
        self.timers: Dict[str, float] = {}
        self._lock = threading.Lock()

    def record_metric(self, name: str, value: float, unit: str = "ms", **tags):
        """记录指标"""
        with self._lock:
            metric = MetricPoint(
                timestamp=time.time(),
                name=name,
                value=value,
                unit=unit,
                tags=tags
            )
            self.metrics.append(metric)

    def increment_counter(self, name: str, delta: int = 1, **tags):
        """增加计数器"""
        with self._lock:
            key = f"{name}_{tags}"
            self.counters[key] = self.counters.get(key, 0) + delta

    def start_timer(self, name: str) -> str:
        """启动计时器"""
        timer_id = f"{name}_{time.time()}_{id(self)}"
        self.timers[timer_id] = time.time()
        return timer_id

    def stop_timer(self, timer_id: str, name: str, **tags):
        """停止计时器并记录"""
        if timer_id in self.timers:
            elapsed = (time.time() - self.timers[timer_id]) * 1000  # ms
            self.record_metric(name, elapsed, "ms", **tags)
            del self.timers[timer_id]
            return elapsed
        return None

    def get_summary(self) -> Dict:
        """获取指标摘要"""
        with self._lock:
            summary = {
                "total_metrics": len(self.metrics),
                "total_counters": len(self.counters),
                "metrics_by_name": {},
            }

            # 按名称分组统计
            for metric in self.metrics:
                if metric.name not in summary["metrics_by_name"]:
                    summary["metrics_by_name"][metric.name] = {
                        "count": 0,
                        "total": 0,
                        "avg": 0,
                        "min": float('inf'),
                        "max": 0,
                    }
                stats = summary["metrics_by_name"][metric.name]
                stats["count"] += 1
                stats["total"] += metric.value
                stats["min"] = min(stats["min"], metric.value)
                stats["max"] = max(stats["max"], metric.value)
                stats["avg"] = stats["total"] / stats["count"]

            return summary

    def print_summary(self):
        """打印指标摘要"""
        summary = self.get_summary()

        print("\n📊 性能指标摘要")
        print("=" * 50)
        print(f"总指标数: {summary['total_metrics']}")
        print(f"总计数器数: {summary['total_counters']}")

        if summary['metrics_by_name']:
            print("\n按名称分组的指标:")
            for name, stats in summary['metrics_by_name'].items():
                print(f"\n{name}:")
                print(f"  次数: {stats['count']}")
                print(f"  总计: {stats['total']:.2f} ms")
                print(f"  平均: {stats['avg']:.2f} ms")
                print(f"  最小: {stats['min']:.2f} ms")
                print(f"  最大: {stats['max']:.2f} ms")

    def export_json(self, filepath: str):
        """导出为 JSON 文件"""
        import json
        with open(filepath, 'w') as f:
            json.dump({
                "metrics": [m.to_dict() for m in self.metrics],
                "counters": self.counters,
                "summary": self.get_summary()
            }, f, indent=2)


# 全局指标收集器
_global_collector: Optional[MetricsCollector] = None


def get_collector() -> MetricsCollector:
    """获取全局指标收集器"""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector
