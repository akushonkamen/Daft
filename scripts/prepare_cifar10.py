#!/usr/bin/env python3
"""
准备 CIFAR-10 真实数据集

下载 CIFAR-10 并转换为 Parquet 格式，用于真实 Demo。
"""

import os
import sys
from pathlib import Path

# 确保路径
script_dir = Path(__file__).parent
project_root = script_dir.parent
test_data_dir = project_root / "test_data"
test_data_dir.mkdir(exist_ok=True)

sys.path.insert(0, str(project_root))

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def download_cifar10_torchvision():
    """使用 torchvision 下载 CIFAR-10"""
    print("正在下载 CIFAR-10 数据集...")

    try:
        import torchvision
        import torchvision.transforms as transforms
    except ImportError:
        print("安装 torchvision...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "torchvision", "torch"])
        import torchvision
        import torchvision.transforms as transforms

    # 下载数据集
    transform = transforms.Compose([
        transforms.ToTensor(),
    ])

    trainset = torchvision.datasets.CIFAR10(
        root=str(test_data_dir / "cifar10_raw"),
        train=True,
        download=True,
        transform=transform
    )

    print(f"✅ 下载完成: {len(trainset)} 张图像")

    return trainset


def cifar10_to_parquet(dataset, output_path, num_samples=None):
    """将 CIFAR-10 转换为 Parquet 格式

    Args:
        dataset: CIFAR-10 dataset
        output_path: 输出 parquet 文件路径
        num_samples: 转换的样本数量，None 表示全部
    """
    print(f"\n正在转换为 Parquet 格式...")

    if num_samples is None:
        num_samples = len(dataset)

    # CIFAR-10 类别
    classes = ['airplane', 'automobile', 'bird', 'cat', 'deer',
               'dog', 'frog', 'horse', 'ship', 'truck']

    # 准备数据
    ids = []
    images = []  # 存储图像路径/引用
    labels = []
    class_names = []

    # 保存原始图像为 PNG
    images_dir = test_data_dir / "cifar10_images"
    images_dir.mkdir(exist_ok=True)

    try:
        from PIL import Image
        has_pil = True
    except ImportError:
        print("安装 PIL...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow"])
        from PIL import Image
        has_pil = True

    for i in range(min(num_samples, len(dataset))):
        img_tensor, label_idx = dataset[i]

        # 保存图像
        img_path = images_dir / f"cifar10_{i:06d}.png"
        img_np = img_tensor.numpy().transpose(1, 2, 0) * 255
        img_pil = Image.fromarray(img_np.astype('uint8'))
        img_pil.save(img_path)

        # 记录数据
        ids.append(i)
        images.append(str(img_path))
        labels.append(int(label_idx))
        class_names.append(classes[label_idx])

        if (i + 1) % 1000 == 0:
            print(f"  处理进度: {i + 1}/{num_samples}")

    # 创建 Arrow Table
    table = pa.table({
        'id': pa.array(ids, type=pa.int64()),
        'image_path': pa.array(images, type=pa.string()),
        'label': pa.array(labels, type=pa.int32()),
        'class': pa.array(class_names, type=pa.string()),
    })

    # 写入 Parquet
    pq.write_table(table, output_path, compression='snappy')

    print(f"\n✅ Parquet 文件已保存: {output_path}")
    print(f"   总计: {len(ids)} 条记录")
    print(f"   文件大小: {output_path.stat().st_size / 1024 / 1024:.2f} MB")

    # 显示类别分布
    print("\n类别分布:")
    for cls in classes:
        count = sum(1 for c in class_names if c == cls)
        print(f"  {cls:12s}: {count:5d}")

    return output_path


def main():
    """主函数"""
    print("=" * 70)
    print("CIFAR-10 数据集准备")
    print("=" * 70)

    # 1. 下载数据集
    dataset = download_cifar10_torchvision()

    # 2. 转换为 Parquet (使用全部数据或样本)
    output_path = test_data_dir / "cifar10.parquet"

    # 可以指定样本数量，例如 5000 张图像
    # 设置为 None 使用全部 50,000 张
    cifar10_to_parquet(dataset, output_path, num_samples=5000)

    print("\n" + "=" * 70)
    print("✅ 数据准备完成!")
    print(f"   数据文件: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
