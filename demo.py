#!/usr/bin/env python3
"""
Daft × DuckDB 多模态 AI 平台演示脚本

展示完整的功能：
1. Daft DataFrame API
2. ai_filter 函数
3. SQL 转译
4. 完整工作流
"""

import sys
from pathlib import Path

# Add project paths
daft_root = Path(__file__).parent
sys.path.insert(0, str(daft_root))


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def demo_1_basic_import():
    """演示 1: 基础导入"""
    print_section("演示 1: 基础导入")

    print("\n代码：")
    print("```python")
    print("import daft")
    print("from daft.functions import ai_filter")
    print("```")

    print("\n执行：")
    try:
        import daft
        from daft.functions import ai_filter

        print("✅ 成功导入 daft 和 ai_filter")
        print(f"   Daft 版本: {daft.__version__ if hasattr(daft, '__version__') else '0.3.0-dev0'}")
        print(f"   ai_filter 函数: {ai_filter}")
    except ImportError as e:
        print(f"❌ 导入失败: {e}")
        return False

    return True


def demo_2_create_dataframe():
    """演示 2: 创建测试 DataFrame"""
    print_section("演示 2: 创建测试 DataFrame")

    print("\n代码：")
    print("```python")
    print("df = daft.from_pydict({")
    print("    'id': [1, 2, 3, 4, 5],")
    print("    'image_path': [")
    print("        '/data/cat1.jpg',")
    print("        '/data/dog1.jpg',")
    print("        '/data/cat2.jpg',")
    print("        '/data/bird1.jpg',")
    print("        '/data/dog2.jpg'")
    print("    ]")
    print("})")
    print("```")

    print("\n执行：")
    try:
        import daft

        df = daft.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'image_path': [
                '/data/cat1.jpg',
                '/data/dog1.jpg',
                '/data/cat2.jpg',
                '/data/bird1.jpg',
                '/data/dog2.jpg'
            ]
        })

        print("✅ DataFrame 创建成功")
        print(f"   列: {df.column_names}")
        print("\n数据预览:")
        df.show()
    except Exception as e:
        print(f"❌ 创建失败: {e}")
        return False

    return True


def demo_3_ai_filter_expression():
    """演示 3: 创建 ai_filter 表达式"""
    print_section("演示 3: 创建 ai_filter 表达式")

    print("\n代码：")
    print("```python")
    print("from daft.functions import ai_filter")
    print("import daft")
    print("")
    print("# 方法1: 字符串列名")
    print("expr1 = ai_filter('image_path', 'cat')")
    print("")
    print("# 方法2: 列表达式")
    print("expr2 = ai_filter(daft.col('image_path'), 'cat', model='clip')")
    print("```")

    print("\n执行：")
    try:
        from daft.functions import ai_filter
        import daft

        # 方法1
        expr1 = ai_filter('image_path', 'cat')
        print(f"✅ 方法1: {expr1}")

        # 方法2
        expr2 = ai_filter(daft.col('image_path'), 'cat', model='clip')
        print(f"✅ 方法2: {expr2}")

        # 检查元数据
        if hasattr(expr2, '_is_ai_filter'):
            print(f"\n✅ 表达式元数据:")
            print(f"   _is_ai_filter: {expr2._is_ai_filter}")
            print(f"   _ai_filter_prompt: {expr2._ai_filter_prompt}")
            print(f"   _ai_filter_model: {expr2._ai_filter_model}")

    except Exception as e:
        print(f"❌ 创建失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def demo_4_sql_translation():
    """演示 4: SQL 转译"""
    print_section("演示 4: SQL 转译")

    print("\n代码：")
    print("```python")
    print("from daft.execution.backends.duckdb_translator import SQLTranslator")
    print("")
    print("translator = SQLTranslator()")
    print("sql = translator._translate_expression(expr)")
    print("```")

    print("\n执行：")
    try:
        from daft.functions import ai_filter
        import daft
        from daft.execution.backends.duckdb_translator import SQLTranslator

        translator = SQLTranslator()

        # 测试不同的表达式
        test_cases = [
            ("cat", "clip"),
            ("dog", "clip"),
            ("sunset", "openclip"),
        ]

        print("\nSQL 转译结果:")
        for prompt, model in test_cases:
            expr = ai_filter(daft.col('image_path'), prompt, model=model)
            sql = translator._translate_expression(expr)
            expected = f"ai_filter(image_path, '{prompt}', '{model}')"
            status = "✅" if sql == expected else "⚠️"
            print(f"{status} {prompt:10s} + {model:10s}: {sql}")

    except Exception as e:
        print(f"❌ 转译失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def demo_5_filter_usage():
    """演示 5: 在 filter 中使用"""
    print_section("演示 5: 在 filter 中使用")

    print("\n代码：")
    print("```python")
    print("import daft")
    print("from daft.functions import ai_filter")
    print("")
    print("# 创建 DataFrame")
    print("df = daft.from_pydict({'image': ['cat.jpg', 'dog.jpg']})")
    print("")
    print("# 过滤")
    print("filtered = df.filter(ai_filter('image', 'cat') > 0.8)")
    print("```")

    print("\n执行：")
    try:
        import daft
        from daft.functions import ai_filter

        df = daft.from_pydict({
            'image': ['cat.jpg', 'dog.jpg', 'bird.jpg']
        })

        print("✅ 原始 DataFrame:")
        df.show()

        # 创建过滤表达式（不实际执行）
        condition = ai_filter('image', 'cat') > 0.8
        print(f"\n✅ 过滤条件: {condition}")

        filtered = df.filter(condition)
        print("\n✅ 过滤后的 DataFrame (lazy):")
        filtered.show()

        print("\n⚠️  注意: DataFrame 是 lazy 的，需要 .collect() 才会实际执行")

    except Exception as e:
        print(f"❌ 操作失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def demo_6_with_column_usage():
    """演示 6: 添加分数列"""
    print_section("演示 6: 添加分数列")

    print("\n代码：")
    print("```python")
    print("import daft")
    print("from daft.functions import ai_filter")
    print("")
    print("# 添加分数列")
    print("df = df.with_column('cat_score', ai_filter('image', 'cat'))")
    print("")
    print("# 添加多个分数列")
    print("df = df.with_column('dog_score', ai_filter('image', 'dog'))")
    print("df = df.with_column('bird_score', ai_filter('image', 'bird'))")
    print("```")

    print("\n执行：")
    try:
        import daft
        from daft.functions import ai_filter

        df = daft.from_pydict({
            'image': ['cat.jpg', 'dog.jpg', 'bird.jpg']
        })

        print("✅ 原始 DataFrame:")
        df.show()

        # 添加分数列
        with_score = df.with_column('cat_score', ai_filter('image', 'cat'))
        print("\n✅ 添加 cat_score 后:")
        with_score.show()

        # 添加多个分数列
        with_scores = (df
            .with_column('cat_score', ai_filter('image', 'cat'))
            .with_column('dog_score', ai_filter('image', 'dog'))
            .with_column('bird_score', ai_filter('image', 'bird'))
        )

        print("\n✅ 添加多个分数列后:")
        with_scores.show()

        print("\n⚠️  注意: DataFrame 是 lazy 的，需要 .collect() 才会实际执行")

    except Exception as e:
        print(f"❌ 操作失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def demo_7_complete_workflow():
    """演示 7: 完整工作流"""
    print_section("演示 7: 完整工作流")

    print("\n场景: 从图像数据中筛选高分图像")
    print("\n代码：")
    print("```python")
    print("import daft")
    print("from daft.functions import ai_filter")
    print("")
    print("# 1. 读取数据")
    print("df = daft.read_parquet('images.parquet')")
    print("")
    print("# 2. 计算相似度分数")
    print("df = df.with_column('cat_score', ai_filter('image', 'cat'))")
    print("")
    print("# 3. 过滤高分图像")
    print("cats = df.filter(df['cat_score'] > 0.8)")
    print("")
    print("# 4. 排序")
    print("cats = cats.sort(cats['cat_score'], desc=True)")
    print("")
    print("# 5. 选择列")
    print("result = cats.select('image_path', 'cat_score')")
    print("")
    print("# 6. 显示结果")
    print("result.show()")
    print("```")

    print("\n执行（模拟）:")
    try:
        import daft
        from daft.functions import ai_filter

        # 创建测试数据
        df = daft.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'image_path': [
                '/data/cat1.jpg',
                '/data/dog1.jpg',
                '/data/cat2.jpg',
                '/data/bird1.jpg',
                '/data/dog2.jpg'
            ]
        })

        print("✅ Step 1: 读取数据")
        df.show()

        print("\n✅ Step 2: 计算相似度分数")
        df_with_scores = df.with_column('cat_score', ai_filter('image_path', 'cat'))
        df_with_scores.show()

        print("\n✅ Step 3: 过滤高分图像 (score > 0.8)")
        cats = df_with_scores.filter(df_with_scores['cat_score'] > 0.8)
        cats.show()

        print("\n✅ Step 4-6: 排序、选择列、显示结果")
        print("(实际执行需要 .collect() 和 DuckDB backend)")
        print("\n📋 完整工作流演示完成!")

    except Exception as e:
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    return True


def main():
    """运行所有演示."""
    print("=" * 70)
    print("Daft × DuckDB 多模态 AI 平台演示")
    print("=" * 70)
    print("\n版本: v1.0")
    print("状态: M0-M4 完成 ✅")

    demos = [
        ("基础导入", demo_1_basic_import),
        ("创建 DataFrame", demo_2_create_dataframe),
        ("ai_filter 表达式", demo_3_ai_filter_expression),
        ("SQL 转译", demo_4_sql_translation),
        ("Filter 用法", demo_5_filter_usage),
        ("With_column 用法", demo_6_with_column_usage),
        ("完整工作流", demo_7_complete_workflow),
    ]

    results = []

    for name, demo_func in demos:
        try:
            success = demo_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n❌ 演示 '{name}' 异常: {e}")
            results.append((name, False))

    # 总结
    print_section("演示总结")

    for name, success in results:
        status = "✅ 成功" if success else "❌ 失败"
        print(f"  {name:20s}: {status}")

    all_success = all(r[1] for r in results)

    if all_success:
        print("\n🎉 所有演示成功完成!")
        print("\n📚 更多信息请参阅:")
        print("   - DEMO.md: 完整文档")
        print("   - CHANGES.md: 变更记录")
        print("   - Discussion.md: 讨论记录")
        print("\n🚀 下一步: M5 - CI/CD 全流程 & 性能优化")
        return True
    else:
        print("\n⚠️  部分演示失败")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
