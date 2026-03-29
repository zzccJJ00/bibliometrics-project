"""
数据质量检查脚本：统计缺失率/重复率/歧义率
输出：reports/data_quality_report.md
"""
import pandas as pd
import os
from typing import Dict, Tuple

class DataQualityChecker:
    def __init__(self, raw_data_path: str = "data/raw/"):
        self.raw_data_path = raw_data_path
        self.required_fields = [
            "title", "authors", "affiliations", "year", "venue",
            "abstract", "keywords", "doi", "references"  
        ]

    def load_raw_data(self) -> pd.DataFrame:
        """加载所有原始数据并合并"""
        data_frames = []
        for file in os.listdir(self.raw_data_path):
            if file.endswith(".csv") and not file.startswith("field_dictionary"):
                df = pd.read_csv(os.path.join(self.raw_data_path, file), encoding="utf-8-sig")
                data_frames.append(df)
        if not data_frames:
            raise FileNotFoundError("未找到原始数据文件（CSV格式）")
        return pd.concat(data_frames, ignore_index=True)

    def calculate_missing_rate(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算各字段缺失率（数据质量三指标之一）"""
        missing_rate = {}
        for field in self.required_fields:
            if field in df.columns:
                missing_rate[field] = round(df[field].isna().sum() / len(df) * 100, 2)
            else:
                missing_rate[field] = 100.0  # 字段不存在视为100%缺失
        return missing_rate

    def calculate_duplicate_rate(self, df: pd.DataFrame) -> float:
        """计算重复率（基于DOI/标题，数据质量三指标之一）"""
        # 优先用DOI去重，无DOI则用标题
        if "doi" in df.columns:
            duplicate_count = df.duplicated(subset=["doi"], keep=False).sum()
        else:
            duplicate_count = df.duplicated(subset=["title"], keep=False).sum()
        return round(duplicate_count / len(df) * 100, 2)

    def evaluate_ambiguity_rate(self, df: pd.DataFrame) -> float:
        """评估歧义率（作者/机构消歧难度，数据质量三指标之一）"""
        # 简单评估：同名作者数量占比（实际项目可结合消歧工具）
        if "authors" not in df.columns:
            return 100.0
        # 提取所有作者（分号分隔）
        all_authors = []
        for authors_str in df["authors"].dropna():
            all_authors.extend([auth.strip() for auth in authors_str.split(";")])
        # 统计重复作者名数量（视为潜在歧义）
        unique_authors = set(all_authors)
        ambiguity_count = len(all_authors) - len(unique_authors)
        return round(ambiguity_count / len(all_authors) * 100, 2) if all_authors else 0.0

    def generate_report(self, missing_rate: Dict[str, float], duplicate_rate: float, ambiguity_rate: float) -> None:
        """生成数据质量报告（要求输出到reports目录）"""
        report_content = f"""
# 数据质量报告v0.1
## 报告说明
- 生成时间：[填写当前日期]
- 数据来源：Web of Science、CNKI、OpenAlex
- 原始数据量：{len(self.load_raw_data())}条
- 检索式版本：v0.1

## 数据质量三指标（核心要求）
### 1. 缺失率（各字段缺失比例）
| 字段名 | 缺失率（%） | 备注 |
|--------|-------------|------|
"""
        for field, rate in missing_rate.items():
            status = "✅ 正常" if rate < 10 else "⚠️  需关注" if rate < 30 else "❌ 严重缺失"
            report_content += f"| {field} | {rate} | {status} |\n"
        
        report_content += f"""
### 2. 重复率
- 重复记录占比：{duplicate_rate}%
- 去重依据：DOI优先，标题兜底
- 状态：{"✅ 正常" if duplicate_rate < 5 else "⚠️  需去重"}

### 3. 歧义率
- 作者/机构歧义率：{ambiguity_rate}%
- 评估方式：同名作者数量占比
- 状态：{"✅ 低歧义" if ambiguity_rate < 15 else "⚠️  需消歧"}

## 问题与解决方案（课件Lesson 3要求）
| 问题类型 | 具体描述 | 解决方案 |
|----------|----------|----------|
| 缺失字段 | references字段缺失率{missing_rate.get("references", 0)}% | 重新导出WOS/CNKI数据，确保勾选"参考文献"字段 |
| 重复记录 | 重复率{duplicate_rate}% | 基于DOI+标题联合去重，保留最新版本 |
| 作者歧义 | 歧义率{ambiguity_rate}% | 使用作者机构+研究方向辅助消歧，必要时调用Scholarcy API |

## 数据质量结论
- 整体质量：{"✅ 合格" if duplicate_rate < 5 and max(missing_rate.values()) < 30 else "⚠️  需优化"}
- 是否可用于后续分析：{"是" if duplicate_rate < 5 and max(missing_rate.values()) < 30 else "否，需重新导出/清洗"}
"""
        # 保存报告到reports目录
        with open("reports/data_quality_report.md", "w", encoding="utf-8") as f:
            f.write(report_content)
        print("数据质量报告已生成：reports/data_quality_report.md")

if __name__ == "__main__":
    # 运行数据质量检查（课件要求可一键执行）
    checker = DataQualityChecker()
    df = checker.load_raw_data()
    missing_rate = checker.calculate_missing_rate(df)
    duplicate_rate = checker.calculate_duplicate_rate(df)
    ambiguity_rate = checker.evaluate_ambiguity_rate(df)
    checker.generate_report(missing_rate, duplicate_rate, ambiguity_rate)
