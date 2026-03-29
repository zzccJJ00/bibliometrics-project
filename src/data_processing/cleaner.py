"""
数据清洗模块：实现课程要求的数据质量检查（去重、缺失值处理、格式标准化）
"""
import pandas as pd
import numpy as np
from src.utils.config_loader import ConfigLoader

class DataCleaner:
    def __init__(self):
        self.config_loader = ConfigLoader()
        self.synonym_map = self._get_synonym_map()
    
    def _get_synonym_map(self):
        """获取同义词映射表（从synonyms.yaml加载）"""
        synonym_map = {}
        for category, term_map in self.config_loader.synonyms_config.items():
            for standard_term, synonyms in term_map.items():
                for synonym in synonyms:
                    synonym_map[synonym.strip().lower()] = standard_term
        return synonym_map
    
    def clean(self, input_files: list, output_file: str) -> pd.DataFrame:
        """
        数据清洗主函数：
        1. 合并多数据库数据
        2. 去重（基于DOI/标题）
        3. 缺失值处理
        4. 格式标准化
        """
        # 合并多数据库数据
        data_frames = []
        for file in input_files:
            if os.path.exists(file):
                df = pd.read_csv(file, encoding="utf-8-sig")
                data_frames.append(df)
        if not data_frames:
            raise FileNotFoundError("未找到原始数据文件")
        merged_df = pd.concat(data_frames, ignore_index=True)
        
        # 1. 去重（基于DOI或标题，课程要求：重复记录识别）
        merged_df["doi_clean"] = merged_df["原文链接"].str.extract(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", flags=re.IGNORECASE)
        merged_df = merged_df.drop_duplicates(subset=["doi_clean", "标题"], keep="first")
        
        # 2. 缺失值处理（课程要求：缺失字段检测）
        required_fields = ["标题", "作者", "来源", "发表时间", "关键词", "摘要"]
        for field in required_fields:
            if field not in merged_df.columns:
                raise ValueError(f"缺失必需字段：{field}")
        # 删除关键字段缺失的记录
        merged_df = merged_df.dropna(subset=required_fields, how="any")
        
        # 3. 格式标准化
        # 发表时间标准化为YYYY-MM-DD
        merged_df["发表时间"] = pd.to_datetime(merged_df["发表时间"], errors="coerce").dt.strftime("%Y-%m-%d")
        # 关键词格式统一（分号分隔）
        merged_df["关键词"] = merged_df["关键词"].str.replace("，", ";").str.replace(",", ";")
        
        # 4. 数据质量报告（课程要求：记录QC结果）
        self._generate_quality_report(merged_df)
        
        return merged_df
    
    def standardize_keywords(self, df: pd.DataFrame) -> pd.DataFrame:
        """关键词标准化（基于同义词表，避免口径不一致）"""
        def _standardize(keyword_str):
            if pd.isna(keyword_str):
                return ""
            keywords = [kw.strip().lower() for kw in keyword_str.split(";")]
            standardized = [self.synonym_map.get(kw, kw) for kw in keywords]
            return ";".join(list(set(standardized)))  # 去重
        
        df["关键词_标准化"] = df["关键词"].apply(_standardize)
        return df
    
    def _generate_quality_report(self, df: pd.DataFrame):
        """生成数据质量报告"""
        report = f"""
        数据质量检查报告
        ===============
        原始数据条数：{len(df)}
        去重后条数：{len(df.drop_duplicates(subset=["标题"]))}
        关键字段缺失率：
            标题：{df["标题"].isna().sum()/len(df)*100:.2f}%
            作者：{df["作者"].isna().sum()/len(df)*100:.2f}%
            关键词：{df["关键词"].isna().sum()/len(df)*100:.2f}%
        发表时间范围：{df["发表时间"].min()} 至 {df["发表时间"].max()}
        """
        # 保存报告
        with open("reports/data_quality_report.md", "w", encoding="utf-8") as f:
            f.write(report)
        print(report)

if __name__ == "__main__":
    # 测试数据清洗功能
    cleaner = DataCleaner()
    cleaned_data = cleaner.clean(
        input_files=["data/raw/cnki_literature.csv", "data/raw/openalex_literature.csv"],
        output_file="data/processed/cleaned_literature.csv"
    )
    print("数据清洗完成，清洗后数据条数：", len(cleaned_data))
