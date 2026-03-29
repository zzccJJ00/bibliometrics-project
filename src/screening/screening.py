"""
PRISMA筛选流程实现
功能：实现三阶段筛选，生成含reason code的筛选记录与PRISMA流程图
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyArrowPatch
import os
from typing import List, Dict

class PRISMAScreener:
    def __init__(self, input_data_path: str = "data/processed/cleaned_literature.csv", output_dir: str = "outputs/screening/"):
        self.input_data = pd.read_csv(input_data_path, encoding="utf-8-sig")
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        # 筛选规则（与reports/screening_rules.md对齐）
        self.inclusion_criteria = {
            "year": (2015, 2024),
            "doc_type": ["Article", "Review", "Conference Paper", "期刊论文", "学位论文", "会议论文"],
            "language": ["中文", "英文"],
            "required_fields": ["title", "authors", "abstract", "keywords", "doi"]
        }
        # Reason code映射（课件Lesson 4标准）
        self.reason_codes = {
            "E1": "非本主题",
            "E2": "无全文",
            "E3": "方法不符",
            "E4": "时间不符",
            "E5": "语言不符",
            "E6": "质量不符",
            "E7": "重复记录"
        }

    def _check_duplicates(self) -> pd.DataFrame:
        """阶段0：去重（基于DOI，课件Lesson 3要求）"""
        initial_count = len(self.input_data)
        # 去重，保留第一条
        cleaned_data = self.input_data.drop_duplicates(subset=["doi"], keep="first")
        duplicate_count = initial_count - len(cleaned_data)
        # 记录重复文献（E7）
        duplicate_records = self.input_data[self.input_data.duplicated(subset=["doi"], keep=False)].copy()
        duplicate_records["筛选阶段"] = "去重"
        duplicate_records["筛选结果"] = "排除"
        duplicate_records["reason_code"] = "E7"
        duplicate_records["reason_desc"] = self.reason_codes["E7"]
        return cleaned_data, duplicate_records

    def _title_abstract_screening(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """阶段1：标题/摘要初筛（课件Lesson 4要求）"""
        screening_results = []
        included = []
        for idx, row in data.iterrows():
            result = {
                "doi": row["doi"],
                "title": row["title"],
                "authors": row["authors"],
                "year": row["year"],
                "筛选阶段": "初筛（标题+摘要）",
                "筛选结果": "纳入",
                "reason_code": "",
                "reason_desc": ""
            }
            # 检查时间（E4）
            if not (self.inclusion_criteria["year"][0] <= row["year"] <= self.inclusion_criteria["year"][1]):
                result["筛选结果"] = "排除"
                result["reason_code"] = "E4"
                result["reason_desc"] = self.reason_codes["E4"]
            # 检查语言（E5）
            elif row.get("language") not in self.inclusion_criteria["language"]:
                result["筛选结果"] = "排除"
                result["reason_code"] = "E5"
                result["reason_desc"] = self.reason_codes["E5"]
            # 检查主题相关性（E1）
            elif not self._check_topic_relevance(row["title"], row["abstract"]):
                result["筛选结果"] = "排除"
                result["reason_code"] = "E1"
                result["reason_desc"] = self.reason_codes["E1"]
            # 纳入
            if result["筛选结果"] == "纳入":
                included.append(row)
            screening_results.append(result)
        return pd.DataFrame(included), pd.DataFrame(screening_results)

    def _check_topic_relevance(self, title: str, abstract: str) -> bool:
        """辅助函数：检查主题相关性（与query.yaml关键词对齐）"""
        core_keywords = ["交通智能管理", "智能交通", "拥堵治理", "交通流调度", "AI", "大数据", "机器学习", "车路协同", "V2X", "intelligent traffic", "traffic congestion", "machine learning", "deep learning"]
        text = f"{title} {abstract}".lower()
        return any(keyword.lower() in text for keyword in core_keywords)

    def _full_text_screening(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """阶段2：全文复筛（课件Lesson 4要求）"""
        screening_results = []
        included = []
        for idx, row in data.iterrows():
            result = {
                "doi": row["doi"],
                "title": row["title"],
                "authors": row["authors"],
                "year": row["year"],
                "筛选阶段": "复筛（全文）",
                "筛选结果": "纳入",
                "reason_code": "",
                "reason_desc": ""
            }
            # 检查全文获取（E2）
            if pd.isna(row.get("full_text_link")) or row["full_text_link"] == "":
                result["筛选结果"] = "排除"
                result["reason_code"] = "E2"
                result["reason_desc"] = self.reason_codes["E2"]
            # 检查方法相关性（E3）
            elif not self._check_method_relevance(row["abstract"], row["keywords"]):
                result["筛选结果"] = "排除"
                result["reason_code"] = "E3"
                result["reason_desc"] = self.reason_codes["E3"]
            # 检查文献质量（E6）
            elif not self._check_quality(row["venue"], row["cited_by_count"]):
                result["筛选结果"] = "排除"
                result["reason_code"] = "E6"
                result["reason_desc"] = self.reason_codes["E6"]
            # 纳入
            if result["筛选结果"] == "纳入":
                included.append(row)
            screening_results.append(result)
        return pd.DataFrame(included), pd.DataFrame(screening_results)

    def _check_method_relevance(self, abstract: str, keywords: str) -> bool:
        """辅助函数：检查方法相关性"""
        method_keywords = ["AI", "人工智能", "大数据", "机器学习", "深度学习", "计算机视觉", "强化学习", "machine learning", "deep learning", "artificial intelligence", "big data"]
        text = f"{abstract} {keywords}".lower()
        return any(method.lower() in text for method in method_keywords)

    def _check_quality(self, venue: str, cited_by_count: int) -> bool:
        """辅助函数：检查文献质量（课件Lesson 3数据质量要求）"""
        # 核心期刊/高被引筛选
        core_venues = ["中国公路学报", "交通运输工程学报", "IEEE Transactions on Intelligent Transportation Systems", "Transportation Research Part C", "Journal of Intelligent Transportation Systems"]
        if venue in core_venues:
            return True
        # 被引次数筛选（≥5次）
        return cited_by_count >= 5 if not pd.isna(cited_by_count) else False

    def generate_prisma_plot(self, stats: Dict[str, int]) -> None:
        """生成PRISMA流程图（课件Lesson 4要求）"""
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis('off')

        # 定义流程节点
        nodes = [
            ("Identification\n检索结果", 5, 9, stats["initial"]),
            ("Screening\n标题/摘要初筛", 5, 7, stats["after_screening1"]),
            ("Eligibility\n全文复筛", 5, 5, stats["after_screening2"]),
            ("Included\n最终纳入", 5, 3, stats["final_included"])
        ]

        # 绘制节点
        for label, x, y, count in nodes:
            rect = patches.Rectangle((x-1.5, y-0.5), 3, 1, linewidth=2, edgecolor='black', facecolor='white')
            ax.add_patch(rect)
            ax.text(x, y+0.1, label, ha='center', va='center', fontsize=11, fontweight='bold')
            ax.text(x, y-0.1, f"n={count}", ha='center', va='center', fontsize=10)

        # 绘制排除节点
        exclude_nodes = [
            ("排除：重复记录\nn={}".format(stats["excluded_duplicate"]), 2, 8, 'E7'),
            ("排除：时间/语言/主题\nn={}".format(stats["excluded_screening1"]), 2, 6, 'E1/E4/E5'),
            ("排除：无全文/方法/质量\nn={}".format(stats["excluded_screening2"]), 2, 4, 'E2/E3/E6')
        ]

        for label, x, y, code in exclude_nodes:
            rect = patches.Rectangle((x-1.5, y-0.5), 3, 1, linewidth=2, edgecolor='gray', facecolor='lightgray')
            ax.add_patch(rect)
            ax.text(x, y, label, ha='center', va='center', fontsize=9, color='gray')

        # 绘制箭头
        arrows = [
            (5, 8.5, 5, 8.0),  # Identification → Screening
            (5, 6.5, 5, 6.0),  # Screening → Eligibility
            (5, 4.5, 5, 4.0),  # Eligibility → Included
            (3.5, 8.5, 2.0, 8.5),  # Identification → 重复排除
            (3.5, 6.5, 2.0, 6.5),  # Screening → 初筛排除
            (3.5, 4.5, 2.0, 4.5)   # Eligibility → 复筛排除
        ]

        for x1, y1, x2, y2 in arrows:
            arrow = FancyArrowPatch((x1, y1), (x2, y2), arrowstyle='->', linewidth=2, color='black')
            ax.add_patch(arrow)

        # 保存图片
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "PRISMA.png"), dpi=300, bbox_inches='tight')
        print("PRISMA流程图已生成：", os.path.join(self.output_dir, "PRISMA.png"))

    def run_screening(self) -> None:
        """运行完整筛选流程（课件Lesson 4要求）"""
        print("=== 开始PRISMA筛选流程 ===")
        # 阶段0：去重
        data_after_duplicate, duplicate_records = self._check_duplicates()
        # 阶段1：标题/摘要初筛
        data_after_screening1, screening1_records = self._title_abstract_screening(data_after_duplicate)
        # 阶段2：全文复筛
        data_after_screening2, screening2_records = self._full_text_screening(data_after_screening1)

        # 统计筛选数据（用于PRISMA图）
        stats = {
            "initial": len(self.input_data),
            "excluded_duplicate": len(duplicate_records),
            "after_screening1": len(data_after_screening1),
            "excluded_screening1": len(screening1_records[screening1_records["筛选结果"] == "排除"]),
            "after_screening2": len(data_after_screening2),
            "excluded_screening2": len(screening2_records[screening2_records["筛选结果"] == "排除"]),
            "final_included": len(data_after_screening2)
        }

        # 合并所有筛选记录
        all_screening_records = pd.concat([duplicate_records, screening1_records, screening2_records], ignore_index=True)
        # 保存筛选记录（含reason code，课件Lesson 4要求）
        all_screening_records.to_csv(os.path.join(self.output_dir, "screening.csv"), index=False, encoding="utf-8-sig")
        # 保存最终纳入文献
        data_after_screening2.to_csv(os.path.join(self.output_dir, "included_literature.csv"), index=False, encoding="utf-8-sig")

        # 生成PRISMA流程图
        self.generate_prisma_plot(stats)

        # 输出筛选统计
        print(f"=== 筛选流程完成 ===")
        print(f"初始文献量：{stats['initial']}")
        print(f"去重排除：{stats['excluded_duplicate']}（E7）")
        print(f"初筛排除：{stats['excluded_screening1']}（E1/E4/E5）")
        print(f"复筛排除：{stats['excluded_screening2']}（E2/E3/E6）")
        print(f"最终纳入：{stats['final_included']}")
        print(f"筛选记录：{os.path.join(self.output_dir, 'screening.csv')}")
        print(f"最终文献：{os.path.join(self.output_dir, 'included_literature.csv')}")

if __name__ == "__main__":
    # 运行筛选流程（课件要求可一键执行）
    screener = PRISMAScreener()
    screener.run_screening()
