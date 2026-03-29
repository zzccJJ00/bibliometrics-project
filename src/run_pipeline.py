"""
一键运行脚本：数据采集→处理→分析→可视化
课程要求：可复现、一键运行、输出清晰
"""
import os
import sys
import time
import logging

# 配置日志（可追溯运行过程）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("run_pipeline.log"), logging.StreamHandler()]
)

def run_data_acquisition():
    """步骤1：数据采集（调用collector模块）"""
    logging.info("=== 开始数据采集 ===")
    from src.data_acquisition.collector import LiteratureCollector
    from src.utils.config_loader import ConfigLoader
    
    # 固定随机种子
    os.environ["PYTHONHASHSEED"] = "0"
    
    try:
        # 加载配置
        config_loader = ConfigLoader(
            query_path="config/query.yaml",
            synonyms_path="config/synonyms.yaml"
        )
        collector = LiteratureCollector(config_loader)
        
        # 采集指定任务（可配置）
        task_id = "任务ID_001"
        # 采集CNKI文献
        collector.collect_cnki(
            task_id=task_id,
            save_path="data/raw/cnki_literature.csv",
            page_limit=10
        )
        # 采集OpenAlex文献
        collector.collect_openalex(
            task_id=task_id,
            save_path="data/raw/openalex_literature.csv",
            per_page=200
        )
        logging.info("=== 数据采集完成，原始数据保存至 data/raw/ ===")
        return True
    except Exception as e:
        logging.error(f"数据采集失败：{e}")
        return False

def run_data_processing():
    """步骤2：数据处理（清洗、去重、标准化）"""
    logging.info("=== 开始数据处理 ===")
    from src.data_processing.cleaner import DataCleaner
    
    try:
        cleaner = DataCleaner()
        # 加载原始数据
        cnki_data = "data/raw/cnki_literature.csv"
        openalex_data = "data/raw/openalex_literature.csv"
        
        # 数据清洗（去重、缺失值处理、格式标准化）
        cleaned_data = cleaner.clean(
            input_files=[cnki_data, openalex_data],
            output_file="data/processed/cleaned_literature.csv"
        )
        # 关键词标准化（基于synonyms.yaml）
        cleaned_data = cleaner.standardize_keywords(cleaned_data)
        # 保存处理后的数据
        cleaned_data.to_csv("data/processed/cleaned_literature.csv", index=False, encoding="utf-8-sig")
        
        logging.info("=== 数据处理完成，清洗后数据保存至 data/processed/ ===")
        return True
    except Exception as e:
        logging.error(f"数据处理失败：{e}")
        return False

def run_metrics_analysis():
    """步骤3：计量分析（核心算法执行）"""
    logging.info("=== 开始计量分析 ===")
    from src.metrics_analysis.analyzer import MetricsAnalyzer
    
    try:
        analyzer = MetricsAnalyzer()
        data = "data/processed/cleaned_literature.csv"
        
        # 关键词共现网络分析
        co_occurrence_network = analyzer.keyword_co_occurrence(data, top_n=50)
        # 作者合作网络分析
        author_network = analyzer.author_collaboration(data, top_n=30)
        # 突现检测（研究热点识别）
        burst_terms = analyzer.burst_detection(data, time_slice=3)
        
        # 保存分析结果
        analyzer.save_network(co_occurrence_network, "outputs/tables/keyword_co_occurrence.csv")
        analyzer.save_network(author_network, "outputs/tables/author_collaboration.csv")
        burst_terms.to_csv("outputs/tables/burst_terms.csv", index=False, encoding="utf-8-sig")
        
        logging.info("=== 计量分析完成，结果保存至 outputs/tables/ ===")
        return True
    except Exception as e:
        logging.error(f"计量分析失败：{e}")
        return False

def run_visualization():
    """步骤4：可视化（生成知识图谱等）"""
    logging.info("=== 开始可视化 ===")
    from src.visualization.visualizer import Visualizer
    
    try:
        visualizer = Visualizer()
        # 加载分析结果
        co_occurrence_data = "outputs/tables/keyword_co_occurrence.csv"
        author_network_data = "outputs/tables/author_collaboration.csv"
        burst_terms_data = "outputs/tables/burst_terms.csv"
        
        # 生成关键词共现图谱
        visualizer.plot_keyword_co_occurrence(co_occurrence_data, "outputs/figures/keyword_co_occurrence.png")
        # 生成作者合作网络图谱
        visualizer.plot_author_collaboration(author_network_data, "outputs/figures/author_collaboration.html")
        # 生成突现术语时序图
        visualizer.plot_burst_terms(burst_terms_data, "outputs/figures/burst_terms.png")
        
        logging.info("=== 可视化完成，图表保存至 outputs/figures/ ===")
        return True
    except Exception as e:
        logging.error(f"可视化失败：{e}")
        return False

def main():
    """主流程：按顺序执行所有步骤"""
    start_time = time.time()
    logging.info("=== 启动交通智能管理系统文献计量学分析流水线 ===")
    
    # 执行流程（一步失败则终止）
    steps = [
        ("数据采集", run_data_acquisition),
        ("数据处理", run_data_processing),
        ("计量分析", run_metrics_analysis),
        ("可视化", run_visualization)
    ]
    
    for step_name, step_func in steps:
        if not step_func():
            logging.error(f"=== 流水线终止：{step_name}步骤失败 ===")
            sys.exit(1)
    
    total_time = time.time() - start_time
    logging.info(f"=== 流水线执行完成！总耗时：{total_time:.2f}秒 ===")
    logging.info("=== 所有输出文件已保存至 outputs/ 目录 ===")

if __name__ == "__main__":
    main()
