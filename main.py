import pandas as pd
import re
import datetime
from pathlib import Path
from typing import Dict, List
import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor

# 常量定义
MERGE_GROUPS = {
    "看过": ["看过", "在看", "想看"],
    "听过": ["听过", "在听", "想听"],
    "玩过": ["玩过", "在玩", "想玩"],
    "读过": ["读过", "在读", "想读"]
}

COLUMN_MAPPINGS = {
    "标签": {"source": "标签", "target": "标签"},
    "豆瓣评分": {"source": "豆瓣评分", "target": "豆瓣评分"},
    "简介": {"source": "简介", "target": "简介"},
    "NeoDB链接": {"source": "NeoDB链接", "target": "NeoDB链接"}
}

def merge_excel_sheets(input_file: str, output_file: str) -> None:
    """合并Excel中的工作表"""
    sheets = pd.read_excel(input_file, sheet_name=None)
    
    modified = {}
    for name, data in sheets.items():
        data = data.copy()
        data["Status"] = name
        modified[name] = data

    combined = {}
    for group, sheets_to_merge in MERGE_GROUPS.items():
        dfs = [modified[name] for name in sheets_to_merge if name in modified]
        if dfs:
            combined[group] = pd.concat(dfs, ignore_index=True)

    with pd.ExcelWriter(output_file) as writer:
        for name, data in combined.items():
            data.to_excel(writer, sheet_name=name, index=False)

def process_tags(neodb_file: str, douban_file: str, output_file: str) -> None:
    """处理标签合并"""
    douban_data = load_douban_data(douban_file)
    neodb_data = load_neodb_data(neodb_file)
    
    with pd.ExcelWriter(output_file) as writer:
        for category in MERGE_GROUPS.keys():
            process_category(category, douban_data, neodb_data, writer)

def load_douban_data(file_path: str):
    """加载豆瓣数据"""
    return {sheet: pd.read_excel(file_path, sheet_name=sheet) 
            for sheet in MERGE_GROUPS.keys()}

def load_neodb_data(file_path: str):
    """加载NeoDB数据"""
    return pd.read_excel(file_path, sheet_name=None)

def process_category(category: str, douban, neodb, writer) -> None:
    """处理单个分类的数据"""
    try:
        df_mark = neodb.get(category, pd.DataFrame())
        df_z = douban.get(category, pd.DataFrame())
        
        if df_mark.empty or df_z.empty:
            return

        df_mark = safe_column_cast(df_mark, "链接", str)
        df_z = safe_column_cast(df_z, "链接", str)
        
        for col, mapping in COLUMN_MAPPINGS.items():
            if mapping["source"] in df_mark.columns:
                update_column(df_z, df_mark, "链接", mapping["source"], mapping["target"])

        df_z.to_excel(writer, sheet_name=category, index=False)
    except Exception as e:
        print(f"处理分类 {category} 时出错: {str(e)}")

def safe_column_cast(df: pd.DataFrame, col: str, dtype) -> pd.DataFrame:
    """安全转换列类型"""
    df = df.copy()
    if col in df.columns:
        df[col] = df[col].astype(dtype, errors="ignore")
    return df

def update_column(target_df: pd.DataFrame, source_df: pd.DataFrame, 
                 key_col: str, source_col: str, target_col: str) -> None:
    """更新目标数据框的列"""
    if source_col not in source_df.columns:
        return

    mapping = source_df.set_index(key_col)[source_col].to_dict()
    target_df[target_col] = target_df[key_col].map(mapping)

def clean_string(s: str) -> str:
    """清理字符串中的特殊字符"""
    if isinstance(s, str):
        return re.sub(r'[^\x00-\x7F\u4e00-\u9FFF]', '', s)
    return s

def get_user_date_input() -> datetime.datetime:
    """获取用户输入的日期，格式为YYMMDD"""
    while True:
        date_input = input("请输入创建时间（格式为YYMMDD）：")
        if len(date_input) != 6 or not date_input.isdigit():
            print("时间格式不正确，请重新输入！格式应为YYMMDD，例如：231026")
            continue
        try:
            year = int(date_input[:2])
            month = int(date_input[2:4])
            day = int(date_input[4:6])

            current_year = datetime.datetime.now().year
            century = current_year // 100 * 100
            year = century + year
            if year > current_year + 1:
                year -= 100

            date_obj = datetime.datetime(year, month, day, 0, 0, 0)
            return date_obj
        except ValueError:
            print("时间格式不正确，请重新输入！确保年、月、日有效。")

def export_to_csv(input_file: str, user_date: datetime.datetime) -> None:
    """导出为CSV文件，并按创建时间筛选"""
    xlsx = pd.ExcelFile(input_file)

    # 定义一个常见的浏览器User-Agent
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    
    # 使用多线程处理数据
    def process_urls_multithreaded(urls):
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(get_cover_link_from_html, url) for url in urls]
            cover_links = [future.result() for future in futures]
        return cover_links
    
    # 函数：从网页获取HTML并提取封面链接
    def get_cover_link_from_html(url):
        headers = {"User-Agent": USER_AGENT}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            tree = etree.HTML(response.text)
            
            # 修改 XPath 以正确匹配封面图片的 <img> 标签
            cover_element = tree.xpath('//*[@id="item-cover"]/img')
            if cover_element:
                # 提取封面链接，并拼接完整的域名
                cover_src = cover_element[0].get("src")
                if cover_src.startswith("/"):
                    cover_link = "https://neodb.social" + cover_src
                else:
                    cover_link = cover_src  # 如果是完整链接，直接使用
                return cover_link
            else:
                print(f"未找到封面图片链接：{url}")
                return None
        except requests.RequestException as e:
            print(f"请求失败：{e}")
            return None

    for sheet in MERGE_GROUPS.keys():
        try:
            df = pd.read_excel(xlsx, sheet)

            # 检查并转换 '创建时间' 列
            if '创建时间' in df.columns:
                try:
                    df['创建时间'] = pd.to_datetime(df['创建时间'], format='%Y-%m-%d %H:%M:%S')
                    df = df[df['创建时间'] >= user_date]  # 筛选
                except ValueError as e:
                    print(f"转换 {sheet} 中的'创建时间'列出错：{e}。跳过筛选。")
            else:
                print(f"{sheet} 中没有 '创建时间' 列，跳过筛选。")

            # 清理字符串
            for col in df.select_dtypes(include=['object']):
                df[col] = df[col].apply(clean_string)

            # 拆分简介列
            def split_intro(intro, parts):
                return (intro.split(" / ") + [None] * (parts - 1))[:parts]
            
            if sheet == "听过":
                df[["表演者", "发行时间"]] = pd.DataFrame(df["简介"].apply(lambda x: split_intro(x, 2)).tolist(), index=df.index)
            elif sheet == "读过":
                df[["作者", "出版日期", "出版社"]] = pd.DataFrame(df["简介"].apply(lambda x: split_intro(x, 3)).tolist(), index=df.index)
            elif sheet == "玩过":
                df[["类型", "平台", "发行时间"]] = pd.DataFrame(df["简介"].apply(lambda x: split_intro(x, 3)).tolist(), index=df.index)
            elif sheet == "看过":
                df[["年代", "制片国家/地区", "类型", "导演", "演员"]] = pd.DataFrame(df["简介"].apply(lambda x: split_intro(x, 5)).tolist(), index=df.index)

            # 删除原始的“简介”列
            df.drop(columns=["简介"], inplace=True)

            # 获取封面链接
            df["封面"] = process_urls_multithreaded(df["NeoDB链接"])

            # 输出最终CSV文件
            output_file_name = f"zout_final_{sheet}.csv"
            df.to_csv(output_file_name, index=False, encoding='utf-8-sig')
            print(f"更新后的数据已保存到'{output_file_name}'文件中。")

        except Exception as e:
            print(f"处理 {sheet} 时出错: {str(e)}")

def main() -> None:
    # 合并NeoDB数据
    merge_excel_sheets("marks.xlsx", "mark.xlsx")
    
    # 合并豆瓣数据
    merge_excel_sheets("z.xlsx", "z1.xlsx")
    
    # 处理标签合并
    process_tags("mark.xlsx", "z1.xlsx", "mark_updated.xlsx")
    
    # 获取用户输入的日期
    user_date = get_user_date_input()

    # 导出CSV文件，并进行筛选
    export_to_csv("mark_updated.xlsx", user_date)
    
    print("所有处理已完成！")

if __name__ == "__main__":
    main()
