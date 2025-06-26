import requests
from bs4 import BeautifulSoup
import datetime
import time
import random
import re
import json
import sys
import ssl
import os
from requests.exceptions import SSLError, RequestException

# 配置参数
START_DATE = datetime.date(2025, 1, 1)  # 起始日期
END_DATE = datetime.date.today()  # 结束日期（自动设置为当前日期）
BASE_URL = "https://www.cna.com.tw/news/aipl/{date}{num:04d}.aspx"
VALID_CATEGORIES = {'政治', '國際', '兩岸', '產經', '證券', '科技'}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# 创建数据目录
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 去重文件路径
TITLE_HASH_FILE = "crawled_title_hashes.txt"

# 全局变量存储结果
grouped_articles = {}  # 按频道和日期分组的文章
processed_urls = 0
success_count = 0
error_count = 0
last_save_time = time.time()  # 上次保存时间
SAVE_INTERVAL = 20 * 60  # 20分钟保存一次（秒）
crawled_title_hashes = set()  # 存储已爬取标题的哈希值


def load_crawled_hashes():
    """加载已爬取标题的哈希值"""
    if not os.path.exists(TITLE_HASH_FILE):
        return set()

    try:
        with open(TITLE_HASH_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"加载去重文件失败: {str(e)}")
        return set()


def save_crawled_hash(title_hash):
    """保存新的标题哈希值到文件"""
    try:
        with open(TITLE_HASH_FILE, 'a', encoding='utf-8') as f:
            f.write(title_hash + '\n')
        return True
    except Exception as e:
        print(f"保存去重哈希失败: {str(e)}")
        return False


def generate_title_hash(title):
    """生成标题的哈希标识（MD5）"""
    return hashlib.md5(title.encode('utf-8')).hexdigest()


def generate_dates():
    """生成从结束日期到起始日期的所有日期"""
    current = END_DATE
    while current >= START_DATE:
        yield current.strftime("%Y%m%d")
        current -= datetime.timedelta(days=1)


def extract_category(soup):
    """
    从面包屑导航中提取文章分类
    新的宽松判定逻辑：只要面包屑导航中有任一目标频道即视为有效
    """
    # 查找面包屑导航
    breadcrumb_div = soup.find('div', class_='breadcrumb')
    if not breadcrumb_div:
        return None

    # 查找所有蓝色链接（频道标签）
    category_tags = breadcrumb_div.find_all('a', class_='blue')
    if not category_tags:
        return None

    # 检查所有找到的频道标签是否在有效分类中
    for tag in category_tags:
        category_name = tag.text.strip()
        if category_name in VALID_CATEGORIES:
            return category_name

    return None


def extract_publish_time(soup):
    """提取发布时间并格式化为标准格式"""
    update_div = soup.find('div', class_='updatetime')
    if update_div:
        first_span = update_div.find('span')
        if first_span:
            # 获取原始时间字符串
            raw_time = first_span.text.strip()

            try:
                # 将原始时间转换为datetime对象
                if ":" in raw_time:  # 包含时间
                    dt = datetime.datetime.strptime(raw_time, "%Y/%m/%d %H:%M")
                else:  # 只有日期
                    dt = datetime.datetime.strptime(raw_time, "%Y/%m/%d")
                    # 添加默认时间
                    dt = dt.replace(hour=0, minute=0, second=0)

                # 格式化为标准字符串
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                # 如果解析失败，返回原始字符串
                return raw_time

    return ""


def extract_authors(soup):
    """
    从<div class="names">标签中提取作者信息
    新逻辑：获取所有此类标签的内容，用空格分隔
    """
    names_divs = soup.find_all('div', class_='names')
    if not names_divs:
        return ""

    authors_list = []
    for names_div in names_divs:
        # 提取所有<span class="txt">的内容
        txt_spans = names_div.find_all('span', class_='txt')
        for span in txt_spans:
            # 清理文本：移除前导/尾随空格和特殊字符
            text = span.text.strip()
            if text and not text.isspace():
                # 移除可能的分隔符（如竖线）
                if text.startswith('|'):
                    text = text[1:].strip()
                authors_list.append(text)

    # 用空格连接所有找到的作者信息
    return " ".join(authors_list)


def save_progress():
    """保存当前进度到文件"""
    global grouped_articles

    if not grouped_articles:
        print("没有文章可保存")
        return ""

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DATA_DIR, f'cna_articles_{timestamp}.json')

    try:
        # 将所有文章合并到一个列表中
        all_articles = []
        for articles_list in grouped_articles.values():
            all_articles.extend(articles_list)

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_articles, f, ensure_ascii=False, indent=2)
        print(f"\n已保存 {len(all_articles)} 篇文章到 {filename}")
        return filename
    except Exception as e:
        print(f"保存进度时出错: {str(e)}")
        return ""


def save_grouped_articles(target_date=None):
    """保存分组文章到文件"""
    global grouped_articles

    if not grouped_articles:
        print("没有分组文章可保存")
        return 0

    save_time = datetime.datetime.now()
    save_time_str = save_time.strftime("%H%M%S")  # 当前时间（时分秒）

    saved_files = 0

    # 为每个分组创建单独的文件
    for (category, article_date), articles_list in grouped_articles.items():
        # 如果指定了目标日期，只保存该日期的分组
        if target_date and article_date != target_date:
            continue

        # 格式化文件名
        filename = os.path.join(DATA_DIR, f"62_{category}_{article_date}_{save_time_str}.json")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(articles_list, f, ensure_ascii=False, indent=2)
            print(f"已保存分组文件: {filename} ({len(articles_list)} 篇文章)")
            saved_files += 1
        except Exception as e:
            print(f"保存分组文件 {filename} 时出错: {str(e)}")

    return saved_files


def print_progress():
    """打印当前进度统计"""
    global processed_urls, success_count, error_count

    total_urls = len(list(generate_dates())) * 500
    progress_percent = (processed_urls / total_urls) * 100 if total_urls > 0 else 0

    # 计算分组统计
    grouped_count = sum(len(articles) for articles in grouped_articles.values())

    print("\n" + "=" * 60)
    print(f"爬取进度: {processed_urls}/{total_urls} ({progress_percent:.1f}%)")
    print(f"成功文章: {success_count} | 错误/跳过: {error_count}")
    print(f"分组文章: {grouped_count} 篇 ({len(grouped_articles)} 个分组)")
    print(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


def check_and_save_grouped():
    """检查是否需要保存分组文章"""
    global last_save_time

    current_time = time.time()
    if current_time - last_save_time >= SAVE_INTERVAL:
        print("\n" + "=" * 60)
        print(f"达到20分钟保存间隔，正在保存分组文章...")
        saved_files = save_grouped_articles()
        print(f"已保存 {saved_files} 个分组文件")
        print("=" * 60 + "\n")

        # 更新最后保存时间
        last_save_time = current_time
        return True
    return False


def crawl_articles():
    """主爬虫函数"""
    global grouped_articles, processed_urls, success_count, error_count, last_save_time
    global crawled_title_hashes

    try:
        dates = list(generate_dates())
        total_days = len(dates)
        total_urls = total_days * 500

        # 显示爬取日期范围信息
        print(f"爬取日期范围: {START_DATE} 到 {END_DATE}")
        print(f"总天数: {total_days}, 总URL数: {total_urls}")
        print(f"分组保存间隔: {SAVE_INTERVAL / 60} 分钟")
        print(f"已加载去重记录: {len(crawled_title_hashes)} 条")

        for day_idx, date_str in enumerate(dates):
            print(f"\n{'=' * 60}")
            print(f"处理日期: {date_str} ({day_idx + 1}/{total_days})")
            date_count = 0  # 当前日期文章计数

            for article_num in range(1, 501):  # 0001到0500
                processed_urls += 1

                # 每处理20个URL打印一次进度
                if processed_urls % 20 == 0:
                    print_progress()

                # 检查是否需要保存分组文章
                check_and_save_grouped()

                url = BASE_URL.format(date=date_str, num=article_num)
                print(f"正在爬取: {url}")

                try:
                    # 带延迟的请求（防止被封）
                    delay = random.uniform(0.5, 1.5)
                    time.sleep(delay)

                    response = requests.get(url, headers=HEADERS, timeout=15)

                    # 处理404
                    if response.status_code == 404:
                        print(f"  × 页面不存在 (404) - 跳过")
                        error_count += 1
                        continue

                    # 检查内容类型，跳过非文本响应
                    content_type = response.headers.get('Content-Type', '')
                    if 'text/html' not in content_type:
                        print(f"  × 非HTML内容 - 跳过")
                        error_count += 1
                        continue

                    soup = BeautifulSoup(response.text, 'html.parser')

                    # 提取分类（使用宽松判定）
                    category = extract_category(soup)

                    # 跳过无效分类
                    if not category:
                        print(f"  × 未找到有效分类 - 跳过")
                        error_count += 1
                        continue

                    # 提取标题
                    title_tag = soup.find('h1')
                    if not title_tag:
                        print(f"  × 未找到标题 - 跳过")
                        error_count += 1
                        continue

                    # 获取标题文本（移除HTML标签）
                    title_text_content = title_tag.get_text(strip=True)
                    title_text_display = title_text_content[:50] + "..." if len(
                        title_text_content) > 50 else title_text_content

                    # 检查标题是否已爬取过
                    title_hash = generate_title_hash(title_text_content)
                    if title_hash in crawled_title_hashes:
                        print(f"  × 重复文章: {title_text_display} - 跳过")
                        error_count += 1
                        continue

                    # 提取内容
                    content_div = soup.find('div', class_='paragraph')
                    if not content_div:
                        print(f"  × 未找到内容 - 跳过")
                        error_count += 1
                        continue

                    # 提取内容文本（移除所有<p>标签，只保留文本）
                    content_text = ""
                    for p in content_div.find_all('p'):
                        # 直接获取段落文本，不保留HTML标签
                        content_text += p.get_text(strip=False) + "\n"

                    # 移除末尾多余的换行符
                    content_text = content_text.strip()

                    # 提取元数据
                    publish_time = extract_publish_time(soup)  # 已格式化为标准时间
                    authors = extract_authors(soup)  # 使用新的作者提取方法

                    # 构建结果对象
                    article_data = {
                        "title": title_text_content,  # 使用纯文本标题
                        "content": content_text,  # 使用纯文本内容
                        "sources": {
                            "current_site": "台湾中央社CAN",  # 固定值
                            "current_siteurl": "www.cna.com.tw",
                            "origin_url": url
                        },
                        "metadata": {
                            "publish_time": publish_time,  # 已格式化为标准时间
                            "authors": authors,
                            "category": category
                        },
                        "crawling_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    # 将文章添加到分组
                    group_key = (category, date_str)  # (频道, 日期)
                    if group_key not in grouped_articles:
                        grouped_articles[group_key] = []
                    grouped_articles[group_key].append(article_data)

                    # 记录已爬取标题
                    crawled_title_hashes.add(title_hash)
                    save_crawled_hash(title_hash)

                    date_count += 1
                    success_count += 1
                    print(f"  ✓ 找到文章: {category} - {title_text_display}")
                    if authors:
                        print(f"     作者: {authors}")

                except SSLError as ssl_error:
                    print(f"  × SSL错误: {ssl_error} - 跳过")
                    error_count += 1
                    continue
                except RequestException as req_error:
                    print(f"  × 请求错误: {req_error} - 跳过")
                    error_count += 1
                    continue
                except Exception as e:
                    print(f"  × 处理错误: {str(e)} - 跳过")
                    error_count += 1
                    continue

            print(f"日期 {date_str} 完成: 找到 {date_count} 篇文章")

            # 保存当前日期的分组文章
            print(f"正在保存日期 {date_str} 的分组文章...")
            saved_files = save_grouped_articles(target_date=date_str)
            print(f"已保存 {saved_files} 个分组文件（日期 {date_str}）")

    except KeyboardInterrupt:
        print("\n\n检测到键盘中断，正在保存进度...")
        saved_file = save_progress()
        # 保存分组文章
        save_grouped_articles()
        print_progress()
        sys.exit(f"进度已保存，程序退出")


# 执行爬虫
if __name__ == "__main__":
    # 导入hashlib用于生成标题哈希
    import hashlib

    # 加载已爬取的标题哈希
    crawled_title_hashes = load_crawled_hashes()

    # 显示当前日期信息
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    print(f"爬虫启动时间: {current_date}")
    print(f"开始爬取: {START_DATE} 到 {END_DATE}")
    print(f"目标分类: {', '.join(VALID_CATEGORIES)}")
    print(f"数据保存目录: {os.path.abspath(DATA_DIR)}")
    print(f"已加载去重记录: {len(crawled_title_hashes)} 条")
    print("=" * 60)

    start_time = datetime.datetime.now()
    crawl_articles()
    end_time = datetime.datetime.now()

    total_time = (end_time - start_time).total_seconds()
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    print("\n" + "=" * 60)
    print("爬取完成!")
    print(f"总耗时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒")
    print(f"处理URL总数: {processed_urls}")
    print(f"成功文章数: {success_count}")
    print(f"错误/跳过数: {error_count}")
    print(f"新增去重记录: {len(crawled_title_hashes)} 条")
    if processed_urls > 0:
        success_rate = success_count / processed_urls * 100
        print(f"成功率: {success_rate:.1f}%")

    # 保存分组文章
    print("\n正在保存最终的分组文章...")
    saved_files = save_grouped_articles()
    print(f"已保存 {saved_files} 个分组文件")
    print("=" * 60)

    # 保存最终结果
    if grouped_articles:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(DATA_DIR, f'cna_articles_final_{timestamp}.json')

        try:
            # 合并所有文章到一个列表
            all_articles = []
            for articles_list in grouped_articles.values():
                all_articles.extend(articles_list)

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, ensure_ascii=False, indent=2)
            print(f"\n最终结果已保存到 {filename}")
        except Exception as e:
            print(f"保存最终结果时出错: {str(e)}")
    else:
        print("\n未找到任何文章")