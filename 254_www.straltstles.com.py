from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import time
import json
import signal
import sys
import logging
import os
from datetime import timezone
import re
import glob
import hashlib
import threading

# 配置日志
log_filename = f"straitstimes_crawler_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

# 声明全局变量
current_channel_articles = {}  # 改为字典，按文章发布日期分组
current_channel_name = ""
current_crawl_date = ""  # 添加当前爬取日期跟踪
last_output_time = time.time()  # 记录上次输出时间
output_lock = threading.Lock()  # 输出锁，防止并发写入问题
periodic_data = {}  # 用于存储周期性输出的数据


# 创建状态目录
os.makedirs('.status', exist_ok=True)


def save_data_on_exit(signal, frame):
    """在程序退出时保存数据的函数"""
    # 先执行周期性输出
    output_periodic_data()
    sys.exit(0)  # 直接退出，不再保存到频道日期文件


# 注册信号处理函数
signal.signal(signal.SIGINT, save_data_on_exit)
signal.signal(signal.SIGTERM, save_data_on_exit)

# 定义频道结构
channel_dict = {
    "国际": {
        "sub_channels": [
            "world/united-states", "world/europe",
            "world/middle-east", "asia"
        ],
        "base_path": "world"
    },
    "经济": {
        "sub_channels": [],
        "base_path": "business"
    },
    "科技": {
        "sub_channels": [],
        "base_path": "tech"
    }
}

# 创建请求会话
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Referer': 'https://www.straitstimes.com/',
    'DNT': '1',
})


def get_current_time_iso():
    """获取当前时间的标准格式（东八区）"""
    tz = timezone(timedelta(hours=8))
    current_time = datetime.now(tz)
    # 返回格式为：YYYY-MM-DD HH:MM:SS
    return current_time.strftime('%Y-%m-%d %H:%M:%S')


def parse_date(date_str):
    """将日期字符串转换为标准格式"""
    try:
        # 尝试解析带时间戳的日期格式，如 "Jun 13, 2025, 05:22 PM"
        date_obj = datetime.strptime(date_str, '%b %d, %Y, %I:%M %p')
        # 转换为标准格式：YYYY-MM-DD HH:MM:SS
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            # 尝试解析日期格式，如 "Jun 13, 2025"
            date_obj = datetime.strptime(date_str, '%b %d, %Y')
            # 转换为标准格式：YYYY-MM-DD 00:00:00
            return date_obj.strftime('%Y-%m-%d 00:00:00')
        except:
            return date_str  # 如果无法解析，返回原始字符串


def extract_article_content(url):
    """从文章URL提取文章内容"""
    try:
        response = session.get(url, timeout=15)
        if response.status_code != 200:
            logging.warning(f"文章页面返回状态码: {response.status_code} - URL: {url}")
            return None, None, None, None

        soup = BeautifulSoup(response.content, 'html.parser')

        # 提取标题 - 根据提供的元素示例
        title_elem = soup.find('h1', class_=re.compile(r'headline'))
        if not title_elem:
            title_elem = soup.find('h1')
        if not title_elem:
            return None, None, None, None
        title = title_elem.get_text(strip=True)

        # 提取作者 - 根据提供的元素示例
        author_elem = soup.find('a', class_=re.compile(r'byline-name'))
        if not author_elem:
            author_elem = soup.find('span', class_=re.compile(r'byline-name'))
        author = author_elem.get_text(strip=True) if author_elem else ""

        # 提取发布时间 - 优先使用meta标签
        time_elem = soup.find('meta', {'property': 'article:published_time'})
        if time_elem:
            publish_time = time_elem.get('content', '')
            # 处理带有时区信息的时间格式
            if '+' in publish_time:
                # 去除时区信息
                publish_time = publish_time.split('+')[0]
            # 转换为标准格式
            try:
                dt = datetime.fromisoformat(publish_time)
                publish_time = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
        else:
            # 如果meta标签不存在，尝试解析页面上的时间元素
            time_button = soup.find('button', class_=re.compile(r'updated-timestamp'))
            if time_button:
                # 提取时间文本并清理
                time_text = time_button.get_text(strip=True)
                # 移除"UPDATED"文本
                time_text = re.sub(r'^UPDATED\s*', '', time_text)
                publish_time = parse_date(time_text)
            else:
                publish_time = ""

        # 提取正文内容 - 根据提供的元素示例
        content_paragraphs = soup.find_all('p', class_=re.compile(r'paragraph-base'))
        if not content_paragraphs:
            # 备用方案：尝试提取文章主体
            content_div = soup.find('div', class_=re.compile(r'article-body'))
            if content_div:
                content_paragraphs = content_div.find_all('p')

        if not content_paragraphs:
            return None, None, None, None

        # 处理内容段落
        paragraphs = []
        for p in content_paragraphs:
            text = p.get_text(strip=True)
            # 跳过空段落和特定文本（如"READ ALSO"）
            if text and not text.startswith('READ ALSO'):
                paragraphs.append(text)

        content = '\n'.join(paragraphs)

        return title, content, publish_time, author

    except Exception as e:
        logging.error(f"提取文章内容错误: {e} - URL: {url}")
        return None, None, None, None


def crawl_single_channel(channel_path, articles_dict, channel_name, date_str):
    """爬取单个频道在特定日期的文章"""
    global last_output_time, periodic_data

    consecutive_empty = 0
    articles_found = 0
    page = 1  # 总是从第一页开始

    logging.info(f"开始爬取频道: {channel_path} (日期: {date_str})，从第 {page} 页开始")

    while consecutive_empty < 5:  # 连续5页无结果时停止
        # 根据频道路径是否包含斜杠构建不同的URL
        if '/' not in channel_path:
            # 不包含斜杠的频道路径使用/latest格式
            url = f"https://www.straitstimes.com/{channel_path}/latest?page={page}&date={date_str}"
        else:
            # 包含斜杠的频道路径使用直接路径格式
            url = f"https://www.straitstimes.com/{channel_path}?page={page}&date={date_str}"

        try:
            logging.debug(f"访问列表页: {url}")
            response = session.get(url, timeout=15)

            if response.status_code != 200:
                consecutive_empty += 1
                logging.warning(f"列表页返回状态码: {response.status_code} - URL: {url}")
                page += 1
                time.sleep(1)
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # 查找文章链接 - 使用更精确的选择器
            article_links = []
            # 尝试两种常见的选择器
            article_items = soup.select('div.views-row') or soup.select('div.list-object')

            for item in article_items:
                link = item.find('a')
                if link and link.get('href'):
                    article_links.append(link)

            # 如果没有找到文章链接
            if not article_links:
                consecutive_empty += 1
                logging.info(f"第 {page} 页没有找到文章 (连续空页: {consecutive_empty})")
                page += 1
                time.sleep(1)
                continue
            else:
                # 重置连续空页计数器
                consecutive_empty = 0

            logging.info(f"第 {page} 页找到 {len(article_links)-1} 篇文章")

            # 处理本页的所有文章
            for link in article_links:
                article_url = "https://www.straitstimes.com" + link['href']
                title, content, publish_time, author = extract_article_content(article_url)

                if not title or not content:
                    continue

                # 确定文章实际发布日期
                article_date = date_str  # 默认使用爬取日期
                if publish_time:
                    # 从发布时间提取日期部分 (YYYY-MM-DD)
                    if len(publish_time) >= 10:
                        try:
                            # 验证是否为有效日期
                            datetime.strptime(publish_time[:10], '%Y-%m-%d')
                            article_date = publish_time[:10]
                        except ValueError:
                            pass

                # 创建文章对象
                article = {
                    "title": title,
                    "content": content,
                    "sources": {
                        "current_site": "The Straits Times",
                        "current_siteurl": "https://www.straitstimes.com/",
                        "origin_url": article_url
                    },
                    "metadata": {
                        "publish_time": publish_time,
                        "authors": author,
                        "category": channel_name
                    },
                    "crawling_time": get_current_time_iso()
                }

                # 按实际日期分组存储
                if article_date not in articles_dict:
                    articles_dict[article_date] = []
                articles_dict[article_date].append(article)
                articles_found += 1
                logging.info(f"成功爬取文章: {title} (发布日期: {article_date}）")

                # 将文章添加到周期性输出数据
                with output_lock:
                    if channel_name not in periodic_data:
                        periodic_data[channel_name] = {}
                    if article_date not in periodic_data[channel_name]:
                        periodic_data[channel_name][article_date] = []
                    periodic_data[channel_name][article_date].append(article)

                # 检查是否需要周期性输出
                current_time = time.time()
                if current_time - last_output_time >= 300:  # 5分钟
                    output_periodic_data()
                    last_output_time = current_time

                # 文章间延迟
                time.sleep(1)

            # 翻页
            page += 1
            time.sleep(1)  # 页间延迟

        except requests.exceptions.RequestException as e:
            logging.error(f"网络错误: {e} - URL: {url}")
            consecutive_empty += 1
            time.sleep(1)
        except Exception as e:
            logging.error(f"处理错误: {e} - URL: {url}")
            consecutive_empty += 1
            time.sleep(1)

    logging.info(f"频道 {channel_path} 爬取完成 (日期: {date_str})，共找到 {articles_found} 篇文章")
    return articles_found


def output_periodic_data():
    """每5分钟输出一次数据到JSON文件"""
    global periodic_data

    if not periodic_data:
        return

    with output_lock:
        # 复制数据并清空
        data_to_output = periodic_data.copy()
        periodic_data = {}

    # 确保data目录存在
    os.makedirs("data", exist_ok=True)

    for channel_name, date_dict in data_to_output.items():
        for date_str, articles in date_dict.items():
            # 生成文件名：254_国际_20250618_055635.json
            file_date = date_str.replace('-', '')
            file_time = datetime.now().strftime("%H%M%S")
            output_filename = f"data/254_{channel_name}_{file_date}_{file_time}.json"

            # 保存到文件
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=4)

            logging.info(
                f"周期性输出: 频道 {channel_name} 日期 {date_str} 的 {len(articles)} 篇文章已保存到 {output_filename}")


def get_latest_article_date(channel_name):
    """获取频道中最新文章的发布日期"""
    # 获取频道目录下所有JSON文件
    json_files = glob.glob(f"data/254_{channel_name}_*.json")
    if not json_files:
        return None

    # 找到最新的文件
    latest_file = max(json_files, key=os.path.getmtime)

    try:
        # 加载文件内容
        with open(latest_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 找到最新文章的发布日期
        latest_date = None
        for article in data:
            pub_time = article['metadata']['publish_time']
            if pub_time:
                # 解析日期字符串
                try:
                    # 尝试标准格式
                    pub_date = datetime.strptime(pub_time, '%Y-%m-%d %H:%M:%S')
                except:
                    # 尝试其他格式
                    try:
                        pub_date = datetime.strptime(pub_time, '%Y-%m-%dT%H:%M:%S')
                    except:
                        continue

                if latest_date is None or pub_date > latest_date:
                    latest_date = pub_date

        return latest_date.date() if latest_date else None

    except Exception as e:
        logging.error(f"获取最新文章日期失败: {e}")
        return None


def crawl_channel_for_date(channel_name, date_str):
    """爬取整个频道在特定日期的内容"""
    global current_channel_articles, current_channel_name, current_crawl_date

    current_channel_articles = {}  # 初始化为空字典
    current_channel_name = channel_name
    current_crawl_date = date_str
    channel_info = channel_dict[channel_name]

    # 检查是否需要跳过爬取
    latest_article_date = get_latest_article_date(channel_name)
    current_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

    # 只有当最新文章日期大于当前爬取日期时才跳过
    if latest_article_date and latest_article_date > current_date_obj:
        logging.info(f"频道 {channel_name} 已有比 {date_str} 更新的文章，跳过爬取")
        return

    logging.info(f"=== 开始爬取 {channel_name} 频道 (日期: {date_str}) ===")

    # 爬取主频道
    crawl_single_channel(channel_info["base_path"], current_channel_articles, channel_name, date_str)

    # 爬取所有子频道
    for sub_channel in channel_info["sub_channels"]:
        channel_path = sub_channel  # 子频道路径已经包含完整路径
        crawl_single_channel(channel_path, current_channel_articles, channel_name, date_str)

    logging.info(f"{channel_name}频道爬取完成 (日期: {date_str})")


def generate_date_range():
    """生成从2025年1月1日到今天的日期列表"""
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()
    date_list = []

    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_list[::-1]  # 从最近日期开始


def main():
    """主函数"""
    global last_output_time

    os.makedirs("data", exist_ok=True)
    date_range = generate_date_range()
    logging.info(f"开始爬取日期范围: {len(date_range)} 天 (从2025-01-01到今天)")

    # 爬取所有频道
    for channel_name in channel_dict.keys():
        # 为每个日期爬取该频道
        for date_str in date_range:
            try:
                crawl_channel_for_date(channel_name, date_str)
            except Exception as e:
                logging.error(f"爬取 {channel_name} 频道 (日期: {date_str}) 时发生错误: {e}")

            # 每天爬取后休息
            time.sleep(1)

    # 程序结束前输出剩余数据
    output_periodic_data()


if __name__ == "__main__":
    main()