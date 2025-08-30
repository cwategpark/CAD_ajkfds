from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta, timezone
import time
import json
import signal
import sys
import logging
import os
import threading
from zoneinfo import ZoneInfo  # NEW: 用于时区转换

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f"yomiuri_crawler_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 全局变量
current_channel_articles = []
current_channel_name = ""
current_date = ""
DATA_DIR = "data"

crawler_state = {
    "running": True,
    "force_restart": False,
    "last_channel": None,
    "last_date": None
}

def save_data_on_exit(signal, frame):
    """在程序退出时保存数据"""
    if current_channel_articles and current_channel_name and current_date:
        chinese_name = channel_to_chinese[current_channel_name]
        current_time = datetime.now().strftime("%H%M%S")
        output_filename = f"241_{chinese_name}_{current_date}_{current_time}.json"
        output_path = os.path.join(DATA_DIR, output_filename)

        os.makedirs(DATA_DIR, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(current_channel_articles, json_file, ensure_ascii=False, indent=4)

        date_txt_path = "241_date.txt"
        try:
            with open(date_txt_path, 'a', encoding='utf-8') as f:
                f.write(output_filename + '\n')
        except Exception as e:
            logging.error(f"写入241_date.txt失败: {e}")
        logging.info(f"\n程序被强制暂停，{chinese_name}频道的数据已保存到 {output_path}")
    sys.exit(0)

signal.signal(signal.SIGINT, save_data_on_exit)
signal.signal(signal.SIGTERM, save_data_on_exit)

channel_dict = {
    "politics": {
        "sub_channels": ["election/togisen/", "election/sangiin/", "election/shugiin/", "election/tochijisen/",
                         "election/archive/", "election/words/", "election/yoron-chosa/"],
        "base_path": "politics/"
    },
    "science": {
        "sub_channels": ["feature/titlelist/originatorprofile/", "feature/titlelist/future-ai/", "column/dreamchaser/",
                         "life/nyancology/column/"],
        "base_path": "science/"
    },
    "economic": {
        "sub_channels": ["feature/titlelist/yomiuri333/", "market/", "hobby/atcars/",
                         "feature/titlelist/top_interview/", "feature/titlelist/land-price/"],
        "base_path": "economic/"
    },
    "sengo": {
        "sub_channels": [],
        "base_path": "sengo/"
    },
}

channel_to_chinese = {
    "politics": "政治",
    "science": "科学",
    "economic": "经济",
    "sengo": "历史"
}

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://www.yomiuri.co.jp/',
    'DNT': '1',
})

def get_current_time_iso():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def crawl_single_path(path, articles, channel_name, date_str):
    """爬取单个路径"""
    consecutive_invalid_count = 0
    articles_found = 0
    logging.info(f"开始爬取路径：{path} (日期: {date_str})")

    for i in range(1, 999):
        formatted_number = str(i).zfill(3)
        if path.endswith('/'):
            url = f"https://www.yomiuri.co.jp/{path}{date_str}-OYT1T50{formatted_number}/"
        else:
            url = f"https://www.yomiuri.co.jp/{path}/{date_str}-OYT1T50{formatted_number}/"

        try:
            response = session.get(url, timeout=10)
            time.sleep(0.1)

            if response.status_code == 404:
                consecutive_invalid_count += 1
                if consecutive_invalid_count >= 150:
                    break
                continue

            if response.status_code != 200:
                consecutive_invalid_count += 1
                if consecutive_invalid_count >= 150:
                    break
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.find('h1', class_='title-article') or soup.find('h1', class_='c-article-title')
            if not title:
                title = soup.find('h1')
                if not title:
                    consecutive_invalid_count += 1
                    if consecutive_invalid_count >= 150:
                        break
                    continue

            paragraphs = []
            for p_count in range(1, 100):
                paragraph = soup.find('p', class_=f'par{p_count}')
                if paragraph:
                    paragraphs.append(paragraph.get_text(strip=True))
                else:
                    break

            if not paragraphs:
                content_div = soup.find('div', class_='article-body') or soup.find('article') or soup.find('div','content')
                if content_div:
                    paragraphs = [p.get_text(strip=True) for p in content_div.find_all('p') if p.get_text(strip=True)]

            if not paragraphs:
                consecutive_invalid_count += 1
                if consecutive_invalid_count >= 150:
                    break
                continue

            # === NEW: 过滤含有 "読者会員" 的文章 ===
            if "読者会員" in "".join(paragraphs):
                logging.info(f"文章包含 読者会員 ，跳过: {url}")
                continue

            consecutive_invalid_count = 0
            article = {}
            article["title"] = title.get_text(strip=True)
            article["content"] = ' '.join(paragraphs)
            article["sources"] = {
                "current_site": "读卖新闻",
                "current_siteurl": "https://www.yomiuri.co.jp/",
                "origin_url": url
            }
            crawling_time = get_current_time_iso()

            # === NEW: 发布时间转东八区 ===
            publish_time_str = None
            time_element = soup.find('time')
            if time_element and time_element.get('datetime'):
                try:
                    dt_value = time_element.get('datetime')
                    publish_dt = datetime.fromisoformat(dt_value.replace('T', ' ').split('+')[0])
                    publish_dt = publish_dt.replace(tzinfo=ZoneInfo("Asia/Tokyo")).astimezone(ZoneInfo("Asia/Shanghai"))
                    publish_time_str = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    publish_time_str = time_element.get('datetime')
            else:
                date_span = soup.find('span', class_='date')
                if date_span:
                    try:
                        publish_dt = datetime.strptime(date_span.get_text(strip=True), "%Y-%m-%d %H:%M")
                        publish_dt = publish_dt.replace(tzinfo=ZoneInfo("Asia/Tokyo")).astimezone(ZoneInfo("Asia/Shanghai"))
                        publish_time_str = publish_dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        publish_time_str = date_span.get_text(strip=True)
                else:
                    publish_time_str = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

            article["metadata"] = {
                "publish_time": publish_time_str,
                "authors": "",
                "category": channel_to_chinese[channel_name]
            }

            author_element = soup.find('div', class_="article-author__item") or soup.find('span', class_='author')
            if author_element:
                article["metadata"]["authors"] = author_element.get_text(strip=True)

            article["crawling_time"] = crawling_time
            articles.append(article)
            articles_found += 1
            logging.info(f"成功爬取文章: {article['title']}")

        except Exception as e:
            consecutive_invalid_count += 1
            logging.error(f"错误 {e} - URL: {url}")
            if consecutive_invalid_count >= 150:
                break
            time.sleep(3)

        if consecutive_invalid_count >= 150:
            break

    logging.info(f"{path} 路径爬取完成 (日期: {date_str})，共 {articles_found} 篇")
    return articles_found

def crawl_channel_for_date(channel_name, date_str):
    global current_channel_articles, current_channel_name, current_date
    current_channel_articles = []
    current_channel_name = channel_name
    current_date = date_str
    channel_info = channel_dict[channel_name]
    chinese_name = channel_to_chinese[channel_name]

    logging.info(f"=== 开始爬取 {chinese_name} (日期: {date_str}) ===")

    save_timer = threading.Timer(300, save_data_periodically)
    save_timer.daemon = True
    save_timer.start()

    crawl_single_path(channel_info["base_path"], current_channel_articles, channel_name, date_str)
    for sub_path in channel_info["sub_channels"]:
        crawl_single_path(sub_path, current_channel_articles, channel_name, date_str)

    save_timer.cancel()
    save_data_periodically()
    logging.info(f"{chinese_name} 完成 (日期: {date_str})，共 {len(current_channel_articles)} 篇")
    current_channel_articles = []

def save_data_periodically():
    if current_channel_articles and current_channel_name and current_date:
        chinese_name = channel_to_chinese[current_channel_name]
        current_time = datetime.now().strftime("%H%M%S")
        output_filename = f"241_{chinese_name}_{current_date}_{current_time}.json"
        output_path = os.path.join(DATA_DIR, output_filename)
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(current_channel_articles, json_file, ensure_ascii=False, indent=4)
        try:
            with open("241_date.txt", 'a', encoding='utf-8') as f:
                f.write(output_filename + '\n')
        except Exception as e:
            logging.error(f"写入241_date.txt失败: {e}")
        logging.info(f"定期保存: {chinese_name} 已保存到 {output_path}")
        save_timer = threading.Timer(300, save_data_periodically)
        save_timer.daemon = True
        save_timer.start()

def generate_date_range():
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()
    date_list = []
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    return date_list[::-1]

def run_crawler():
    os.makedirs(DATA_DIR, exist_ok=True)
    date_range = generate_date_range()
    logging.info(f"开始爬取 {len(date_range)} 天 (2025-01-01 到今天)")

    for channel_name in ["politics", "science", "economic", "sengo"]:
        chinese_name = channel_to_chinese[channel_name]
        date_txt_path = "241_date.txt"
        if os.path.exists(date_txt_path):
            with open(date_txt_path, 'r', encoding='utf-8') as f:
                date_txt_lines = [line.strip() for line in f if line.strip()]
        else:
            date_txt_lines = []

        for date_str in date_range:
            if crawler_state["force_restart"]:
                crawler_state["force_restart"] = False
                logging.info("检测到重启请求，继续爬取...")

            file_prefix = f"241_{chinese_name}_{date_str}"
            if any(line.startswith(file_prefix) for line in date_txt_lines):
                logging.info(f"已记录: {file_prefix}，跳过")
                continue

            try:
                crawler_state["last_channel"] = channel_name
                crawler_state["last_date"] = date_str
                crawl_channel_for_date(channel_name, date_str)
            except Exception as e:
                logging.error(f"爬取 {chinese_name} (日期: {date_str}) 出错: {e}")
                crawler_state["force_restart"] = True
                crawler_state["last_channel"] = channel_name
                crawler_state["last_date"] = date_str
                raise
            time.sleep(10)

def calculate_next_run():
    now = datetime.now()
    if now.hour < 6:
        next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
    else:
        next_run = (now + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
    return (next_run - now).total_seconds()

def main():
    logging.info("首次启动，立即运行爬虫任务")
    while crawler_state["running"]:
        try:
            run_crawler()
            logging.info("爬虫完成，等待下一次运行...")
            sleep_time = calculate_next_run()
            logging.info(f"休眠 {sleep_time/3600:.2f} 小时，直到早上6点")
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"未捕获异常: {e}")
            logging.info("5秒后重启...")
            time.sleep(5)
            crawler_state["force_restart"] = True

if __name__ == "__main__":
    main()
