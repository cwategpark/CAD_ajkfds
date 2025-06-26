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
import threading


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',  # 设置日志时间格式
    handlers=[
        logging.FileHandler(f"yomiuri_crawler_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 声明一个全局变量来存储当前频道的文章数据
current_channel_articles = []
current_channel_name = ""
current_date = ""


def save_data_on_exit(signal, frame):
    """在程序退出时保存数据的函数"""
    if current_channel_articles and current_channel_name and current_date:
        chinese_name = channel_to_chinese[current_channel_name]
        current_time = datetime.now().strftime("%H%M%S")
        output_filename = f"241_{chinese_name}_{current_date}_{current_time}.json"
        output_path = os.path.join("data", output_filename)

        os.makedirs("data", exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(current_channel_articles, json_file, ensure_ascii=False, indent=4)
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
    """获取当前时间的“YYYY-MM-DD HH:MM:SS”格式（东八区）"""
    tz = timezone(timedelta(hours=8))
    current_time = datetime.now(tz)
    return current_time.strftime("%Y-%m-%d %H:%M:%S")


def crawl_single_path(path, articles, channel_name, date_str):
    """爬取单个路径并添加到文章列表的函数"""
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
            logging.debug(f"尝试访问URL: {url}")
            response = session.get(url, timeout=10)
            time.sleep(0.1)

            if response.status_code == 404:
                consecutive_invalid_count += 1
                logging.debug(f"URL 404: {url}")

                if consecutive_invalid_count % 10 == 0:
                    logging.info(f"连续 {consecutive_invalid_count} 次无效响应")

                if consecutive_invalid_count >= 150:
                    logging.info(f"连续150次无效响应（404），{path}路径爬取结束")
                    break
                continue

            if response.status_code != 200:
                consecutive_invalid_count += 1
                logging.warning(f"URL: {url} 返回状态码: {response.status_code}")

                if consecutive_invalid_count >= 150:
                    logging.info(f"连续150次无效响应（非200状态码），{path}路径爬取结束")
                    break
                continue

            content = response.text
            soup = BeautifulSoup(content, 'html.parser')

            title = soup.find('h1', class_='title-article') or soup.find('h1', class_='c-article-title')
            if not title:
                title = soup.find('h1')
                if not title:
                    consecutive_invalid_count += 1
                    logging.warning(f"在URL {url} 中未找到标题")

                    if consecutive_invalid_count >= 150:
                        logging.info(f"连续150次无效响应（无标题），{path}路径爬取结束")
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
                content_div = soup.find('div', class_='article-body') or soup.find('article') or soup.find('div', class_='content')
                if content_div:
                    paragraphs = [p.get_text(strip=True) for p in content_div.find_all('p') if p.get_text(strip=True)]

            if not paragraphs:
                consecutive_invalid_count += 1
                logging.warning(f"在URL {url} 中未找到正文内容")

                if consecutive_invalid_count >= 150:
                    logging.info(f"连续150次无效响应（无正文），{path}路径爬取结束")
                    break
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

            time_element = soup.find('time')
            if time_element:
                datetime_value = time_element.get('datetime')
                try:
                    publish_time = datetime.fromisoformat(datetime_value.replace('T', ' ').split('+')[0])
                    article["metadata"] = {
                        "publish_time": publish_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "authors": "",
                        "category": channel_to_chinese[channel_name]
                    }
                except ValueError:
                    article["metadata"] = {
                        "publish_time": datetime_value,
                        "authors": "",
                        "category": channel_to_chinese[channel_name]
                    }
            else:
                date_span = soup.find('span', class_='date')
                if date_span:
                    publish_time = date_span.get_text(strip=True)
                    article["metadata"] = {
                        "publish_time": publish_time,
                        "authors": "",
                        "category": channel_to_chinese[channel_name]
                    }
                else:
                    article["metadata"] = {
                        "publish_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "authors": "",
                        "category": channel_to_chinese[channel_name]
                    }

            author_element = soup.find('div', class_="article-author__item") or soup.find('span', class_='author')
            if author_element and "metadata" in article:
                article["metadata"]["authors"] = author_element.get_text(strip=True)

            article["crawling_time"] = crawling_time

            articles.append(article)
            articles_found += 1
            logging.info(f"成功爬取文章: {article['title']}")

        except requests.exceptions.Timeout:
            consecutive_invalid_count += 1
            logging.warning(f"请求超时: {url}")

            if consecutive_invalid_count >= 150:
                logging.info(f"连续150次无效响应（超时），{path}路径爬取结束")
                break

            continue

        except requests.exceptions.RequestException as e:
            consecutive_invalid_count += 1
            logging.error(f"网络错误：{e} - URL: {url}")

            if consecutive_invalid_count >= 150:
                logging.info(f"连续150次网络错误，{path}路径爬取结束")
                break

            time.sleep(5)

        except Exception as e:
            consecutive_invalid_count += 1
            logging.error(f"处理错误：{e} - URL: {url}")

            if consecutive_invalid_count >= 150:
                logging.info(f"连续150次处理错误，{path}路径爬取结束")
                break

            time.sleep(3)

        time.sleep(0.1)

        if consecutive_invalid_count >= 150:
            logging.info(f"连续150次无效响应，{path}路径爬取结束")
            break

    logging.info(f"{path}路径爬取完成 (日期: {date_str})，共找到 {articles_found} 篇文章")
    return articles_found


def crawl_channel_for_date(channel_name, date_str):
    """爬取整个频道在特定日期的内容（包括主频道和所有子频道）"""
    global current_channel_articles, current_channel_name, current_date

    current_channel_articles = []
    current_channel_name = channel_name
    current_date = date_str
    channel_info = channel_dict[channel_name]
    chinese_name = channel_to_chinese[channel_name]

    logging.info(f"=== 开始爬取 {chinese_name} 频道 (日期: {date_str}) ===")

    save_timer = threading.Timer(300, save_data_periodically)
    save_timer.daemon = True
    save_timer.start()

    crawl_single_path(channel_info["base_path"], current_channel_articles, channel_name, date_str)

    for sub_path in channel_info["sub_channels"]:
        crawl_single_path(sub_path, current_channel_articles, channel_name, date_str)

    save_timer.cancel()

    save_data_periodically()

    logging.info(f"{chinese_name}频道爬取完成 (日期: {date_str})，共爬取 {len(current_channel_articles)} 篇文章")
    logging.info(f"数据已保存到 data/241_{chinese_name}_{date_str}_*.json\n")

    current_channel_articles = []


def save_data_periodically():
    """定期保存数据的函数"""
    if current_channel_articles and current_channel_name and current_date:
        chinese_name = channel_to_chinese[current_channel_name]

        current_time = datetime.now().strftime("%H%M%S")
        output_filename = f"241_{chinese_name}_{current_date}_{current_time}.json"
        output_path = os.path.join("data", output_filename)

        os.makedirs("data", exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as json_file:
            json.dump(current_channel_articles, json_file, ensure_ascii=False, indent=4)
        logging.info(f"定期保存数据: {chinese_name}频道的数据已保存到 {output_path}")

        save_timer = threading.Timer(300, save_data_periodically)
        save_timer.daemon = True
        save_timer.start()


def generate_date_range():
    """生成从2025年1月1日到今天的日期列表"""
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


def main():
    os.makedirs("data", exist_ok=True)

    date_range = generate_date_range()
    logging.info(f"开始爬取日期范围: {len(date_range)} 天 (从2025-01-01到今天)")

    for channel_name in ["politics", "science", "economic", "sengo"]:
        chinese_name = channel_to_chinese[channel_name]

        for date_str in date_range:
            file_prefix = f"241_{chinese_name}_{date_str}"
            existing_files = [f for f in os.listdir("data") if f.startswith(file_prefix)]

            if existing_files:
                logging.info(f"文件已存在: data/{existing_files[0]}，跳过爬取")
                continue

            try:
                crawl_channel_for_date(channel_name, date_str)
            except Exception as e:
                logging.error(f"爬取 {chinese_name} 频道 (日期: {date_str}) 时发生错误: {e}")

            time.sleep(10)


if __name__ == "__main__":
    main()