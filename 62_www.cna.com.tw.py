#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import hashlib
from requests.exceptions import SSLError, RequestException

# 配置参数
START_DATE = datetime.date(2025, 1, 1)  # 起始日期
END_DATE = datetime.date.today()  # 结束日期
BASE_URL = "https://www.cna.com.tw/news/aipl/{date}{num:04d}.aspx"
VALID_CATEGORIES = {'政治', '國際', '兩岸', '產經', '證券', '科技'}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

TITLE_HASH_FILE = "crawled_title_hashes.txt"

grouped_articles = {}
processed_urls = 0
success_count = 0
error_count = 0
last_save_time = time.time()
SAVE_INTERVAL = 20 * 60
crawled_title_hashes = set()


def load_crawled_hashes():
    if not os.path.exists(TITLE_HASH_FILE):
        return set()
    try:
        with open(TITLE_HASH_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"加载去重文件失败: {str(e)}")
        return set()


def save_crawled_hash(title_hash):
    try:
        with open(TITLE_HASH_FILE, 'a', encoding='utf-8') as f:
            f.write(title_hash + '\n')
        return True
    except Exception as e:
        print(f"保存去重哈希失败: {str(e)}")
        return False


def generate_title_hash(title):
    return hashlib.md5(title.encode('utf-8')).hexdigest()


def generate_dates():
    current = END_DATE
    while current >= START_DATE:
        yield current.strftime("%Y%m%d")
        current -= datetime.timedelta(days=1)


def extract_category(soup):
    breadcrumb_div = soup.find('div', class_='breadcrumb')
    if not breadcrumb_div:
        return None
    category_tags = breadcrumb_div.find_all('a', class_='blue')
    if not category_tags:
        return None
    for tag in category_tags:
        category_name = tag.text.strip()
        if category_name in VALID_CATEGORIES:
            return category_name
    return None


def extract_publish_time(soup):
    update_div = soup.find('div', class_='updatetime')
    if update_div:
        first_span = update_div.find('span')
        if first_span:
            raw_time = first_span.text.strip()
            try:
                if ":" in raw_time:
                    dt = datetime.datetime.strptime(raw_time, "%Y/%m/%d %H:%M")
                else:
                    dt = datetime.datetime.strptime(raw_time, "%Y/%m/%d")
                    dt = dt.replace(hour=0, minute=0, second=0)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return raw_time
    return ""


def extract_authors(soup):
    names_divs = soup.find_all('div', class_='names')
    if not names_divs:
        return ""
    authors_list = []
    for names_div in names_divs:
        txt_spans = names_div.find_all('span', class_='txt')
        for span in txt_spans:
            text = span.text.strip()
            if text and not text.isspace():
                if text.startswith('|'):
                    text = text[1:].strip()
                authors_list.append(text)
    return " ".join(authors_list)


def save_grouped_articles(target_date=None):
    global grouped_articles
    if not grouped_articles:
        print("没有分组文章可保存")
        return 0
    save_time_str = datetime.datetime.now().strftime("%H%M%S")
    saved_files = 0
    for (category, article_date), articles_list in grouped_articles.items():
        if target_date and article_date != target_date:
            continue
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
    global processed_urls, success_count, error_count
    total_urls = len(list(generate_dates())) * 500
    progress_percent = (processed_urls / total_urls) * 100 if total_urls > 0 else 0
    grouped_count = sum(len(articles) for articles in grouped_articles.values())
    print("\n" + "=" * 60)
    print(f"爬取进度: {processed_urls}/{total_urls} ({progress_percent:.1f}%)")
    print(f"成功文章: {success_count} | 错误/跳过: {error_count}")
    print(f"分组文章: {grouped_count} 篇 ({len(grouped_articles)} 个分组)")
    print(f"当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


def check_and_save_grouped():
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time >= SAVE_INTERVAL:
        print("\n" + "=" * 60)
        print(f"达到20分钟保存间隔，正在保存分组文章...")
        saved_files = save_grouped_articles()
        print(f"已保存 {saved_files} 个分组文件")
        print("=" * 60 + "\n")
        last_save_time = current_time
        return True
    return False


def crawl_articles():
    global grouped_articles, processed_urls, success_count, error_count, last_save_time
    global crawled_title_hashes
    try:
        dates = list(generate_dates())
        total_days = len(dates)
        total_urls = total_days * 500
        print(f"爬取日期范围: {START_DATE} 到 {END_DATE}")
        print(f"总天数: {total_days}, 总URL数: {total_urls}")
        print(f"已加载去重记录: {len(crawled_title_hashes)} 条")
        for day_idx, date_str in enumerate(dates):
            print(f"\n{'=' * 60}")
            print(f"处理日期: {date_str} ({day_idx + 1}/{total_days})")
            date_count = 0
            for article_num in range(1, 501):
                processed_urls += 1
                if processed_urls % 20 == 0:
                    print_progress()
                check_and_save_grouped()
                url = BASE_URL.format(date=date_str, num=article_num)
                print(f"正在爬取: {url}")
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    response = requests.get(url, headers=HEADERS, timeout=15)
                    if response.status_code == 404:
                        print(f"  × 页面不存在 (404) - 跳过")
                        error_count += 1
                        continue
                    if 'text/html' not in response.headers.get('Content-Type', ''):
                        print(f"  × 非HTML内容 - 跳过")
                        error_count += 1
                        continue
                    soup = BeautifulSoup(response.text, 'html.parser')
                    category = extract_category(soup)
                    if not category:
                        print(f"  × 未找到有效分类 - 跳过")
                        error_count += 1
                        continue
                    title_tag = soup.find('h1')
                    if not title_tag:
                        print(f"  × 未找到标题 - 跳过")
                        error_count += 1
                        continue
                    title_text_content = title_tag.get_text(strip=True)
                    title_hash = generate_title_hash(title_text_content)
                    if title_hash in crawled_title_hashes:
                        print(f"  × 重复文章: {title_text_content} - 跳过")
                        error_count += 1
                        continue
                    content_div = soup.find('div', class_='paragraph')
                    if not content_div:
                        print(f"  × 未找到内容 - 跳过")
                        error_count += 1
                        continue
                    content_text = "\n".join(p.get_text(strip=False) for p in content_div.find_all('p')).strip()
                    publish_time = extract_publish_time(soup)
                    authors = extract_authors(soup)
                    article_data = {
                        "title": title_text_content,
                        "content": content_text,
                        "sources": {
                            "current_site": "台湾中央社CAN",
                            "current_siteurl": "www.cna.com.tw",
                            "origin_url": url
                        },
                        "metadata": {
                            "publish_time": publish_time,
                            "authors": authors,
                            "category": category
                        },
                        "crawling_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    group_key = (category, date_str)
                    grouped_articles.setdefault(group_key, []).append(article_data)
                    crawled_title_hashes.add(title_hash)
                    save_crawled_hash(title_hash)
                    date_count += 1
                    success_count += 1
                    print(f"  ✓ 找到文章: {category} - {title_text_content}")
                except (SSLError, RequestException) as req_error:
                    print(f"  × 请求错误: {req_error} - 跳过")
                    error_count += 1
                    continue
                except Exception as e:
                    print(f"  × 处理错误: {str(e)} - 跳过")
                    error_count += 1
                    continue
            print(f"日期 {date_str} 完成: 找到 {date_count} 篇文章")
            save_grouped_articles(target_date=date_str)
    except KeyboardInterrupt:
        print("\n手动中断，保存进度...")
        save_grouped_articles()
        print_progress()
        sys.exit(0)


# 新增：守护调度逻辑
def run_once():
    global crawled_title_hashes, grouped_articles, processed_urls, success_count, error_count
    grouped_articles = {}
    processed_urls = 0
    success_count = 0
    error_count = 0
    crawled_title_hashes = load_crawled_hashes()
    print(f"\n===== 启动爬虫 {datetime.datetime.now()} =====")
    crawl_articles()
    print(f"===== 本轮完成 {datetime.datetime.now()} =====")


def wait_until(hour, minute):
    while True:
        now = datetime.datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        print(f"等待 {wait_seconds/60:.1f} 分钟直到 {target}")
        time.sleep(wait_seconds)
        return


if __name__ == "__main__":
    while True:
        try:
            run_once()
            wait_until(6, 0)  # 等到第二天早上6点
        except Exception as e:
            print(f"[错误] 程序异常中断: {e}")
            print("5 秒后自动重启...")
            time.sleep(5)
            continue
