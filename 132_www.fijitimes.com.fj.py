#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fiji Times 爬虫 - 每轮新发现链接批量爬取并按日期分组合并存储
"""
import requests
from bs4 import BeautifulSoup, Tag
import json
import os
from datetime import datetime, timedelta
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from time import sleep
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
import logging
import shutil

TXT_FILE = '132_fijitimes.txt'
JSON_DIR = 'data'

# 抑制警告和错误输出
warnings.filterwarnings("ignore")
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

MONTH_MAP = {
    'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
    'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12',
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

def load_titles():
    if not os.path.exists(TXT_FILE):
        return set()
    with open(TXT_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def save_title(title):
    with open(TXT_FILE, 'a', encoding='utf-8') as f:
        f.write(title + '\n')

def safe_publish_time(publish_time):
    today = datetime.now()
    m = re.search(r'Published: (\d+) hours? ago', publish_time)
    if m:
        return today.strftime('%Y-%m-%d')
    m = re.search(r'Published: (\d+) days? ago', publish_time)
    if m:
        days = int(m.group(1))
        dt = today - timedelta(days=days)
        return dt.strftime('%Y-%m-%d')
    m = re.search(r'Published: (\d+) weeks? ago', publish_time)
    if m:
        weeks = int(m.group(1))
        dt = today - timedelta(days=weeks*7)
        return dt.strftime('%Y-%m-%d')
    m = re.search(r'Published: ([A-Za-z]+) (\d{1,2}), (\d{4})', publish_time)
    if m:
        month = MONTH_MAP.get(m.group(1), '01')
        day = m.group(2).zfill(2)
        year = m.group(3)
        return f'{year}-{month}-{day}'
    pt = ''.join(filter(str.isdigit, publish_time))
    if len(pt) == 8:
        return f'{pt[:4]}-{pt[4:6]}-{pt[6:]}'
    return 'unknown'

def safe_filename(s):
    return re.sub(r'[^\w\u4e00-\u9fa5]', '', s)

def cleanup_chrome_temp():
    """清理Chrome临时目录"""
    temp_dir = './chrome_temp'
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print("🧹 已清理Chrome临时目录")
        except Exception as e:
            print(f"⚠️ 清理临时目录失败: {e}")

def save_articles_grouped_by_date(articles, channel_name):
    """将同一天的文章合并存为一个json文件，所有文件保存在data/下"""
    from collections import defaultdict
    grouped = defaultdict(list)
    for art in articles:
        date_str = art['metadata']['publish_time']
        grouped[date_str].append(art)
    now_str = datetime.now().strftime('%H%M%S')
    cat = safe_filename(channel_name)
    for date_str, arts in grouped.items():
        pt = date_str.replace('-', '')
        filename = f'132_{cat}_{pt}_{now_str}.json'
        filepath = os.path.join(JSON_DIR, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(arts, f, ensure_ascii=False, indent=2)
        print(f'💾 已保存{len(arts)}篇文章到 {filepath}')

def crawl_article(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            title_elem = soup.find('h1', class_='fijitimes_title wp-block-post-title has-x-large-font-size')
            if not (isinstance(title_elem, Tag)):
                print(f"  × 未找到标题元素")
                return None, None, None
            title_text = title_elem.get_text(strip=True)
            content_elem = soup.find('div', class_='entry-content post_content wp-block-post-content is-layout-flow wp-block-post-content-is-layout-flow')
            if not (isinstance(content_elem, Tag)):
                print(f"  × 未找到内容元素")
                return None, None, None
            content = '\n'.join([p.get_text(strip=True) for p in content_elem.find_all('p') if isinstance(p, Tag) and p.get_text(strip=True)])
            info_elem = soup.find('div', class_='fijitimes_post__info')
            publish_time, authors = '', ''
            if isinstance(info_elem, Tag):
                spans = [span for span in info_elem.find_all('span') if isinstance(span, Tag)]
                if len(spans) >= 2:
                    publish_time = spans[1].get_text(strip=True)
                if len(spans) >= 4:
                    authors = spans[3].get_text(strip=True)
            if authors.lower().startswith('by '):
                authors = authors[3:].strip()
            category = "经济"
            if '/local-news/' in url:
                category = "当地新闻"
            elif '/world/' in url:
                category = "国际新闻"
            elif '/business/' in url:
                category = "经济"
            article_data = {
                "title": title_text,
                "content": content,
                "sources": {
                    "current_site": "每日时报",
                    "current_siteurl": "www.fijitimes.com.fj",
                    "origin_url": url
                },
                "metadata": {
                    "publish_time": safe_publish_time(publish_time),
                    "authors": authors,
                    "category": category
                },
                "crawlingtime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            return article_data, title_text, publish_time
        except requests.exceptions.SSLError as e:
            print(f"  × SSL错误，重试第{attempt+1}次: {url}")
            sleep(2)
            continue
        except Exception as e:
            print(f"  × 爬取文章失败 {url}: {str(e)}")
            return None, None, None
    return None, None, None

def crawl_channel(channel_url):
    print(f"\n🌐 启动无头浏览器加载频道: {channel_url}")
    
    # 1. 自动清理chrome_temp目录，并用绝对路径
    chrome_temp_dir = os.path.abspath('./chrome_temp')
    if os.path.exists(chrome_temp_dir):
        try:
            shutil.rmtree(chrome_temp_dir)
        except Exception as e:
            print(f"无法删除旧的chrome_temp目录: {e}")
    os.makedirs(chrome_temp_dir, exist_ok=True)
    
    # 2. 配置Chrome选项为无头模式，添加反检测功能
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')  # 使用新版无头模式
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--log-level=3')  # 只显示致命错误
    chrome_options.add_argument('--silent')  # 静默模式
    chrome_options.add_argument('--disable-logging')  # 禁用日志
    chrome_options.add_argument('--disable-web-security')  # 禁用Web安全，可能解决SSL问题
    chrome_options.add_argument('--ignore-ssl-errors')  # 忽略SSL错误
    chrome_options.add_argument('--ignore-certificate-errors')  # 忽略证书错误
    chrome_options.add_argument(f'--user-data-dir={chrome_temp_dir}')  # 使用绝对路径
    chrome_options.add_argument('--no-first-run')  # 跳过首次运行设置
    chrome_options.add_argument('--no-default-browser-check')  # 不检查默认浏览器
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(log_output=os.devnull)  # 将Chrome日志输出到空设备
    driver = webdriver.Chrome(options=chrome_options, service=service)
    
    # 执行JavaScript来隐藏自动化特征
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
    
    try:
        driver.get(channel_url)
    except Exception as e:
        print(f"⚠️ driver.get({channel_url}) 失败: {e}")
        try:
            driver.quit()
        except:
            pass
        return
    # 等待页面完全加载
    sleep(3)
    
    max_clicks = 100
    click_count = 0
    seen_links = set()
    titles_set = load_titles()
    print(f"已加载 {len(titles_set)} 个历史标题用于去重")
    # 频道名直接根据入口URL判断
    if '/local-news/' in channel_url:
        channel_name = '当地新闻'
    elif '/world/' in channel_url:
        channel_name = '国际新闻'
    elif '/business/' in channel_url:
        channel_name = '经济'
    else:
        channel_name = '未知频道'
    
    # 用于中断保存的变量
    all_articles = []
    no_loadmore_count = 0  # 连续未检测到Load more按钮的计数器
    no_loadmore_threshold = 15
    try:
        while click_count < max_clicks:
            print(f"\n--- 第 {click_count + 1} 次加载 ---")
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a', class_='ps-no-underline ps-leading-tight ps-text-blockBlack')
            urls = []
            for link in links:
                if isinstance(link, Tag):
                    href = link.get('href')
                    if isinstance(href, str) and href.startswith('http'):
                        urls.append(href)
            urls = list(set(urls))
            new_urls = [u for u in urls if u not in seen_links]
            print(f'本轮新发现 {len(new_urls)} 个链接')
            # 批量爬取本轮所有新链接
            articles_this_round = []
            for url in new_urls:
                seen_links.add(url)
                article_data, title_text, publish_time = crawl_article(url)
                if not article_data or not title_text:
                    continue
                if title_text in titles_set:
                    print(f'  × 已爬取过: {title_text}')
                    continue
                articles_this_round.append(article_data)
                save_title(title_text)
                titles_set.add(title_text)
                print(f'  ✅ 新文章: {title_text}')
                sleep(1.5)
            # 本轮所有新文章按日期分组存储
            if not os.path.exists(JSON_DIR):
                os.makedirs(JSON_DIR)
            if articles_this_round:
                save_articles_grouped_by_date(articles_this_round, channel_name)
                all_articles.extend(articles_this_round)
            # 连续5次未检测到Load more才break
            try:
                load_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'ps-cursor-pointer') and contains(., 'Load more')]"))
                )
                no_loadmore_count = 0  # 检测到按钮，重置计数
            except:
                no_loadmore_count += 1
                print(f"未找到'Load more'按钮，累计{no_loadmore_count}次")
                if no_loadmore_count >= no_loadmore_threshold:
                    print(f"连续{no_loadmore_threshold}次未检测到'Load more'按钮，频道可能已加载全部内容")
                    break
                else:
                    sleep(2)
                    continue
            # 点击按钮前先滚动到可见区域，失败重试3次
            click_success = False
            for click_attempt in range(3):
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", load_btn)
                    sleep(0.5)
                    load_btn.click()
                    click_count += 1
                    print(f"点击'Load more'按钮 ({click_count}/{max_clicks})")
                    sleep(2)
                    try:
                        driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                        print("已滚动到页面底部")
                        sleep(1)
                    except:
                        print("滚动失败，继续处理")
                    click_success = True
                    break
                except Exception as e:
                    print(f"点击按钮失败（第{click_attempt+1}次）: {str(e)}")
                    sleep(1)
            if not click_success:
                print(f"连续3次点击'Load more'按钮失败，跳出循环")
                break
    except KeyboardInterrupt:
        print("\n⚠️ 检测到用户中断（Ctrl+C），正在保存已爬取内容...")
    finally:
        # 无论如何都保存一次
        if all_articles:
            print(f"\n⚠️ 正在保存已爬取的{len(all_articles)}篇文章...")
            save_articles_grouped_by_date(all_articles, channel_name)
        else:
            print("\n⚠️ 没有需要额外保存的文章。")
        try:
            driver.quit()
            print("🔚 浏览器已关闭")
        except:
            pass

def main():
    print("🎯 Fiji Times 频道逐步爬虫启动")
    channels = [
        "https://www.fijitimes.com.fj/category/news/business/",
        "https://www.fijitimes.com.fj/category/news/local-news/",
        "https://www.fijitimes.com.fj/category/news/world/"
    ]
    try:
        for channel_url in channels:
            crawl_channel(channel_url)
        print("\n🎯 所有频道爬取完成！")
    finally:
        cleanup_chrome_temp()

if __name__ == '__main__':
    main() 