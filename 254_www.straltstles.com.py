#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Straits Times 爬虫 - 本地新闻 / 国际新闻 / 经济
每日早晨6点运行，异常自动重启
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import shutil
import traceback
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

driver = None

# ========== Chrome 内核 ==========
def kernel_chrome():
    global driver
    if os.path.exists("254_chromedriver.exe"):
        print("✅ ChromeDriver已存在")
    else:
        driver_path = ChromeDriverManager().install()
        shutil.move(driver_path, "254_chromedriver.exe")
    target_path = os.path.join(os.getcwd(), "254_chromedriver.exe")
    print(f"✅ ChromeDriver已复制到: {target_path}")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")   # ✅ 无头模式
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    service = ChromeService(executable_path=target_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get("https://www.straitstimes.com/singapore")

def dismiss_overlays():
    """处理页面可能出现的弹窗"""
    try:
        close_btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[aria-label='Close']"))
        )
        close_btn.click()
        print("✅ 关闭弹窗")
    except:
        pass

# ========== 工具函数 ==========
def wait_until_next_6am():
    now = datetime.now()
    tomorrow = now + timedelta(days=1)
    target_time = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=6, minute=0, second=0)
    wait_seconds = (target_time - now).total_seconds()
    print(f"⏳ 休眠 {int(wait_seconds)} 秒，等待次日6点")
    time.sleep(wait_seconds)

def safe_publish_time(raw_time):
    """解析发布时间"""
    try:
        if not raw_time:
            return ""
        raw_time = raw_time.replace("Published", "").strip()
        return raw_time
    except:
        return raw_time

# ========== JSON 存储 ==========
def save_articles_grouped_by_date(articles, channel_name):
    if not articles:
        return
    today = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ✅ 创建 data 文件夹（如果不存在）
    save_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(save_dir, exist_ok=True)

    filename = f"254_{channel_name}_{today}.json"
    filepath = os.path.join(save_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"💾 已保存 {len(articles)} 篇文章到 {filepath}")

# ✅ 改名为 254_titles.txt
titles_file = "254_titles.txt"

def load_titles():
    if os.path.exists(titles_file):
        with open(titles_file, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

def save_title(title):
    with open(titles_file, "a", encoding="utf-8") as f:
        f.write(title.strip() + "\n")

# ========== 文章解析 ==========
def crawl_st_article(url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 标题
        title_elem = soup.find("h1")
        title_text = title_elem.get_text(strip=True) if title_elem else ""

        # 发布时间
        publish_elem = soup.find("p", class_="font-eyebrow-baseline-regular", string=lambda x: x and "Published" in x)
        publish_time = publish_elem.get_text(strip=True) if publish_elem else ""

        # 作者
        author_elem = soup.find("a", {"data-testid": "author-byline-default-byline-left"})
        authors = ""
        if author_elem:
            p = author_elem.find("p")
            if p:
                authors = p.get_text(strip=True)

        # 正文
        paragraphs = []
        for p in soup.find_all("p", {"data-testid": "article-paragraph-annotation-test-id"}):
            text = p.get_text(" ", strip=True)
            if text:
                paragraphs.append(text)
        content = "\n".join(paragraphs)

        # 分类
        category = "新闻"
        if "/singapore/" in url:
            category = "本地新闻"
        elif "/world/" in url:
            category = "国际新闻"
        elif "/business/" in url:
            category = "经济"

        article_data = {
            "title": title_text,
            "content": content,
            "sources": {
                "current_site": "The Straits Times",
                "current_siteurl": "www.straitstimes.com",
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
    except Exception as e:
        print(f"  × 爬取失败 {url}: {e}")
        return None, None, None

# ========== 翻页逻辑 ==========
def find_bottom_load_more(driver, wait_sec=5):
    try:
        btn = WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.XPATH, "//button//span[contains(text(), 'Load more')]/.."))
        )
        return btn
    except:
        return None

def crawl_channel(channel_url, channel_name):
    seen_links = set()
    titles_set = load_titles()
    all_articles = []
    fail_clicks = 0
    last_article_count = 0

    driver.get(channel_url)
    time.sleep(3)
    print(f"📺 开始爬取频道: {channel_name} {channel_url}")

    while True:
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # ✅ headline-lg-card-test-id 区块里的链接
        article_blocks = soup.find_all("div", {"data-testid": "headline-lg-card-test-id"})
        urls = []
        for block in article_blocks:
            parent_a = block.find_parent("a", {"data-testid": "custom-link"})
            if parent_a and parent_a.has_attr("href"):
                href = parent_a['href']
                if href.startswith("http"):
                    urls.append(href)
                else:
                    urls.append("https://www.straitstimes.com" + href)

        new_urls = [u for u in urls if u not in seen_links]
        print(f"发现 {len(new_urls)} 个新文章链接")

        articles_this_round = []
        for url in new_urls:
            seen_links.add(url)
            article_data, title_text, publish_time = crawl_st_article(url)
            if not article_data or not title_text:
                continue
            if title_text in titles_set:
                continue
            articles_this_round.append(article_data)
            save_title(title_text)
            titles_set.add(title_text)
            print(f"  ✅ 新文章: {title_text}")
            time.sleep(1)

        if articles_this_round:
            save_articles_grouped_by_date(articles_this_round, channel_name)
            all_articles.extend(articles_this_round)

        # 翻页
        load_btn = find_bottom_load_more(driver)
        if not load_btn:
            print("🛑 没有更多按钮，结束该频道")
            break
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", load_btn)
            print("点击 Load more")
            time.sleep(3)

            new_article_count = len(seen_links)
            if new_article_count <= last_article_count:
                fail_clicks += 1
                print(f"⚠️ 页面无新内容（连续失败 {fail_clicks} 次）")
            else:
                fail_clicks = 0
                last_article_count = new_article_count

            if fail_clicks >= 3:
                print("🛑 连续多次点击无效，结束该频道")
                break
        except Exception as e:
            fail_clicks += 1
            print(f"❌ 点击失败（连续失败 {fail_clicks} 次）: {e}")
            if fail_clicks >= 3:
                print("🛑 连续多次点击异常，结束该频道")
                break

    print(f"🎉 {channel_name} 完成，共获取 {len(all_articles)} 篇")

# ========== 主函数 ==========
def main():
    kernel_chrome()
    dismiss_overlays()
    channels = [
        ("https://www.straitstimes.com/singapore", "本地新闻"),
        ("https://www.straitstimes.com/world", "国际新闻"),
        ("https://www.straitstimes.com/business", "经济"),
    ]
    for url, name in channels:
        crawl_channel(url, name)
    driver.quit()

# ========== 自动调度 ==========
if __name__ == "__main__":
    while True:
        try:
            main()
            print("✅ 所有频道爬取完成，进入休眠")
            wait_until_next_6am()
        except KeyboardInterrupt:
            print("检测到手动关闭，程序退出。")
            break
        except Exception as e:
            print(f"爬虫异常中断，自动重启。异常信息: {e}")
            traceback.print_exc()
            print("3秒后自动重启...")
            time.sleep(3)
