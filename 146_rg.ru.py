#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RG.ru 爬虫 - 带异常中断重启功能
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
import warnings
import logging
import shutil
import subprocess
import platform
import random
import traceback
import time
import glob

# 尝试导入webdriver_manager，如果失败则使用备用方案
try:
    from webdriver_manager.chrome import ChromeDriverManager

    WEBDRIVER_MANAGER_AVAILABLE = True
except ImportError:
    WEBDRIVER_MANAGER_AVAILABLE = False
    print("⚠️ webdriver-manager未安装，将使用备用方案")

TXT_FILE = 'rg_ru_titles.txt'
JSON_DIR = 'data'

# 全局变量，记录上一个有效日期
last_valid_date = None

warnings.filterwarnings("ignore")
logging.getLogger("selenium").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

MONTH_MAP = {
    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06',
    'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
    'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04', 'июн': '06', 'июл': '07', 'авг': '08', 'сен': '09', 'окт': '10',
    'ноя': '11', 'дек': '12'
}

# 异常计数器
exception_count = 0
MAX_EXCEPTION_RETRY = 5
EXCEPTION_COOLDOWN = 60  # 异常后冷却时间（秒）


def find_chromedriver():
    """查找系统中已安装的ChromeDriver"""
    possible_paths = []

    # Windows路径
    if platform.system() == "Windows":
        possible_paths.extend([
            "chromedriver.exe",
            "C:\\chromedriver\\chromedriver.exe",
            "C:\\Program Files\\chromedriver\\chromedriver.exe",
            "C:\\Program Files (x86)\\chromedriver\\chromedriver.exe",
            os.path.join(os.getcwd(), "chromedriver.exe"),
            os.path.join(os.path.dirname(__file__), "chromedriver.exe")
        ])
    else:
        # Linux/Mac路径
        possible_paths.extend([
            "chromedriver",
            "/usr/local/bin/chromedriver",
            "/usr/bin/chromedriver",
            "/opt/chromedriver/chromedriver",
            os.path.join(os.getcwd(), "chromedriver"),
            os.path.join(os.path.dirname(__file__), "chromedriver")
        ])

    # 检查PATH环境变量
    try:
        result = subprocess.run(['chromedriver', '--version'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            return "chromedriver"  # 在PATH中找到
    except:
        pass

    # 检查可能的路径
    for path in possible_paths:
        if os.path.exists(path):
            return path

    return None


def download_chromedriver_manual():
    """手动下载ChromeDriver的备用方案"""
    import urllib.request
    import zipfile

    # 根据系统确定下载URL
    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "windows":
        if "64" in machine:
            url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
            # 获取最新版本号
            try:
                with urllib.request.urlopen(url) as response:
                    version = response.read().decode('utf-8').strip()
                download_url = f"https://chromedriver.storage.googleapis.com/{version}/chromedriver_win32.zip"
            except:
                # 使用固定版本
                download_url = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_win32.zip"
        else:
            download_url = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_win32.zip"
    elif system == "linux":
        download_url = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip"
    elif system == "darwin":  # macOS
        if "arm" in machine:
            download_url = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_mac_arm64.zip"
        else:
            download_url = "https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_mac64.zip"
    else:
        return None

    try:
        print(f"🔧 正在手动下载ChromeDriver: {download_url}")

        # 创建下载目录
        download_dir = os.path.join(os.getcwd(), "chromedriver_download")
        os.makedirs(download_dir, exist_ok=True)

        # 下载文件
        zip_path = os.path.join(download_dir, "chromedriver.zip")
        urllib.request.urlretrieve(download_url, zip_path)

        # 解压文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_dir)

        # 找到chromedriver可执行文件
        for root, dirs, files in os.walk(download_dir):
            for file in files:
                if file.startswith('chromedriver'):
                    chromedriver_path = os.path.join(root, file)
                    # 设置执行权限（Linux/Mac）
                    if system != "windows":
                        os.chmod(chromedriver_path, 0o755)
                    return chromedriver_path

        return None
    except Exception as e:
        print(f"❌ 手动下载ChromeDriver失败: {e}")
        return None


def get_local_chrome_version():
    """自动检测本地Chrome主版本号（仅支持Windows）"""
    import winreg
    chrome_reg_paths = [
        r"SOFTWARE\Google\Chrome\BLBeacon",
        r"SOFTWARE\WOW6432Node\Google\Chrome\BLBeacon"
    ]
    for reg_path in chrome_reg_paths:
        try:
            reg_key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, reg_path)
            version, _ = winreg.QueryValueEx(reg_key, "version")
            winreg.CloseKey(reg_key)
            return version
        except:
            pass
        try:
            reg_key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
            version, _ = winreg.QueryValueEx(reg_key, "version")
            winreg.CloseKey(reg_key)
            return version
        except:
            pass
    # 备选：尝试通过chrome.exe --version
    try:
        result = subprocess.run([
            r"C:\Program Files\Google\Chrome\Application\chrome.exe", "--version"
        ], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            m = re.search(r"(\d+\.\d+\.\d+\.\d+)", result.stdout)
            if m:
                return m.group(1)
    except:
        pass
    return None


def get_chromedriver_path():
    """获取ChromeDriver路径，优先自动检测本地Chrome主版本号并下载对应版本"""
    # 方案1: 查找系统中已安装的ChromeDriver
    chromedriver_path = find_chromedriver()
    if chromedriver_path:
        print(f"✅ 找到已安装的ChromeDriver: {chromedriver_path}")
        return chromedriver_path

    # 方案2: 自动检测本地Chrome主版本号并下载对应版本
    if WEBDRIVER_MANAGER_AVAILABLE:
        chrome_version = get_local_chrome_version()
        if chrome_version:
            main_version = chrome_version.split('.')[0]
            print(f"🔍 检测到本地Chrome主版本号: {main_version}")
            sources = [
                ("默认源", None),
                ("阿里云", 'https://registry.npmmirror.com/-/binary/chromedriver'),
                ("清华源", 'https://mirrors.tuna.tsinghua.edu.cn/chromedriver/')
            ]
            for name, mirror in sources:
                try:
                    print(f"🔧 webdriver-manager尝试下载ChromeDriver（{name}，版本{main_version}）...")
                    if mirror is None:
                        os.environ.pop('WDM_MIRROR', None)
                    else:
                        os.environ['WDM_MIRROR'] = mirror
                    os.environ['WDM_CACHE_PATH'] = os.path.abspath('./chromedriver_cache')
                    os.environ['WDM_LOCAL'] = '0'
                    os.environ['WDM_SSL_VERIFY'] = 'false'
                    chromedriver_path = ChromeDriverManager(driver_version=main_version).install()
                    print(f"✅ webdriver-manager（{name}）下载成功: {chromedriver_path}")
                    return chromedriver_path
                except Exception as e:
                    print(f"❌ webdriver-manager（{name}）下载失败: {e}")
        else:
            print("⚠️ 未能自动检测到本地Chrome版本，尝试通用方式下载...")
            # 继续后续逻辑
        # 兼容原有逻辑：尝试不指定版本的三源
        sources = [
            ("默认源", None),
            ("阿里云", 'https://registry.npmmirror.com/-/binary/chromedriver'),
            ("清华源", 'https://mirrors.tuna.tsinghua.edu.cn/chromedriver/')
        ]
        for name, mirror in sources:
            try:
                print(f"🔧 尝试使用webdriver-manager下载ChromeDriver（{name}）...")
                if mirror is None:
                    os.environ.pop('WDM_MIRROR', None)
                else:
                    os.environ['WDM_MIRROR'] = mirror
                os.environ['WDM_CACHE_PATH'] = os.path.abspath('./chromedriver_cache')
                os.environ['WDM_LOCAL'] = '0'
                os.environ['WDM_SSL_VERIFY'] = 'false'
                chromedriver_path = ChromeDriverManager().install()
                print(f"✅ webdriver-manager（{name}）下载成功: {chromedriver_path}")
                return chromedriver_path
            except Exception as e:
                print(f"❌ webdriver-manager（{name}）下载失败: {e}")

    # 方案3: 手动下载
    print("🔧 尝试手动下载ChromeDriver...")
    chromedriver_path = download_chromedriver_manual()
    if chromedriver_path:
        print(f"✅ 手动下载成功: {chromedriver_path}")
        return chromedriver_path

    # 方案4: 提示用户手动安装
    print("❌ 无法自动获取ChromeDriver（已尝试自动检测版本、默认源、阿里云、清华源和手动下载）")
    print("请手动下载ChromeDriver并放置在以下位置之一:")
    if platform.system() == "Windows":
        print("- 当前目录下的chromedriver.exe")
        print("- C:\\chromedriver\\chromedriver.exe")
        print("- 添加到系统PATH环境变量")
    else:
        print("- 当前目录下的chromedriver")
        print("- /usr/local/bin/chromedriver")
        print("- 添加到系统PATH环境变量")
    return None


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

    if not publish_time:
        return 'unknown'

    # 匹配俄语时间格式 "5 июля 2025"
    m = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{4})', publish_time)
    if m:
        day = m.group(1).zfill(2)
        month_ru = m.group(2).lower()
        month = MONTH_MAP.get(month_ru, '01')
        year = m.group(3)
        return f'{year}-{month}-{day}'

    # 匹配 "сегодня, 15:30" 格式
    m = re.search(r'сегодня, (\d{1,2}):(\d{2})', publish_time)
    if m:
        return today.strftime('%Y-%m-%d')

    # 匹配 "вчера, 15:30" 格式
    m = re.search(r'вчера, (\d{1,2}):(\d{2})', publish_time)
    if m:
        dt = today - timedelta(days=1)
        return dt.strftime('%Y-%m-%d')

    # 匹配 "2025-07-05" 格式
    m = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', publish_time)
    if m:
        year = m.group(1)
        month = m.group(2).zfill(2)
        day = m.group(3).zfill(2)
        return f'{year}-{month}-{day}'

    # 新增：匹配07.07.2025 16:00格式
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})', publish_time)
    if m:
        day = m.group(1)
        month = m.group(2)
        year = m.group(3)
        hour = m.group(4)
        minute = m.group(5)
        return f'{year}-{month}-{day} {hour}:{minute}:00'

    # 新增：匹配07.07.2025格式（只有日期）
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', publish_time)
    if m:
        day = m.group(1)
        month = m.group(2)
        year = m.group(3)
        return f'{year}-{month}-{day}'

    return 'unknown'


def safe_filename(s):
    return re.sub(r'[^\w\u4e00-\u9fa5]', '', s)


def get_latest_date_from_titles():
    """从标题文件中提取最新日期"""
    if not os.path.exists(TXT_FILE):
        return None

    latest_date = None
    with open(TXT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            # 尝试从标题行中提取日期信息
            match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', line)
            if match:
                day = match.group(1)
                month = match.group(2)
                year = match.group(3)
                try:
                    date_obj = datetime(int(year), int(month), int(day))
                    if latest_date is None or date_obj > latest_date:
                        latest_date = date_obj
                except:
                    continue

    return latest_date


def get_previous_day_date():
    """获取前一天的日期（格式为YYMMDD）"""
    # 尝试从标题文件中获取最新日期
    latest_date = get_latest_date_from_titles()
    if latest_date:
        # 减去一天
        prev_day = latest_date - timedelta(days=1)
        # 格式化为YYMMDD
        return prev_day.strftime('%y%m%d')

    # 如果没有找到有效日期，使用当前日期前一天
    prev_day = datetime.now() - timedelta(days=1)
    return prev_day.strftime('%y%m%d')


def save_articles_grouped_by_date(articles, channel_name):
    from collections import defaultdict
    grouped = defaultdict(list)
    for art in articles:
        # 只取年月日部分作为分组依据
        date_str = art['metadata']['publish_time'][:10] if art['metadata']['publish_time'] else 'unknown'
        grouped[date_str].append(art)

    now_str = datetime.now().strftime('%H%M%S')
    cat = safe_filename(channel_name)

    for date_str, arts in grouped.items():
        # 处理时间字符串，确保文件名安全
        if ' ' in date_str:  # 如果包含时间部分，只取日期部分
            date_str = date_str.split(' ')[0]

        # 使用原始日期字符串
        original_date_str = date_str

        # 将日期字符串转换为标准格式
        pt = date_str.replace('-', '')
        # 将4位年份缩短为2位年份
        if len(pt) == 8 and pt.isdigit():  # 确保是8位数字格式
            pt = pt[2:]  # 去掉前两位年份，只保留后两位

        # 如果是unknown，使用前一天日期
        if pt == 'unknown':
            prev_day = get_previous_day_date()
            print(f"⚠️ 使用前一天日期替代unknown: {prev_day}")
            pt = prev_day

        # 如果转换后日期无效（不是6位数字），使用前一天日期
        if len(pt) != 6 or not pt.isdigit():
            prev_day = get_previous_day_date()
            print(f"⚠️ 无效日期 '{pt}'，使用前一天日期替代: {prev_day}")
            pt = prev_day

        filename = f'146_{cat}_{pt}_{now_str}.json'
        filepath = os.path.join(JSON_DIR, filename)

        # 修正：全部覆盖category字段为cat
        for art in arts:
            if 'metadata' in art:
                art['metadata']['category'] = cat

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(arts, f, ensure_ascii=False, indent=2)
            print(f'💾 已保存{len(arts)}篇文章到 {filepath} (原始日期: {original_date_str})')
        except Exception as e:
            print(f'❌ 保存文件失败: {filepath}, 错误: {str(e)}')
            # 尝试使用备用文件名
            backup_filename = f'146_{cat}_backup_{now_str}.json'
            backup_filepath = os.path.join(JSON_DIR, backup_filename)
            try:
                with open(backup_filepath, 'w', encoding='utf-8') as f:
                    json.dump(arts, f, ensure_ascii=False, indent=2)
                print(f'💾 已保存{len(arts)}篇文章到备用文件 {backup_filepath}')
            except Exception as e2:
                print(f'❌ 备用文件保存也失败: {str(e2)}')


def crawl_article(url, session=None):
    if session is None:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })

    for attempt in range(3):
        try:
            # 增加随机延迟，避免被反爬虫检测
            sleep(1 + attempt * 0.5)

            # 使用更长的超时时间
            response = session.get(url, timeout=30, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 查找标题 - 根据提供的HTML结构
            title_elem = soup.find('h1', class_='PageArticleCommonTitle_title__fUDQW')
            if not isinstance(title_elem, Tag):
                print(f"  × 未找到标题元素，重试第{attempt + 1}次")
                if attempt < 2:  # 如果不是最后一次尝试，继续重试
                    continue
                else:
                    print(f"  × 连续3次未找到标题元素，跳过: {url}")
                    return None, None, None

            title_text = title_elem.get_text(strip=True)

            # 优先查找作者 - PageArticleContent_authors__eRDtn 下所有 <a> 标签文本
            authors = ''
            author_elem = soup.find(class_='PageArticleContent_authors__eRDtn')
            if isinstance(author_elem, Tag):
                a_tags = author_elem.find_all('a')
                authors = ' '.join(a.get_text(strip=True) for a in a_tags if a.get_text(strip=True))
            else:
                # 其次查找 PageArticle_authors__cFIb5 下所有 <a> 标签文本
                author_elem = soup.find(class_='PageArticle_authors__cFIb5')
                if isinstance(author_elem, Tag):
                    a_tags = author_elem.find_all('a')
                    authors = ' '.join(a.get_text(strip=True) for a in a_tags if a.get_text(strip=True))
                else:
                    # 备选作者查找逻辑
                    author_elem = (
                            soup.find('span', class_='author') or
                            soup.find('div', class_='author') or
                            soup.find('span', class_='byline') or
                            soup.find('div', class_='byline')
                    )
                    if isinstance(author_elem, Tag):
                        authors = author_elem.get_text(strip=True)

            # 查找文章内容 - 根据提供的HTML结构
            content_elem = soup.find('div', class_='PageContentCommonStyling_text__CKOzO')
            if not isinstance(content_elem, Tag):
                print(f"  × 未找到内容元素，重试第{attempt + 1}次")
                if attempt < 2:  # 如果不是最后一次尝试，继续重试
                    continue
                else:
                    print(f"  × 连续3次未找到内容元素，跳过: {url}")
                    return None, None, None

            # 提取段落内容 - 查找所有p标签，特别是带有_msttexthash属性的
            paragraphs = []
            # 查找所有p标签
            all_p_tags = content_elem.find_all('p')
            for p in all_p_tags:
                if isinstance(p, Tag):
                    # 检查是否有_msttexthash属性
                    class_attr = p.get('class')
                    has_msttexthash = class_attr and any('_msttexthash' in str(cls) for cls in class_attr)
                    text = p.get_text(strip=True)
                    if text and len(text) > 10:  # 过滤太短的段落
                        paragraphs.append(text)
                        if has_msttexthash:
                            print(f"    ✓ 找到带_msttexthash属性的段落: {text[:50]}...")

            # 如果上面的方法没有找到内容，尝试其他方法
            if not paragraphs:
                # 查找所有p标签，不管是否有_msttexthash属性
                all_p_tags = content_elem.find_all('p')
                for p in all_p_tags:
                    if isinstance(p, Tag):
                        text = p.get_text(strip=True)
                        if text and len(text) > 10:
                            paragraphs.append(text)

            # 如果仍然没有找到内容，重试
            if not paragraphs:
                print(f"  × 未找到文章内容，重试第{attempt + 1}次")
                if attempt < 2:  # 如果不是最后一次尝试，继续重试
                    continue
                else:
                    print(f"  × 连续3次未找到文章内容，跳过: {url}")
                    return None, None, None

            content = ''.join(paragraphs)

            # 查找发布时间（只用ContentMetaDefault_date__wS0te类）
            publish_time = ''
            time_elem = soup.find(class_='ContentMetaDefault_date__wS0te')
            if isinstance(time_elem, Tag):
                publish_time = time_elem.get_text(strip=True)

            # 确定分类
            category = "新闻"
            if '/tema/gos' in url:
                category = "政府"
            elif '/tema/ekonomika' in url:
                category = "经济"
            elif '/tema/mir' in url:
                category = "国际"
            elif '/tema/obshestvo' in url:
                category = "社会"
            elif '/tema/bezopasnost' in url:
                category = "安全"

            article_data = {
                "title": title_text,
                "content": content,
                "sources": {
                    "current_site": "俄罗斯报",
                    "current_siteurl": "rg.ru",
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
            print(f"  × SSL错误，重试第{attempt + 1}次: {url}")
            sleep(3 + attempt * 2)  # 增加延迟时间
            continue
        except requests.exceptions.ConnectionError as e:
            print(f"  × 连接错误，重试第{attempt + 1}次: {url}")
            sleep(3 + attempt * 2)
            continue
        except requests.exceptions.Timeout as e:
            print(f"  × 超时错误，重试第{attempt + 1}次: {url}")
            sleep(3 + attempt * 2)
            continue
        except Exception as e:
            print(f"  × 爬取文章失败 {url}: {str(e)}")
            sleep(2)
            continue

    return None, None, None


def extract_article_links_from_page_rubric(soup, base_url):
    """从PageRubricSeo_text__9XF1J元素中提取文章链接"""
    urls = []

    # 查找PageRubricSeo_text__9XF1J元素
    page_rubric_elements = soup.find_all('div', class_='PageRubricSeo_text__9XF1J')

    for element in page_rubric_elements:
        # 在PageRubricSeo_text__9XF1J元素内查找文章链接
        links = element.find_all('a', href=True)
        for link in links:
            if isinstance(link, Tag):
                href = link.get('href')
                if isinstance(href, str):
                    # 处理相对链接
                    if href.startswith('/'):
                        full_url = 'https://rg.ru' + href
                    elif href.startswith('https://rg.ru/'):
                        full_url = href
                    else:
                        continue

                    # 检查是否是文章链接
                    if '/202' in full_url and '.html' in full_url:
                        urls.append(full_url)

    # 同时查找ItemOfListStandard_title__Ajjlf类的链接
    title_links = soup.find_all('span', class_='ItemOfListStandard_title__Ajjlf')
    for title_span in title_links:
        if isinstance(title_span, Tag):
            parent_link = title_span.find_parent('a')
            if isinstance(parent_link, Tag):
                href = parent_link.get('href')
                if isinstance(href, str):
                    # 处理相对链接
                    if href.startswith('/'):
                        full_url = 'https://rg.ru' + href
                    elif href.startswith('https://rg.ru/'):
                        full_url = href
                    else:
                        continue

                    # 检查是否是文章链接
                    if '/202' in full_url and '.html' in full_url:
                        urls.append(full_url)

    return list(set(urls))


def extract_article_links_from_page(soup, base_url):
    """从页面中提取文章链接"""
    urls = []

    # 查找ItemOfListStandard_title__Ajjlf类的链接
    title_links = soup.find_all('span', class_='ItemOfListStandard_title__Ajjlf')
    for title_span in title_links:
        if isinstance(title_span, Tag):
            parent_link = title_span.find_parent('a')
            if isinstance(parent_link, Tag):
                href = parent_link.get('href')
                if isinstance(href, str):
                    # 处理相对链接
                    if href.startswith('/'):
                        full_url = 'https://rg.ru' + href
                    elif href.startswith('https://rg.ru/'):
                        full_url = href
                    else:
                        continue

                    # 检查是否是文章链接
                    if '/202' in full_url and '.html' in full_url:
                        urls.append(full_url)

    # 查找所有可能的文章链接
    links = soup.find_all('a', href=True)
    for link in links:
        if isinstance(link, Tag):
            href = link.get('href')
            if isinstance(href, str):
                # 处理相对链接
                if href.startswith('/'):
                    full_url = 'https://rg.ru' + href
                elif href.startswith('https://rg.ru/'):
                    full_url = href
                else:
                    continue

                # 检查是否是文章链接
                if '/202' in full_url and '.html' in full_url:
                    urls.append(full_url)

    return list(set(urls))


def crawl_channel(channel_url, driver=None, unique_temp_dir=None, chromedriver_path=None):
    print(f"\n🌐 加载频道: {channel_url}")
    # 只在driver为None时才创建新实例，否则始终复用
    if driver is None:
        print("🔧 创建新的浏览器实例...")
        import uuid
        unique_temp_dir = os.path.abspath(f'./chrome_temp_{uuid.uuid4().hex[:8]}')
        os.makedirs(unique_temp_dir, exist_ok=True)
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument(f'--user-data-dir={unique_temp_dir}')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        try:
            if chromedriver_path is None:
                chromedriver_path = get_chromedriver_path()
                if chromedriver_path is None:
                    print("❌ 无法获取ChromeDriver，跳过当前频道")
                    return None, None
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(options=chrome_options, service=service)
        except Exception as e:
            print(f"ChromeDriver启动失败: {e}")
            if unique_temp_dir and os.path.exists(unique_temp_dir):
                try:
                    shutil.rmtree(unique_temp_dir)
                except:
                    pass
            return None, None
    else:
        print("♻️ 复用现有浏览器实例...")
        # 复用时不再更换unique_temp_dir
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    try:
        driver.get(channel_url)
    except Exception as e:
        print(f'⚠️ 访问频道失败: {e}')
        return driver, unique_temp_dir

    sleep(3)

    max_scrolls = 50  # 最大滚动次数
    scroll_count = 0
    seen_links = set()
    titles_set = load_titles()
    print(f"已加载 {len(titles_set)} 个历史标题用于去重")

    # 根据URL确定频道名称
    if '/tema/gos' in channel_url:
        channel_name = '政府'
    elif '/tema/ekonomika' in channel_url:
        channel_name = '经济'
    elif '/tema/mir' in channel_url:
        channel_name = '国际'
    elif '/tema/obshestvo' in channel_url:
        channel_name = '社会'
    elif '/tema/bezopasnost' in channel_url:
        channel_name = '安全'
    else:
        channel_name = '新闻'

    all_articles = []
    no_new_content_count = 0
    no_new_content_threshold = 5

    # 创建会话，提高连接效率
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    })

    try:
        while scroll_count < max_scrolls:
            print(f"\n--- 第 {scroll_count + 1} 次滚动 ---")

            # 新增：频道无文章时重试机制
            retry_channel_count = 0
            while retry_channel_count < 3:
                html = driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                urls = extract_article_links_from_page(soup, channel_url)
                if urls:
                    break
                else:
                    retry_channel_count += 1
                    print(f"⚠️ 未发现任何文章链接，正在重新进入频道（第{retry_channel_count}次）: {channel_url}")
                    driver.get(channel_url)
                    sleep(3 + random.uniform(1, 2))
            else:
                print(f"❌ 连续3次未能在频道页面发现文章，跳过该频道: {channel_url}")
                return driver, unique_temp_dir

            new_urls = [u for u in urls if u not in seen_links]
            print(f'本轮新发现 {len(new_urls)} 个链接')

            # 批量爬取本轮所有新链接
            articles_this_round = []
            success_count = 0
            fail_count = 0

            try:
                for i, url in enumerate(new_urls):
                    seen_links.add(url)
                    print(f'  [{i + 1}/{len(new_urls)}] 爬取: {url}')

                    article_data, title_text, publish_time = crawl_article(url, session)
                    if not article_data or not title_text:
                        fail_count += 1
                        continue
                    if title_text in titles_set:
                        print(f'  × 已爬取过: {title_text}')
                        continue

                    articles_this_round.append(article_data)
                    save_title(title_text)
                    titles_set.add(title_text)
                    success_count += 1
                    print(f'  ✅ 新文章: {title_text}')

                    # 增加随机延迟，避免被反爬虫检测
                    sleep(2 + (i % 3) * 0.5)
            except KeyboardInterrupt:
                print(f"\n⚠️ 在爬取过程中检测到中断，正在保存当前轮已爬取的{len(articles_this_round)}篇文章...")
                if articles_this_round:
                    if not os.path.exists(JSON_DIR):
                        os.makedirs(JSON_DIR)
                    save_articles_grouped_by_date(articles_this_round, channel_name)
                    all_articles.extend(articles_this_round)
                    print(f"✅ 已保存当前轮{len(articles_this_round)}篇文章")
                raise

            print(f'  本轮成功: {success_count}, 失败: {fail_count}')

            # 如果失败率过高，暂停一段时间
            if len(new_urls) > 0 and fail_count / len(new_urls) > 0.7:
                print(f"⚠️ 失败率过高 ({fail_count}/{len(new_urls)})，暂停30秒...")
                sleep(30)

            if not os.path.exists(JSON_DIR):
                os.makedirs(JSON_DIR)
            if articles_this_round:
                save_articles_grouped_by_date(articles_this_round, channel_name)
                all_articles.extend(articles_this_round)
                no_new_content_count = 0
                print(f"✅ 本轮已保存{len(articles_this_round)}篇文章到JSON文件")
            else:
                no_new_content_count += 1
                print(f"本轮未发现新内容，累计{no_new_content_count}次")
                if no_new_content_count >= no_new_content_threshold:
                    print(f"连续{no_new_content_threshold}次未发现新内容，停止滚动")
                    break

            # 滚动到页面底部，等待PageRubricSeo_text__9XF1J元素加载
            try:
                last_height = driver.execute_script("return document.body.scrollHeight")
                # 平滑下滑到底部
                for y in range(0, last_height, 200):
                    driver.execute_script(f"window.scrollTo(0, {y});")
                    sleep(0.02)
                driver.execute_script(f"window.scrollTo(0, {last_height});")
                sleep(0.2)
                # 再往上滑一小部分，模拟真人操作
                driver.execute_script(f"window.scrollTo(0, {last_height - 300});")
                sleep(0.2)
                print("已平滑滚动到页面底部并上滑一小段，等待PageRubricSeo_wrapper__gIhVV元素加载")
                sleep(5 + random.uniform(1, 3))  # 增加随机延迟

                # 等待PageRubricSeo_wrapper__gIhVV元素出现
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "PageRubricSeo_wrapper__gIhVV"))
                    )
                    print("PageRubricSeo_wrapper__gIhVV元素已加载")
                except:
                    print("未检测到PageRubricSeo_wrapper__gIhVV元素，继续尝试...")

                # 等待PageRubricSeo_text__9XF1J元素出现
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "PageRubricSeo_text__9XF1J"))
                    )
                    print("PageRubricSeo_text__9XF1J元素已加载")
                    sleep(5)  # 元素加载后等待5秒
                except:
                    print("未检测到PageRubricSeo_text__9XF1J元素，继续尝试...")

                # 新增：等待5秒后检查页面高度变化，否则尝试点击LoadMore按钮
                loadmore_fail_count = 0
                while True:
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height != last_height:
                        print(f"页面高度从 {last_height} 增加到 {new_height}")
                        no_new_content_count = 0
                        break
                    else:
                        print("页面高度未变化，尝试点击LoadMore按钮...")
                        # 只要是class="LoadMoreBtn_wrapper__A7ItH   "的button都点击
                        btns = driver.find_elements(By.XPATH,
                                                    '//button[contains(@class, "LoadMoreBtn_wrapper__A7ItH")]')
                        found = False
                        for btn in btns:
                            try:
                                btn.click()
                                print("已点击LoadMore按钮，等待内容加载...")
                                sleep(3 + random.uniform(1, 2))
                                found = True
                                break
                            except Exception as e:
                                print(f"点击LoadMore按钮异常: {e}")
                        if not found:
                            print("未找到LoadMore按钮，等待后重试...")
                            sleep(3 + random.uniform(1, 2))
                        loadmore_fail_count += 1
                        if loadmore_fail_count >= 5:
                            print("连续5次等待和点击LoadMore都无效，跳到下一个频道")
                            break
                scroll_count += 1

            except Exception as e:
                print(f"滚动失败: {str(e)}")
                no_new_content_count += 1
                if no_new_content_count >= no_new_content_threshold:
                    print(f"连续{no_new_content_threshold}次滚动失败，停止")
                    break
                sleep(3 + random.uniform(1, 2))
                continue

    except KeyboardInterrupt:
        print("\n⚠️ 检测到用户中断（Ctrl+C），正在保存已爬取内容...")
        print(f"📊 当前频道统计：")
        print(f"  - all_articles列表长度: {len(all_articles)}")
        print(f"  - 已见过的链接数: {len(seen_links)}")
        print(f"  - 滚动次数: {scroll_count}")

        # 检查是否有未保存的文章
        if all_articles:
            print(f"\n⚠️ 正在保存已爬取的{len(all_articles)}篇文章...")
            save_articles_grouped_by_date(all_articles, channel_name)
        else:
            print("\n✅ 所有文章已在每轮中保存到JSON文件，无需额外保存")
        raise
    except Exception as e:
        print(f"\n❌ 频道爬取过程中发生异常: {str(e)}")
        traceback.print_exc()
        # 检查是否有未保存的文章
        if all_articles:
            print(f"\n⚠️ 尝试保存已爬取的{len(all_articles)}篇文章...")
            save_articles_grouped_by_date(all_articles, channel_name)
        # 返回driver和unique_temp_dir供后续使用
        return driver, unique_temp_dir
    finally:
        print(f"\n📊 频道爬取完成统计:")
        print(f"  - 总文章数: {len(all_articles)}")
        print(f"  - 已见过的链接数: {len(seen_links)}")
        print(f"  - 滚动次数: {scroll_count}")

        # 检查是否有未保存的文章
        if all_articles:
            print(f"\n💾 正在保存已爬取的{len(all_articles)}篇文章...")
            save_articles_grouped_by_date(all_articles, channel_name)
        else:
            print("\n✅ 所有文章已在每轮中保存到JSON文件，无需额外保存")
        # 返回driver和unique_temp_dir供后续频道使用
        return driver, unique_temp_dir


def run_crawler():
    print("🎯 RG.ru 频道逐步爬虫启动")

    # 指定的频道列表
    channels = [
        "https://rg.ru/tema/gos",  # 政府
        "https://rg.ru/tema/ekonomika",  # 经济
        "https://rg.ru/tema/mir",  # 国际
        "https://rg.ru/tema/obshestvo",  # 社会
        "https://rg.ru/tema/bezopasnost"  # 安全
    ]

    chromedriver_path = get_chromedriver_path()
    if chromedriver_path is None:
        print("❌ 无法获取ChromeDriver，程序退出")
        return

    driver = None
    unique_temp_dir = None
    try:
        for i, channel_url in enumerate(channels):
            try:
                print(f"\n📺 开始爬取第{i + 1}个频道: {channel_url}")
                driver, unique_temp_dir = crawl_channel(channel_url, driver, unique_temp_dir, chromedriver_path)
                if driver is None:
                    print("❌ 浏览器启动失败，跳过后续频道")
                    break
            except Exception as e:
                print(f"❌ 爬取频道 {channel_url} 时发生异常: {str(e)}")
                traceback.print_exc()
                # 继续下一个频道
                continue
        print("\n🎯 所有频道爬取完成！")
    except KeyboardInterrupt:
        print("\n⚠️ 检测到用户中断（Ctrl+C），正在清理资源...")
        if driver:
            try:
                driver.quit()
                print("🔚 浏览器已关闭")
            except:
                pass
        if unique_temp_dir and os.path.exists(unique_temp_dir):
            try:
                shutil.rmtree(unique_temp_dir)
                print("🧹 已清理临时目录")
            except:
                pass
        return
    except Exception as e:
        print(f"❌ 爬虫运行过程中发生异常: {str(e)}")
        traceback.print_exc()
    finally:
        # 确保浏览器被关闭（只在所有频道后关闭）
        if driver:
            try:
                driver.quit()
                print("🔚 浏览器已关闭")
            except:
                pass
        # 清理临时目录
        if unique_temp_dir and os.path.exists(unique_temp_dir):
            try:
                shutil.rmtree(unique_temp_dir)
                print("🧹 已清理临时目录")
            except:
                pass
        # 清理ChromeDriver缓存
        try:
            if os.path.exists('./chromedriver_cache'):
                shutil.rmtree('./chromedriver_cache')
                print("🧹 已清理ChromeDriver缓存目录")
        except Exception as e:
            print(f"⚠️ 清理ChromeDriver缓存失败: {e}")


def calculate_next_run():
    """计算下一次运行时间（第二天早上6点）"""
    now = datetime.now()
    next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now.hour >= 6:
        next_run += timedelta(days=1)
    return next_run


def main():
    global exception_count

    # 首次运行计数器
    first_run = True

    while True:
        try:
            if first_run:
                print("🚀 首次运行：立即启动爬虫")
                run_crawler()
                first_run = False
                # 重置异常计数器
                exception_count = 0
            else:
                # 计算下一次运行时间
                next_run_time = calculate_next_run()
                wait_seconds = (next_run_time - datetime.now()).total_seconds()

                if wait_seconds > 0:
                    print(f"\n⏳ 爬虫完成，等待下一次运行时间: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"  剩余等待时间: {wait_seconds:.0f}秒 ({wait_seconds / 3600:.1f}小时)")

                    # 每小时报告一次状态
                    while wait_seconds > 0:
                        hours = wait_seconds // 3600
                        minutes = (wait_seconds % 3600) // 60
                        seconds = wait_seconds % 60

                        if hours > 0:
                            print(f"  休眠倒计时: {int(hours)}小时 {int(minutes)}分钟 {int(seconds)}秒", end='\r')
                        elif minutes > 0:
                            print(f"  休眠倒计时: {int(minutes)}分钟 {int(seconds)}秒", end='\r')
                        else:
                            print(f"  休眠倒计时: {int(seconds)}秒", end='\r')

                        sleep(1)
                        wait_seconds -= 1

                    print("\n⏰ 到达预定时间，启动爬虫")
                    run_crawler()
                    # 成功运行后重置异常计数器
                    exception_count = 0
                else:
                    print("⚠️ 等待时间为负，立即启动爬虫")
                    run_crawler()
                    # 成功运行后重置异常计数器
                    exception_count = 0

        except KeyboardInterrupt:
            print("\n👋 用户中断程序，退出")
            break

        except Exception as e:
            exception_count += 1
            print(f"\n❌ 发生未捕获的异常 (第 {exception_count} 次): {str(e)}")
            traceback.print_exc()

            # 检查是否达到最大异常重试次数
            if exception_count >= MAX_EXCEPTION_RETRY:
                print(f"❌ 已达到最大异常重试次数 ({MAX_EXCEPTION_RETRY})，程序退出")
                break

            # 计算冷却时间（随异常次数增加）
            cooldown = EXCEPTION_COOLDOWN * exception_count
            print(f"🔄 {cooldown}秒后重启爬虫...")

            # 带倒计时的冷却等待
            while cooldown > 0:
                print(f"  冷却倒计时: {cooldown}秒", end='\r')
                sleep(1)
                cooldown -= 1
            print("\n🔄 冷却结束，重启爬虫")


if __name__ == '__main__':
    # 确保数据目录存在
    if not os.path.exists(JSON_DIR):
        os.makedirs(JSON_DIR)

    # 运行主程序
    main()