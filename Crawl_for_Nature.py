# crawl for specific Nature papers
# -*- coding: utf-8 -*-
"""
目标页面: https://www.nature.com/collections/gxfyskqtkm
新功能: 
- 引入 requests.Session 和 Retry 机制，应对服务器连接重置错误。
- 增强请求头，模拟真实浏览器。
- 优化延迟策略，提高爬取稳定性。
"""
import os
import logging
import time
import random
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- 1. 初始化和配置 ---
LOG_FILE = "collection_downloader.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, 'a', 'utf-8'),
        logging.StreamHandler()
    ]
)

# --- 2. 核心功能函数 ---

def create_session_with_retries():
    """
    创建一个强大的 requests Session 对象，包含自动重试机制。
    这是应对网络波动和反爬虫封禁的核心。
    """
    session = requests.Session()
    
    # 定义重试策略
    retry_strategy = Retry(
        total=3,  # 总重试次数
        status_forcelist=[429, 500, 502, 503, 504],  # 对这些状态码进行重试
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1  # 重试之间的等待时间会增加 (如 1s, 2s, 4s)
    )
    
    # 创建一个适配器并挂载重试策略
    adapter = HTTPAdapter(max_retries=retry_strategy)
    
    # 为 http 和 https 都应用这个适配器
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # 设置一个更像浏览器的请求头
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    
    return session

def sanitize_filename(name):
    """清理字符串，移除文件名或目录名中的非法字符。"""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:180]

def get_all_download_links(session, article_url):
    """访问单个文章页面，抓取所有需要下载的文件的链接。"""
    logging.info(f"  - 正在解析文章页面: {article_url}")
    links = {}
    try:
        # 使用传入的、带重试功能的 session 对象
        resp = session.get(article_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        all_a_tags = soup.find_all('a', href=True)

        # 查找逻辑不变
        pdf_tag = soup.find('a', class_='c-pdf-download__link')
        if pdf_tag: links['pdf'] = urljoin(article_url, pdf_tag['href'])
        else:
            for a in all_a_tags:
                if 'Download PDF' in a.get_text() or a['href'].endswith('.pdf'):
                    links['pdf'] = urljoin(article_url, a['href']); break
        
        for a in all_a_tags:
            if 'Supplementary Information' in a.get_text(strip=True):
                links['supp'] = urljoin(article_url, a['href']); break
                
        for a in all_a_tags:
            if 'Peer Review File' in a.get_text(strip=True):
                links['peer'] = urljoin(article_url, a['href']); break

        time.sleep(random.uniform(2, 4)) # 增加随机延迟

    except requests.exceptions.RequestException as e:
        logging.error(f"  - 解析文章页面失败 (多次重试后): {article_url}, 错误: {e}")
        
    return links

def download_file(session, url, dest_folder, file_prefix='', base_filename=''):
    """使用带重试的session下载单个文件。"""
    os.makedirs(dest_folder, exist_ok=True)
    try:
        original_filename = url.split("/")[-1].split("?")[0]
        extension = original_filename.split('.')[-1] if '.' in original_filename else 'download'
        local_filename = f"{file_prefix}{base_filename}.{extension}"
        local_path = os.path.join(dest_folder, local_filename)

        if os.path.exists(local_path):
            logging.info(f"    - 文件已存在，跳过: {local_filename}")
            return True

        logging.info(f"    - 正在下载: {local_filename} -> to '{dest_folder}'")
        with session.get(url, stream=True, timeout=90) as r: # 增加下载超时时间
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        time.sleep(random.uniform(3, 6)) # 增加下载后延迟
        return True

    except Exception as e:
        logging.error(f"    - 下载失败 (多次重试后): {url}, 错误: {e}")
        return False

# --- 3. 主执行逻辑 ---

def main():
    """主函数，协调整个爬取和下载流程。"""
    
    collection_url = "https://www.nature.com/collections/gxfyskqtkm"
    collection_name = "Nature Collection " + collection_url.strip('/').split('/')[-1]
    
    logging.info(f"\n{'='*20} 开始处理合集: {collection_name} {'='*20}")
    
    # 创建一个贯穿整个爬取过程的会话对象
    session = create_session_with_retries()
    
    try:
        logging.info(f"-> 正在访问目标页面: {collection_url}")
        resp = session.get(collection_url, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"请求合集页面失败: {collection_url}, 错误: {e}")
        return

    soup = BeautifulSoup(resp.text, 'html.parser')
    articles = soup.find_all('article', class_='u-full-height')
    if not articles: articles = soup.find_all('article', class_='c-card')

    if not articles:
        logging.warning("在此页面未找到任何文章。")
        return
        
    logging.info(f"  - 在合集页面找到 {len(articles)} 篇文章。")

    for article in articles:
        try:
            title_tag = article.find(['h3', 'h2'], class_='c-card__title')
            if not title_tag or not title_tag.a: continue

            article_title_raw = title_tag.a.get_text(strip=True)

            base_site = 'https://www.nature.com'
            article_link = urljoin(base_site, title_tag.a['href'])
            sanitized_title = sanitize_filename(article_title_raw)
            article_folder = os.path.join(collection_name, sanitized_title)
            
            # 使用同一个session对象来获取链接和下载
            download_links = get_all_download_links(session, article_link)
            
            if not download_links:
                logging.warning(f"  - 未找到 '{article_title_raw}' 的任何下载链接。")
                continue

            logging.info(f"-> 开始下载文章 '{article_title_raw}' 的文件...")
            if 'pdf' in download_links:
                download_file(session, download_links['pdf'], article_folder, "PDF_", sanitized_title)
            if 'supp' in download_links:
                download_file(session, download_links['supp'], article_folder, "Supp_", sanitized_title)
            if 'peer' in download_links:
                download_file(session, download_links['peer'], article_folder, "PeerReview_", sanitized_title)
                
        except Exception as e:
            logging.error(f"处理单篇文章时发生未知错误: {e}")
            continue
        
        # 增加更长的文章间随机等待时间
        logging.info("--- 文章处理完毕，暂停一段时间 ---")
        time.sleep(random.uniform(8, 15))

    logging.info(f"\n合集 '{collection_name}' 处理完毕！")

if __name__ == "__main__":
    main()
