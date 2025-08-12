#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import logging
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

try:
    from curl_cffi import requests
    from fake_useragent import UserAgent
    from selectolax.parser import HTMLParser
    from dotenv import load_dotenv
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Установите зависимости: pip install -r requirements.txt")
    sys.exit(1)


class TGStatParser:
    def __init__(self, proxy: Optional[str] = None, delay_base: float = 0.8, delay_jitter: float = 0.4):
        self.base_url = "https://tgstat.ru"
        self.session = requests.Session(impersonate="chrome110")
        self.ua = UserAgent()
        self.delay_base = delay_base
        self.delay_jitter = delay_jitter
        self.proxy = proxy
        self.channels_data: Set[Tuple[str, int, str]] = set()  # title, subscribers, link
        self.chats_data: Set[Tuple[str, int, str]] = set()  # title, subscribers, link
        
        # Настройка прокси
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
            
        # Настройка логирования
        self.setup_logging()
        
    def setup_logging(self):
        """Настройка логирования в файл и консоль"""
        # Создаем папку для логов
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # Настройка логгера
        self.logger = logging.getLogger("tgstat_parser")
        self.logger.setLevel(logging.INFO)
        
        # Убираем существующие handlers
        self.logger.handlers = []
        
        # Формат для логов
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Файловый handler
        file_handler = logging.FileHandler(logs_dir / "app.log", encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Консольный handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
    def get_random_headers(self) -> Dict[str, str]:
        """Генерация случайных заголовков для антибот защиты"""
        return {
            "User-Agent": self.ua.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
        }

    def random_delay(self):
        """Случайная задержка с джиттером"""
        delay = self.delay_base + random.uniform(0, self.delay_jitter)
        time.sleep(delay)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type((Exception,)))
    def make_request(self, url: str) -> Optional[requests.Response]:
        """Выполнение HTTP запроса с обработкой ошибок и ретраями"""
        try:
            headers = self.get_random_headers()
            self.logger.info(f"🌐 Запрос к: {url}")
            
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 429:
                self.logger.warning("⚠️ Rate limit (429), увеличиваем задержку...")
                time.sleep(5 + random.uniform(0, 5))
                raise requests.RequestException("Rate limit exceeded")
            
            if response.status_code not in [200, 404]:
                self.logger.error(f"❌ HTTP {response.status_code}: {url}")
                response.raise_for_status()
                
            return response
        except Exception as e:
            self.logger.error(f"❌ Ошибка запроса {url}: {e}")
            raise

    def normalize_subscribers(self, text: str) -> int:
        """Нормализация количества подписчиков в числовой формат"""
        if not text:
            return 0
            
        # Убираем все символы кроме цифр, точек, запятых и букв K/M
        text = re.sub(r'[^\d.,KMkm]', '', text.strip())
        
        if not text:
            return 0
            
        # Заменяем запятые на точки для единообразия
        text = text.replace(',', '.')
        
        try:
            # Обработка K (тысячи) и M (миллионы)
            if text.lower().endswith('k'):
                number = float(text[:-1])
                return int(number * 1000)
            elif text.lower().endswith('m'):
                number = float(text[:-1]) 
                return int(number * 1000000)
            else:
                # Обычное число, убираем точки если они разделители тысяч
                if '.' in text and len(text.split('.')[-1]) == 3:
                    # Скорее всего разделитель тысяч
                    text = text.replace('.', '')
                return int(float(text))
        except (ValueError, IndexError):
            self.logger.warning(f"⚠️ Не удалось конвертировать подписчиков: {text}")
            return 0

    def extract_telegram_link(self, card_html: str, tgstat_url: str = "") -> str:
        """Извлечение прямой ссылки на Telegram канал/чат"""
        parser = HTMLParser(card_html)
        
        # Ищем прямые ссылки на t.me
        tme_links = parser.css('a[href*="t.me"]')
        if tme_links:
            href = tme_links[0].attributes.get('href', '')
            if href.startswith('https://t.me/'):
                return href
                
        # Ищем в data-атрибутах или скрытых полях
        data_attrs = parser.css('[data-username]')
        if data_attrs:
            username = data_attrs[0].attributes.get('data-username', '').strip('@')
            if username:
                return f"https://t.me/{username}"
        
        # Ищем в тексте username формата @username
        text_content = parser.text()
        username_match = re.search(r'@([a-zA-Z0-9_]+)', text_content)
        if username_match:
            username = username_match.group(1)
            return f"https://t.me/{username}"
            
        # В крайнем случае используем tgstat url с пометкой
        if tgstat_url:
            return f"{tgstat_url} (tgstat)"
            
        return ""

    def parse_page(self, url: str) -> Tuple[List[Dict], bool]:
        """Парсинг одной страницы каталога"""
        self.random_delay()
        
        try:
            response = self.make_request(url)
            if not response or response.status_code != 200:
                return [], False
                
            parser = HTMLParser(response.text)
            
            # Ищем карточки каналов/чатов
            cards = parser.css('.card, .channel-card, .chat-card, .peer-card')
            if not cards:
                # Альтернативные селекторы
                cards = parser.css('[data-peer-id], .peer-item, .rating-item')
                
            items = []
            for card in cards:
                try:
                    # Извлечение названия
                    title_elem = card.css_first('.card-title, .peer-title, .channel-title, h3, h4, .title')
                    if not title_elem:
                        title_elem = card.css_first('a[href*="/channel/"], a[href*="/chat/"]')
                    
                    if not title_elem:
                        continue
                        
                    title = title_elem.text(strip=True)
                    if not title:
                        continue
                    
                    # Извлечение количества подписчиков
                    subscribers = 0
                    subscribers_elem = card.css_first('.subscribers, .members, .peer-subscribers, .count')
                    if subscribers_elem:
                        subscribers_text = subscribers_elem.text(strip=True)
                        subscribers = self.normalize_subscribers(subscribers_text)
                    
                    # Извлечение ссылки
                    tgstat_link = ""
                    link_elem = card.css_first('a[href]')
                    if link_elem:
                        href = link_elem.attributes.get('href', '')
                        if href.startswith('/'):
                            tgstat_link = urljoin(self.base_url, href)
                        else:
                            tgstat_link = href
                    
                    # Извлекаем Telegram ссылку
                    telegram_link = self.extract_telegram_link(card.html, tgstat_link)
                    if not telegram_link:
                        telegram_link = tgstat_link
                    
                    # Определяем тип (канал или чат) по URL или содержимому
                    is_channel = "/channel/" in tgstat_link or "канал" in title.lower()
                    
                    item = {
                        'title': title,
                        'subscribers': subscribers,
                        'link': telegram_link,
                        'type': 'channel' if is_channel else 'chat'
                    }
                    items.append(item)
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Ошибка парсинга карточки: {e}")
                    continue
            
            # Проверяем наличие следующей страницы
            has_next = bool(parser.css('a[href*="page="]:contains("Следующая"), .pagination .next, a.next'))
            
            self.logger.info(f"✅ Найдено {len(items)} элементов на странице")
            return items, has_next
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка парсинга страницы {url}: {e}")
            return [], False

    def build_url(self, category: str = "", url: str = "", page: int = 1, item_type: str = "channels") -> str:
        """Построение URL для запроса"""
        if url:
            # Если передан прямой URL
            if "page=" in url:
                # Заменяем номер страницы
                return re.sub(r'page=\d+', f'page={page}', url)
            else:
                # Добавляем параметр страницы
                separator = "&" if "?" in url else "?"
                return f"{url}{separator}page={page}"
        else:
            # Строим URL из категории
            return f"{self.base_url}/ratings/{item_type}/{category}?page={page}"

    def parse_catalog(self, category: str = "", url: str = "", pages: int = 1, item_type: str = "channels"):
        """Главная функция парсинга каталога"""
        self.logger.info(f"🚀 Начинаем парсинг: категория='{category}', url='{url}', страниц={pages}, тип='{item_type}'")
        
        for page_num in range(1, pages + 1):
            page_url = self.build_url(category, url, page_num, item_type)
            self.logger.info(f"📄 Страница {page_num}/{pages}: {page_url}")
            
            items, has_next = self.parse_page(page_url)
            
            if not items:
                self.logger.warning(f"⚠️ Нет данных на странице {page_num}, завершаем...")
                break
            
            # Сохраняем данные с дедупликацией
            for item in items:
                data_tuple = (item['title'], item['subscribers'], item['link'])
                
                if item['type'] == 'channel':
                    self.channels_data.add(data_tuple)
                else:
                    self.chats_data.add(data_tuple)
            
            # Если нет следующей страницы, завершаем
            if not has_next and page_num < pages:
                self.logger.info(f"📄 Достигнута последняя страница ({page_num})")
                break
                
        self.logger.info(f"✅ Парсинг завершен. Каналов: {len(self.channels_data)}, чатов: {len(self.chats_data)}")

    def save_to_csv(self, output_dir: str = "./output"):
        """Сохранение результатов в CSV файлы"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Сохранение каналов
        if self.channels_data:
            channels_file = output_path / "channels.csv"
            with open(channels_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['title', 'subscribers', 'link'])
                writer.writerows(sorted(self.channels_data, key=lambda x: x[1], reverse=True))
            self.logger.info(f"💾 Каналы сохранены: {channels_file} ({len(self.channels_data)} записей)")
        
        # Сохранение чатов
        if self.chats_data:
            chats_file = output_path / "chats.csv"
            with open(chats_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['title', 'subscribers', 'link'])
                writer.writerows(sorted(self.chats_data, key=lambda x: x[1], reverse=True))
            self.logger.info(f"💾 Чаты сохранены: {chats_file} ({len(self.chats_data)} записей)")


def main():
    # Загрузка переменных окружения
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Парсер каталогов Telegram на TGStat")
    
    # Основные параметры
    parser.add_argument("--url", help="Прямая ссылка на категорию TGStat")
    parser.add_argument("--category", help="Код категории (например: news, travel)")
    parser.add_argument("--type", choices=["channels", "chats"], default="channels", 
                        help="Тип контента для парсинга")
    parser.add_argument("--pages", type=int, default=1, 
                        help="Количество страниц для парсинга")
    
    # Дополнительные настройки
    parser.add_argument("--outdir", default="./output", 
                        help="Папка для сохранения результатов")
    parser.add_argument("--delay", type=float, default=0.8, 
                        help="Базовая задержка между запросами")
    parser.add_argument("--proxy", help="Прокси для запросов")
    parser.add_argument("--self-check", action="store_true", 
                        help="Быстрая проверка на 1 странице")
    
    args = parser.parse_args()
    
    # Валидация аргументов
    if not args.url and not args.category:
        print("❌ Ошибка: укажите --url или --category")
        sys.exit(1)
    
    # Настройка прокси (приоритет: аргумент > переменная окружения)
    proxy = args.proxy or os.getenv('PROXY')
    delay_base = args.delay or float(os.getenv('REQUEST_DELAY_BASE', 0.8))
    delay_jitter = float(os.getenv('REQUEST_DELAY_JITTER', 0.4))
    
    # Быстрая проверка
    if args.self_check:
        print("🔍 Режим быстрой проверки (1 страница)...")
        args.pages = 1
    
    # Создаем парсер и запускаем
    tgstat = TGStatParser(proxy=proxy, delay_base=delay_base, delay_jitter=delay_jitter)
    
    try:
        # Парсинг
        tgstat.parse_catalog(
            category=args.category or "",
            url=args.url or "",
            pages=args.pages,
            item_type=args.type
        )
        
        # Сохранение результатов
        tgstat.save_to_csv(args.outdir)
        
        print(f"\n🎉 Готово! Результаты сохранены в {args.outdir}/")
        print(f"   📊 Каналов: {len(tgstat.channels_data)}")
        print(f"   📊 Чатов: {len(tgstat.chats_data)}")
        
    except KeyboardInterrupt:
        print("\n⏹️ Парсинг прерван пользователем")
        tgstat.save_to_csv(args.outdir)
        sys.exit(0)
    except Exception as e:
        tgstat.logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()