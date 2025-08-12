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
                raise Exception("Rate limit exceeded")
            
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
            
            # Отладочная информация
            self.logger.info(f"🔍 Отладка: размер HTML - {len(response.text)} символов")
            
            # Ищем карточки каналов/чатов - правильные селекторы для TGStat
            cards = parser.css('div[class*="peer"], div[class*="channel"], div[class*="rating"]')
            self.logger.info(f"🔍 Найдено карточек с базовыми селекторами: {len(cards)}")
            
            if not cards:
                # Альтернативные селекторы
                cards = parser.css('div')  # Все div-ы для отладки
                self.logger.info(f"🔍 Всего div элементов: {len(cards)}")
                
                # Ищем div-ы с ссылками на каналы
                cards_with_links = []
                for div in cards:
                    channel_links = div.css('a[href*="/channel/"], a[href*="/chat/"]')
                    if channel_links:
                        cards_with_links.append(div)
                
                cards = cards_with_links[:100]  # Ограничиваем до 100
                self.logger.info(f"🔍 Найдено div-ов со ссылками на каналы: {len(cards)}")
                
            items = []
            for i, card in enumerate(cards):
                try:
                    # Отладочная информация для первых 3 карточек
                    if i < 3:
                        self.logger.info(f"🔍 Карточка {i+1}: {card.html[:200]}...")
                    
                    # Извлечение названия и ссылки
                    title = ""
                    tgstat_link = ""
                    telegram_link = ""
                    
                    # Ищем ссылку на канал/чат внутри карточки
                    channel_link = card.css_first('a[href*="/channel/"], a[href*="/chat/"]')
                    if not channel_link:
                        if i < 3:  # Отладка для первых карточек
                            self.logger.warning(f"🔍 Карточка {i+1}: не найдена ссылка на канал")
                        continue
                        
                    # Получаем название и ссылку
                    title_raw = channel_link.text(strip=True)
                    href = channel_link.attributes.get('href', '')
                    
                    # Очищаем название от лишней информации
                    # Убираем всё после цифр подписчиков
                    title = re.split(r'\d+[\d\s]*подписчик', title_raw)[0].strip()
                    # Убираем категории типа "Новости и СМИ"
                    title = re.split(r'[А-Я][а-я]+ и [А-Я][А-Я]+$', title)[0].strip()
                    
                    if not title or not href:
                        continue
                    
                    # Формируем полную ссылку на TGStat
                    if href.startswith('/'):
                        tgstat_link = urljoin(self.base_url, href)
                    else:
                        tgstat_link = href
                    
                    # Извлекаем username из ссылки TGStat
                    username_match = re.search(r'/(channel|chat)/@([^/]+)', tgstat_link)
                    if username_match:
                        username = username_match.group(2)
                        telegram_link = f"https://t.me/{username}"
                        is_channel = username_match.group(1) == "channel"
                    else:
                        # Если не удалось извлечь username, используем TGStat ссылку
                        telegram_link = f"{tgstat_link} (tgstat)"
                        is_channel = "/channel/" in tgstat_link
                    
                    # Извлечение количества подписчиков из текста карточки
                    subscribers = 0
                    card_text = card.text()
                    
                    # Ищем числа подписчиков в тексте (обычно после названия)
                    # Паттерн: название + число + "подписчиков"
                    subscribers_match = re.search(r'(\d[\d\s]*\d|\d+)\s*подписчиков?', card_text)
                    if not subscribers_match:
                        # Альтернативный поиск - просто большие числа
                        numbers = re.findall(r'(\d[\d\s]{3,})', card_text)
                        if numbers:
                            # Берем самое большое число как количество подписчиков
                            max_num = max([int(n.replace(' ', '')) for n in numbers])
                            if max_num > 100:  # Минимальный порог для подписчиков
                                subscribers = max_num
                    else:
                        subscribers_text = subscribers_match.group(1)
                        subscribers = self.normalize_subscribers(subscribers_text)
                    
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
            # Вместо поиска кнопки "следующая", проверяем количество найденных элементов
            # Если нашли 100 элементов (стандартное количество на странице), скорее всего есть еще страницы
            has_next = len(items) >= 100 or bool(parser.css('a[href*="page="]'))
            
            # Дополнительная проверка - ищем номера страниц больше текущего
            current_page = 1
            try:
                current_page = int(re.search(r'page=(\d+)', url).group(1)) if 'page=' in url else 1
            except:
                current_page = 1
                
            # Ищем ссылки на страницы больше текущей
            page_links = parser.css('a[href*="page="]')
            max_page_found = current_page
            for link in page_links:
                href = link.attributes.get('href', '')
                page_match = re.search(r'page=(\d+)', href)
                if page_match:
                    page_num = int(page_match.group(1))
                    max_page_found = max(max_page_found, page_num)
            
            # Если есть ссылки на страницы больше текущей, значит есть следующие страницы
            if max_page_found > current_page:
                has_next = True
            elif len(items) < 50:  # Если элементов меньше 50, скорее всего это последняя страница
                has_next = False
            
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