# TGStat Parser

Парсер каталогов Telegram каналов и чатов с сайта TGStat.ru

## Краткое описание

Скрипт собирает информацию о Telegram каналах и чатах с TGStat по категориям, парсит заданное количество страниц и сохраняет результаты в CSV файлы с обязательными колонками: title (название), subscribers (подписчики), link (ссылка на t.me).

## Установка

### Windows PowerShell

```powershell
# Клонируйте репозиторий или скопируйте файлы
cd your_project_directory

# Установите зависимости
pip install -r requirements.txt

# Скопируйте файл настроек и настройте при необходимости
copy .env.example .env
```

### Linux/Mac

```bash
# Клонируйте репозиторий или скопируйте файлы
cd your_project_directory

# Установите зависимости
pip install -r requirements.txt

# Скопируйте файл настроек и настройте при необходимости
cp .env.example .env
```

## Примеры использования

### Основные команды

```bash
# Парсинг каналов по прямой ссылке (5 страниц)
python tgstat_parser.py --url "https://tgstat.ru/ratings/channels/news" --type channels --pages 5

# Парсинг чатов по категории (3 страницы)
python tgstat_parser.py --category news --type chats --pages 3

# Парсинг с прокси через переменную окружения
python tgstat_parser.py --url "https://tgstat.ru/ratings/chats/travel" --pages 2 --proxy "%PROXY%"

# Быстрая проверка работоспособности (1 страница)
python tgstat_parser.py --self-check --url "https://tgstat.ru/ratings/channels/business"
```

### Windows примеры

```powershell
# Базовый пример для Windows
python tgstat_parser.py --url "https://tgstat.ru/ratings/channels/crypto" --pages 3

# С настройкой задержек
python tgstat_parser.py --category tech --type channels --pages 5 --delay 1.2

# Сохранение в конкретную папку
python tgstat_parser.py --url "https://tgstat.ru/ratings/chats/gaming" --pages 2 --outdir "D:\results"
```

## Настройка прокси

### Через файл .env

```env
PROXY=http://username:password@proxy.example.com:8080
REQUEST_DELAY_BASE=1.0
REQUEST_DELAY_JITTER=0.5
```

### Через параметры командной строки

```bash
python tgstat_parser.py --proxy "http://proxy.example.com:8080" --url "..." --pages 3
```

## Структура выходных файлов

Результаты сохраняются в папку `output/` в виде CSV файлов:

- `output/channels.csv` - данные по каналам
- `output/chats.csv` - данные по чатам

### Формат CSV

```csv
title,subscribers,link
Новости России,125000,https://t.me/news_russia
IT чат,45300,https://t.me/it_chat_ru
```

## Решение проблем

## Решение проблем

### При блокировках и антиботе

**⚠️ ВАЖНО: Если вы видите ошибку "Требуется авторизация - 429"**

TGStat может блокировать автоматические запросы. Вот способы решения:

1. **Увеличьте задержки до 10+ секунд:**
   ```bash
   python tgstat_parser.py --delay 10.0 --url "..." --pages 1
   ```

2. **Используйте прокси:**
   ```bash
   # Через параметр
   python tgstat_parser.py --proxy "http://proxy:port" --url "..."
   
   # Через .env файл
   echo "PROXY=http://proxy:port" > .env
   python tgstat_parser.py --url "..."
   ```

3. **Попробуйте в разное время:**
   - Утром или поздним вечером (меньше нагрузки)
   - В выходные дни

4. **Уменьшите количество страниц:**
   ```bash
   python tgstat_parser.py --pages 1 --url "..."
   ```

5. **Альтернативные подходы:**
   - Используйте VPN
   - Смените IP адрес
   - Попробуйте мобильный интернет

### ⚡ Быстрое тестирование работоспособности

Если скрипт не работает, сначала проверьте:

```bash
# Тест с минимальными настройками
python tgstat_parser.py --self-check --delay 15.0

# Если не работает, попробуйте с прокси
python tgstat_parser.py --self-check --delay 15.0 --proxy "http://your-proxy:port"
```

### При ошибках 429 (Rate Limit)

```bash
# Максимальные задержки
python tgstat_parser.py --delay 20.0 --pages 1 --url "..."

# С прокси и большими задержками
python tgstat_parser.py --delay 20.0 --pages 1 --proxy "http://proxy:port" --url "..."
```

### При ошибках установки

```bash
# Обновите pip
pip install --upgrade pip

# Установите зависимости по одной
pip install curl_cffi
pip install selectolax
pip install python-dotenv
pip install fake-useragent
pip install tenacity
```

## Логирование

Логи сохраняются в:
- Консоль (в реальном времени)
- Файл `logs/app.log` (полная история)

Уровни логирования:
- `INFO` - основная информация о процессе
- `WARNING` - предупреждения (например, не удалось извлечь данные)
- `ERROR` - серьезные ошибки

## Сборка исполняемого файла (.exe)

### Для Windows с PyInstaller

```powershell
# Установите PyInstaller
pip install pyinstaller

# Соберите исполняемый файл
pyinstaller --onefile --name tgstat_parser tgstat_parser.py

# Исполняемый файл будет в папке dist/
.\dist\tgstat_parser.exe --url "https://tgstat.ru/ratings/channels/news" --pages 2
```

## Дополнительные параметры

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `--url` | Прямая ссылка на категорию TGStat | - |
| `--category` | Код категории (news, tech, crypto, etc.) | - |
| `--type` | Тип контента: channels или chats | channels |
| `--pages` | Количество страниц для парсинга | 1 |
| `--outdir` | Папка для сохранения результатов | ./output |
| `--delay` | Базовая задержка между запросами (сек) | 0.8 |
| `--proxy` | Прокси сервер | - |
| `--self-check` | Быстрая проверка на 1 странице | false |

## Популярные категории TGStat

- `news` - Новости
- `tech` - Технологии
- `crypto` - Криптовалюты
- `business` - Бизнес
- `entertainment` - Развлечения
- `sport` - Спорт
- `travel` - Путешествия
- `education` - Образование
- `gaming` - Игры
- `music` - Музыка

## Лицензия и ограничения

- Скрипт предназначен для образовательных целей
- Соблюдайте правила использования TGStat.ru
- Не злоупотребляйте частотой запросов
- Используйте прокси при интенсивном парсинге
