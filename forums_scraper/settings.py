# Scrapy settings for scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "forums_scraper"

SPIDER_MODULES = ["forums_scraper.spiders"]
NEWSPIDER_MODULE = "forums_scraper.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False  # Wyłączone dla szybszego scrapowania

# Concurrency and throttling settings - OPTYMALIZACJA DLA SZYBKOŚCI
CONCURRENT_REQUESTS = 32  # Zmniejszone z 64 do 32 dla stabilności
CONCURRENT_REQUESTS_PER_DOMAIN = 32
CONCURRENT_REQUESTS_PER_IP = 32

# Download delay - zwiększone dla stabilności
DOWNLOAD_DELAY = 0.5  # Zwiększone z 0.1 do 0.5 sekundy
RANDOMIZE_DOWNLOAD_DELAY = True

# Enable cookies for better forum compatibility
COOKIES_ENABLED = True

# Wyłącz niepotrzebne middleware dla szybszego scrapowania
#DOWNLOADER_MIDDLEWARES = {
#    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None,
#    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
#    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
#    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,
#    'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
#    # Włączamy kompresję HTTP dla lepszej wydajności
#    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
#    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
#    # Custom retry middleware dla lepszego zarządzania timeoutami
#    'forums_scraper.middlewares.CustomRetryMiddleware': 550,
#}

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
   "forums_scraper.middlewares.ProgressMiddleware": 543,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "scraper.middlewares.ScraperDownloaderMiddleware": 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   "forums_scraper.pipelines.SQLitePipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 0.1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 1.0
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 32.0
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = False  # Wyłączone dla świeżych danych
#HTTPCACHE_EXPIRATION_SECS = 3600  # 1 godzina
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Keep-alive connections - ZWIĘKSZONE TIMEOUTY DLA STABILNOŚCI
DOWNLOAD_TIMEOUT = 30  # Zwiększone z 15 do 30 sekund
DOWNLOAD_MAXSIZE = 0
DOWNLOAD_WARNSIZE = 0

# Dodatkowe optymalizacje dla stabilności
REACTOR_THREADPOOL_MAXSIZE = 20
DNS_TIMEOUT = 30  # Zwiększone z 15 do 30 sekund
RETRY_TIMES = 3  # Zwiększone z 1 do 3 dla lepszej odporności
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429, 522, 523, 524]

# Logging settings - wyłączamy INFO logi dla cichszego działania
LOG_LEVEL = 'WARNING'  # Zmienione z 'INFO' na 'WARNING'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# SQLite database settings - jedna wspólna baza danych dla wszystkich forów
SQLITE_DATABASE_PATH = "data/databases/forums_unified.db"

# Ustawienia forums-scraper
# Ścieżka do pliku konfiguracyjnego YAML/TOML (opcjonalna)
# CLI automatycznie generuje konfigurację w data/databases/scraper_config.yaml
FS_CONFIG_PATH = "data/databases/scraper_config.yaml"

# Wymuś asyncio reactor dla wsparcia async pipeline
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
