# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from tqdm import tqdm
import time
import os
import json
from datetime import datetime
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
from twisted.internet.error import TimeoutError, DNSLookupError
from twisted.internet.error import NoRouteError, TCPTimedOutError

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class CustomRetryMiddleware(RetryMiddleware):
    """Custom middleware dla lepszego zarządzania retry i timeoutami"""
    
    def __init__(self, settings):
        super().__init__(settings)
        self.max_retry_times = settings.getint('RETRY_TIMES', 3)
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES', [500, 502, 503, 504, 408, 429, 522, 523, 524]))
    
    def process_exception(self, request, exception, spider):
        """Obsługuje wyjątki związane z timeoutami i błędami połączenia"""
        if isinstance(exception, (TimeoutError, TCPTimedOutError, DNSLookupError, NoRouteError)):
            spider.logger.warning(f"Timeout/błąd połączenia dla {request.url}: {exception}")
            return self._retry(request, exception, spider)
        
        return None
    
    def process_response(self, request, response, spider):
        """Obsługuje odpowiedzi HTTP z błędami"""
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            spider.logger.warning(f"Błąd HTTP {response.status} dla {request.url}: {reason}")
            return self._retry(request, reason, spider) or response
        
        return response
    
    def _retry(self, request, reason, spider):
        """Próbuje ponownie z większym timeoutem"""
        retries = request.meta.get('retry_times', 0) + 1
        
        if retries <= self.max_retry_times:
            spider.logger.info(f"Ponowna próba {retries}/{self.max_retry_times} dla {request.url}")
            
            # Zwiększ timeout dla kolejnych prób
            new_timeout = 30 + (retries * 15)  # 30s, 45s, 60s
            request.meta['download_timeout'] = new_timeout
            request.meta['retry_times'] = retries
            
            # Dodaj losowe opóźnienie przed ponowną próbą
            import random
            delay = random.uniform(1, 3)
            request.meta['download_delay'] = delay
            
            return request.copy()
        
        spider.logger.error(f"Przekroczono maksymalną liczbę prób dla {request.url}")
        return None


class ProgressMiddleware:
    """Middleware do wyświetlania postępu z tqdm"""
    
    def __init__(self):
        self.pbar = None
        self.start_time = None
        self.items_processed = 0
        self.current_section = None
        self.current_thread = None
        self.events_path = os.environ.get("FS_PROGRESS_EVENTS_PATH")
        self._evfh = None
        
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(middleware.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(middleware.request_scheduled, signal=signals.request_scheduled)
        return middleware
    
    def spider_opened(self, spider):
        """Inicjalizuje pasek postępu"""
        self.start_time = time.time()
        # Jeśli podano ścieżkę do zdarzeń – nie zaśmiecaj konsoli paskiem, emituj zdarzenia
        if not self.events_path:
            self.pbar = tqdm(
                desc=f"Scrapowanie {spider.name}",
                unit="itemów",
                dynamic_ncols=True,
                bar_format='{l_bar}{bar}| {n_fmt} [{elapsed}, {rate_fmt}]'
            )
        else:
            # Otwórz plik zdarzeń (line-buffered)
            os.makedirs(os.path.dirname(self.events_path), exist_ok=True)
            self._evfh = open(self.events_path, "a", encoding="utf-8", buffering=1)
            self._emit({
                "type": "spider_started",
                "spider": spider.name,
                "ts": datetime.utcnow().isoformat()
            })
        spider.logger.info("Rozpoczęto scrapowanie (progress middleware)")
    
    def spider_closed(self, spider):
        """Zamyka pasek postępu"""
        elapsed_time = time.time() - self.start_time
        if self.pbar is not None:
            self.pbar.close()
        if self._evfh is not None:
            self._emit({
                "type": "spider_closed",
                "spider": spider.name,
                "items": self.items_processed,
                "elapsed": elapsed_time,
                "ts": datetime.utcnow().isoformat()
            })
            try:
                self._evfh.close()
            except Exception:
                pass
        spider.logger.info(f"Zakończono scrapowanie: {self.items_processed} itemów w {elapsed_time:.2f}s")
    
    def request_scheduled(self, request, spider):
        """Aktualizuje informację o aktualnej sekcji"""
        if hasattr(request, 'meta') and request.meta:
            section_title = request.meta.get('section_title') or request.meta.get('section_name')
            if section_title:
                self.current_section = section_title
                self._emit({
                    "type": "context_update",
                    "spider": spider.name,
                    "section": self.current_section,
                    "ts": datetime.utcnow().isoformat(),
                })
            thread_title = request.meta.get('thread_title')
            if thread_title:
                self.current_thread = thread_title
                self._emit({
                    "type": "context_update",
                    "spider": spider.name,
                    "thread": self.current_thread,
                    "ts": datetime.utcnow().isoformat(),
                })

            # Paginacja – jeżeli spider poda meta z numerem strony
            try:
                pi = request.meta.get('page_index')
                pt = request.meta.get('pages_total')
                if pi is not None and pt is not None:
                    scope = request.meta.get('page_scope') or 'section'
                    self._emit({
                        "type": "pagination_update",
                        "spider": spider.name,
                        "current": int(pi),
                        "total": int(pt),
                        "scope": str(scope),
                        "ts": datetime.utcnow().isoformat(),
                    })
            except Exception:
                pass
    
    def item_scraped(self, item, response, spider):
        """Aktualizuje pasek postępu przy każdym itemie"""
        self.items_processed += 1
        item_type = type(item).__name__
        if self.pbar is not None:
            self.pbar.update(1)
            # Aktualizuj opis z informacją o typie itemu i sekcji
            if item_type == 'ForumThreadItem' and self.current_section:
                description = f"Scrapowanie {spider.name} ({item_type} - {self.current_section})"
            else:
                description = f"Scrapowanie {spider.name} ({item_type})"
            self.pbar.set_description(description)
        else:
            # Emituj zdarzenie JSON
            payload = {
                "type": "item_scraped",
                "spider": spider.name,
                "item_type": item_type,
                "section": self.current_section,
                "thread": self.current_thread,
                "ts": datetime.utcnow().isoformat(),
            }
            self._emit(payload)

    def _emit(self, event: dict):
        if self._evfh is None:
            return
        try:
            self._evfh.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception:
            pass


class ScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware is not modifying the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class ScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware is not modifying the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
