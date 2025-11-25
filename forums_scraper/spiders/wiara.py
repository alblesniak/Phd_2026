import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
import re
from ..items import ForumItem, ForumSectionItem, ForumThreadItem, ForumUserItem, ForumPostItem
from ..base_spider import BaseForumSpider
try:
    from ..utils import clean_post_content, normalize_gender, parse_polish_date, extract_urls_from_html, strip_quotes_from_html
except ImportError:
    from utils import clean_post_content, normalize_gender, parse_polish_date, extract_urls_from_html, strip_quotes_from_html


class WiaraSpider(BaseForumSpider):
    name = "wiara"
    allowed_domains = ["forum.wiara.pl"]
    start_urls = ["https://forum.wiara.pl"]
    forum_title = "wiara.pl"

    def parse(self, response):
        # Najpierw utwórz item dla forum
        forum_item = ForumItem()
        forum_item['spider_name'] = self.name
        forum_item['title'] = 'wiara.pl'
        
        # Wyślij item forum do pipeline
        self.logger.info("Yielding forum item")
        yield forum_item
        
        # Znajdź wszystkie linki z klasą "forumlink"
        forum_links = response.css('a.forumlink')
        
        # Usuń ograniczenie - scrapuj wszystkie sekcje
        # forum_links = forum_links[:1]
        
        self.logger.info(f"Znaleziono {len(forum_links)} linków do sekcji forum")
        
        for link in forum_links:
            # Wyciągnij tytuł sekcji
            title = link.css('::text').get().strip()
            
            # Wyciągnij względny URL i przekształć na pełny URL
            relative_url = link.css('::attr(href)').get()
            full_url = urljoin(response.url, relative_url)
            
            # Utwórz item dla sekcji forum
            section_item = ForumSectionItem()
            section_item['title'] = title
            section_item['url'] = full_url
            section_item['forum_id'] = self.name
            
            # Wyślij item sekcji do pipeline
            self.logger.info(f"Yielding section item: {title}")
            yield section_item
            
            # Uruchom spidera wątków dla każdej sekcji
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_section_threads,
                meta={'section_url': full_url, 'section_title': title}
            )

    def parse_section_threads(self, response):
        """Parsuje wątki z sekcji forum"""
        # Wyciągnij wszystkie wątki z aktualnej strony
        threads = response.css('tr:not(:first-child)')
        
        # Filtruj tylko wiersze z wątkami (ma klasę row1 lub row2)
        valid_threads = [thread for thread in threads if thread.css('.row1, .row2')]
        
        # Limit dla testowania - usuń komentarz aby scrapować wszystkie wątki
        # valid_threads = valid_threads[:10]  # Limit do 10 wątków na sekcję
        
        self.logger.info(f"Znaleziono {len(valid_threads)} wątków w sekcji")
        
        # Dodaj cache dla URL wątków aby uniknąć duplikatów
        processed_urls = set()
        
        for thread in valid_threads:
            # Wyciągnij dane wątku
            thread_data = self._extract_thread_data(thread, response)
            if thread_data:
                # Sprawdź czy URL wątku już był przetwarzany
                if thread_data['url'] in processed_urls:
                    continue
                processed_urls.add(thread_data['url'])
                
                self.logger.debug(f"Yielding thread data: {thread_data['title']}")
                yield thread_data
                
                # Wyślij request do scrapowania postów w wątku
                # Wyciągnij thread_id z URL
                thread_id = self._get_thread_id_from_url(thread_data['url'])
                
                yield scrapy.Request(
                    url=thread_data['url'],
                    callback=self.parse_thread_posts,
                    meta={'thread_url': thread_data['url'], 'thread_title': thread_data['title'], 'thread_id': thread_id}
                )
        
        # Znajdź linki do następnych stron (paginacja)
        next_page_links = self._extract_pagination_links(response)
        # Oszacuj liczbę stron: obecna + unikalne linki
        total_pages = 1 + len(next_page_links)
        for i, next_page_url in enumerate(next_page_links):
            # Kolejne strony indeksujemy od 2
            meta = {
                'section_url': response.meta.get('section_url'),
                'section_title': response.meta.get('section_title'),
                'page_scope': 'section',
                'pages_total': total_pages,
                'page_index': i + 2,
            }
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_section_threads,
                meta=meta,
            )

    def _extract_thread_data(self, thread, response):
        """Wyciąga dane wątku z elementu HTML"""
        try:
            # Tytuł i URL wątku
            title_element = thread.css('td:nth-child(2) a.topictitle')
            if not title_element:
                self.logger.debug("Nie znaleziono elementu tytułu wątku")
                return None
                
            # Bezpieczne wyciąganie tytułu
            title_text = title_element.css('::text').get()
            if not title_text:
                self.logger.debug("Nie znaleziono tekstu tytułu wątku")
                return None
            title = title_text.strip()
            
            relative_url = title_element.css('::attr(href)').get()
            if not relative_url:
                self.logger.debug("Nie znaleziono URL wątku")
                return None
            full_url = urljoin(response.url, relative_url)
            
            # Autor wątku
            author_element = thread.css('td:nth-child(3) .topicauthor a')
            author = None
            if author_element:
                author_text = author_element.css('::text').get()
                if author_text:
                    author = author_text.strip()
            
            # Liczba odpowiedzi
            replies_element = thread.css('td:nth-child(4) .topicdetails')
            replies = 0
            if replies_element:
                replies_text = replies_element.css('::text').get()
                if replies_text:
                    try:
                        replies = int(replies_text.strip())
                    except (ValueError, AttributeError):
                        replies = 0
            
            # Liczba wyświetleń
            views_element = thread.css('td:nth-child(5) .topicdetails')
            views = 0
            if views_element:
                views_text = views_element.css('::text').get()
                if views_text:
                    try:
                        views = int(views_text.strip())
                    except (ValueError, AttributeError):
                        views = 0
            
            # Ostatni post - data
            last_post_date_element = thread.css('td:nth-child(6) .topicdetails')
            last_post_date = None
            if last_post_date_element:
                date_text = last_post_date_element.css('::text').get()
                if date_text:
                    raw_last_post_date = date_text.strip()
                    # Konwertuj polską datę na standardowy format
                    last_post_date = parse_polish_date(raw_last_post_date)
                    if last_post_date is None:
                        # Jeśli nie udało się przekonwertować, użyj oryginalnej daty
                        last_post_date = raw_last_post_date
                    self.logger.debug(f"Wyciągnięto last_post_date: {raw_last_post_date} -> przekonwertowano: {last_post_date}")
            
            # Ostatni post - autor
            last_post_author_element = thread.css('td:nth-child(6) .topicdetails a')
            last_post_author = None
            if last_post_author_element:
                author_text = last_post_author_element.css('::text').get()
                if author_text:
                    last_post_author = author_text.strip()
            
            # Utwórz item wątku
            thread_item = ForumThreadItem()
            thread_item['title'] = title
            thread_item['url'] = full_url
            thread_item['author'] = author
            thread_item['replies'] = replies
            thread_item['views'] = views
            thread_item['last_post_date'] = last_post_date
            thread_item['last_post_author'] = last_post_author
            
            self.logger.debug(f"Utworzono item wątku: {title}")
            return thread_item
            
        except Exception as e:
            self.logger.error(f"Błąd podczas wyciągania danych wątku: {e}")
            return None

    def _extract_pagination_links(self, response):
        """Wyciąga linki do następnych stron z paginacji sekcji."""
        return self._extract_forum_pagination_links(response, view_type="viewforum.php")

    def _get_current_start_from_url(self, url):
        """Wyciąga wartość parametru start z aktualnego URL"""
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        return query_params.get('start', ['0'])[0]

    def parse_thread_posts(self, response):
        """Parsuje posty z wątku forum"""
        thread_url = response.meta.get('thread_url')
        thread_title = response.meta.get('thread_title')
        thread_id = response.meta.get('thread_id')
        
        if not thread_id:
            self.logger.warning(f"Nie znaleziono thread_id dla URL: {thread_url}")
            return None
        
        # Wyciągnij wszystkie posty z aktualnej strony
        # W tej strukturze HTML posty są w wierszach z klasą row1 lub row2
        posts = response.css('tr.row1, tr.row2')
        self.logger.info(f"Znaleziono {len(posts)} wierszy w wątku {thread_title}")
        
        for post in posts:
            # Sprawdź czy to wiersz z postem (ma klasę row1 lub row2)
            # Sprawdź czy wiersz zawiera autor w pierwszej kolumnie
            if not post.css('td:first-child .postauthor'):
                continue
                
            # Sprawdź czy wiersz zawiera treść postu w drugiej kolumnie
            if not post.css('td:nth-child(2) .postbody'):
                continue
                
            # Wyciągnij dane użytkownika
            user_data = self._extract_user_data(post)
            if user_data is not None:
                self.logger.debug(f"Yielding user data: {user_data['username']}")
                yield user_data
                
            # Wyciągnij dane postu
            post_data = self._extract_post_data(post, response, thread_id)
            if post_data is not None:
                self.logger.debug(f"Yielding post data: {post_data['username']}")
                yield post_data
        
        # Znajdź linki do następnych stron (paginacja)
        next_page_links = self._extract_thread_pagination_links(response)
        total_pages = 1 + len(next_page_links)
        for i, next_page_url in enumerate(next_page_links):
            meta = {
                'thread_url': thread_url,
                'thread_title': thread_title,
                'thread_id': thread_id,
                'page_scope': 'thread',
                'pages_total': total_pages,
                'page_index': i + 2,
            }
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_thread_posts,
                meta=meta,
            )

    def _extract_post_data(self, post, response, thread_id):
        """Wyciąga dane postu z elementu HTML"""
        try:
            # Autor postu - w pierwszej kolumnie
            author_element = post.css('td:first-child .postauthor')
            if not author_element:
                self.logger.debug("Nie znaleziono elementu .postauthor w post")
                return None
                
            author_text = author_element.css('::text').get()
            if not author_text:
                self.logger.debug("Nie znaleziono tekstu autora w post")
                return None
            author = author_text.strip()
            
            # URL i numer postu (z URL) - w drugiej kolumnie
            post_link = post.css('td:nth-child(2) .postsubject a::attr(href)').get()
            post_number = None
            post_url = None
            if post_link:
                # Konwertuj względny URL na pełny URL
                post_url = urljoin(response.url, post_link)
                # Wyciągnij numer postu z URL np. viewtopic.php?p=1087395
                match = re.search(r'p=(\d+)', post_link)
                if match:
                    post_number = int(match.group(1))
            
            # Zawartość postu - w drugiej kolumnie
            content_element = post.css('td:nth-child(2) .postbody')
            content = ""
            if content_element:
                # Wyciągnij HTML zawartości
                content_html = content_element.get()
                # Usuń cytaty i ekstrahuj URL-e tylko z oryginalnej treści
                content_html_no_quotes = strip_quotes_from_html(content_html)
                content_urls = extract_urls_from_html(content_html_no_quotes, base_url=response.url)
                # Wyczyść zawartość NA BAZIE HTML BEZ CYTATÓW
                content = clean_post_content(content_html_no_quotes)
            
            # Data postu (znajduje się w następnym wierszu)
            post_date = None
            # Sprawdź następny wiersz w tabeli
            next_row = post.xpath('following-sibling::tr[1]')
            if next_row:
                date_element = next_row.css('.postbottom::text')
                if date_element:
                    date_text = date_element.get()
                    if date_text:
                        raw_post_date = date_text.strip()
                        # Konwertuj polską datę na standardowy format
                        post_date = parse_polish_date(raw_post_date)
                        if post_date is None:
                            # Jeśli nie udało się przekonwertować, użyj oryginalnej daty
                            post_date = raw_post_date
                        self.logger.debug(f"Wyciągnięto post_date: {raw_post_date} -> przekonwertowano: {post_date}")
            
            # Utwórz item postu
            post_item = ForumPostItem()
            post_item['thread_id'] = thread_id
            post_item['user_id'] = None  # Będzie ustawione przez pipeline
            post_item['username'] = author  # Dodaj username dla pipeline
            post_item['post_number'] = post_number
            post_item['content'] = content
            post_item['content_urls'] = content_urls if 'content_urls' in locals() else []
            post_item['post_date'] = post_date
            post_item['url'] = post_url
            
            self.logger.debug(f"Utworzono item postu dla autora: {author}, thread_id: {thread_id}")
            return post_item
            
        except Exception as e:
            self.logger.error(f"Błąd podczas wyciągania danych postu: {e}")
            return None

    def _extract_user_data(self, post):
        """Wyciąga dane użytkownika z postu"""
        try:
            # Username - w pierwszej kolumnie
            author_element = post.css('td:first-child .postauthor')
            if not author_element:
                self.logger.debug("Nie znaleziono elementu .postauthor")
                return None
                
            author_text = author_element.css('::text').get()
            if not author_text:
                self.logger.debug("Nie znaleziono tekstu autora")
                return None
            username = author_text.strip()
            
            # Dane użytkownika z postdetails - w pierwszej kolumnie
            details_element = post.css('td:first-child .postdetails')
            join_date = None
            posts_count = None
            
            if details_element:
                details_text = details_element.get()
                
                # Data dołączenia
                join_match = re.search(r'<b>Dołączył\(a\):</b>\s*([^<]+)', details_text)
                if join_match:
                    raw_join_date = join_match.group(1).strip()
                    # Konwertuj polską datę na standardowy format
                    join_date = parse_polish_date(raw_join_date)
                    if join_date is None:
                        # Jeśli nie udało się przekonwertować, użyj oryginalnej daty
                        join_date = raw_join_date
                    self.logger.debug(f"Wyciągnięto join_date: {raw_join_date} -> przekonwertowano: {join_date}")
                
                # Liczba postów
                posts_match = re.search(r'<b>Posty:</b>\s*(\d+)', details_text)
                if posts_match:
                    posts_count = int(posts_match.group(1))
                    self.logger.debug(f"Wyciągnięto posts_count: {posts_count}")
            
            # Utwórz item z danymi użytkownika
            user_item = ForumUserItem()
            user_item['username'] = username
            user_item['join_date'] = join_date
            user_item['posts_count'] = posts_count
            
            return user_item
            
        except Exception as e:
            self.logger.error(f"Błąd podczas wyciągania danych użytkownika: {e}")
            return None

    def _extract_thread_pagination_links(self, response):
        """Wyciąga linki do następnych stron postów w wątku"""
        pagination_links = []
        
        # Znajdź linki paginacji
        pagination_elements = response.css('td.gensmall a[href*="viewtopic.php"]')
        
        for element in pagination_elements:
            href = element.css('::attr(href)').get()
            if href:
                full_url = urljoin(response.url, href)
                pagination_links.append(full_url)
        
        return pagination_links

    def _get_thread_id_from_url(self, thread_url):
        """Znajduje thread_id na podstawie URL wątku"""
        try:
            parsed_url = urlparse(thread_url)
            query_params = parse_qs(parsed_url.query)
            thread_param = query_params.get('t', [None])[0]
            
            if thread_param:
                # Znajdź thread_id w bazie danych na podstawie URL
                # To będzie zaimplementowane w pipeline
                self.logger.debug(f"Znaleziono thread_id: {thread_param} dla URL: {thread_url}")
                return thread_param
            
            self.logger.warning(f"Nie znaleziono parametru 't' w URL: {thread_url}")
            return None
            
        except Exception as e:
            self.logger.error(f"Błąd podczas znajdowania thread_id: {e}")
            return None
