import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
import re

from ..items import ForumItem, ForumSectionItem, ForumThreadItem, ForumUserItem, ForumPostItem
from ..utils import clean_post_content, normalize_gender, parse_polish_date, extract_urls_from_html, strip_quotes_from_html
from ..base_spider import BaseForumSpider


class ZChrystusemSpider(BaseForumSpider):
    """
    Spider do scrapowania forum Z Chrystusem (zchrystusem.pl)
    
    Ten spider jest funkcjonalnie analogiczny do DolinaModlitwySpider, ale dostosowany 
    do struktury HTML forum Z Chrystusem. Wyodrębnia sekcje, wątki, posty 
    i dane użytkowników z dodatkowymi polami: religia, płeć i lokalizacja.
    """
    name = "z_chrystusem"
    allowed_domains = ["zchrystusem.pl"]
    start_urls = ["https://zchrystusem.pl/"]
    forum_title = "Z Chrystusem"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set do śledzenia odwiedzonych URL-i paginacji w tej sesji
        self.visited_pagination_urls = set()
        # Set do śledzenia odwiedzonych sekcji/subforów, aby ograniczać duplikaty
        self.visited_section_urls = set()

    def parse(self, response):
        """
        Parsuje stronę główną forum, wyodrębnia sekcje i tworzy obiekty ForumItem oraz ForumSectionItem.
        
        Args:
            response: Obiekt odpowiedzi Scrapy
            
        Yields:
            ForumItem: Item reprezentujący forum
            ForumSectionItem: Itemy reprezentujące sekcje forum
            scrapy.Request: Requesty do scrapowania wątków w każdej sekcji
        """
        # Najpierw utwórz item dla forum
        forum_item = ForumItem()
        forum_item['spider_name'] = self.name
        forum_item['title'] = 'Z Chrystusem'
        
        # Wyślij item forum do pipeline
        self.logger.info("Yielding forum item")
        yield forum_item
        
        # Znajdź wszystkie linki do sekcji forum - struktura HTML zchrystusem.pl
        # Sekcje znajdują się w elementach z klasą "forumtitle" w div.forabg
        forum_links = response.css('div.forabg a.forumtitle')
        
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
            
            # Wyślij item sekcji do pipeline
            self.logger.info(f"Yielding section item: {title}")
            yield section_item
            
            # Uruchom spidera wątków dla każdej sekcji
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_section_threads,
                meta={'section_url': full_url, 'section_title': title}
            )

        # Dodatkowo: znajdź subfora wypisane pod sekcjami na stronie głównej
        subforum_links = response.css('div.forabg a.subforum')
        self.logger.info(f"Znaleziono {len(subforum_links)} linków do subforów na stronie głównej")
        for link in subforum_links:
            sub_title_texts = [t.strip() for t in link.css('::text').getall() if t.strip()]
            sub_title = sub_title_texts[0] if sub_title_texts else 'subforum'
            relative_url = link.css('::attr(href)').get()
            full_url = urljoin(response.url, relative_url)

            if full_url in self.visited_section_urls:
                continue
            self.visited_section_urls.add(full_url)

            section_item = ForumSectionItem()
            section_item['title'] = sub_title
            section_item['url'] = full_url
            self.logger.info(f"Yielding subforum section item: {sub_title}")
            yield section_item

            yield scrapy.Request(
                url=full_url,
                callback=self.parse_section_threads,
                meta={'section_url': full_url, 'section_title': sub_title}
            )

    def parse_section_threads(self, response):
        """
        Parsuje wątki z sekcji forum
        
        Args:
            response: Obiekt odpowiedzi Scrapy
            
        Yields:
            ForumThreadItem: Itemy reprezentujące wątki
            scrapy.Request: Requesty do scrapowania postów w wątkach
        """
        # Najpierw: wykryj subfora w obrębie tej sekcji i wejdź do nich rekurencyjnie
        subforum_links = response.css('a.subforum')
        if subforum_links:
            self.logger.info(f"Znaleziono {len(subforum_links)} subforów w sekcji {response.meta.get('section_title')}")
        for link in subforum_links:
            sub_title_texts = [t.strip() for t in link.css('::text').getall() if t.strip()]
            sub_title = sub_title_texts[0] if sub_title_texts else 'subforum'
            href = link.css('::attr(href)').get()
            if not href:
                continue
            full_url = urljoin(response.url, href)
            if full_url in self.visited_section_urls:
                continue
            self.visited_section_urls.add(full_url)

            sub_section_item = ForumSectionItem()
            sub_section_item['title'] = sub_title
            sub_section_item['url'] = full_url
            yield sub_section_item

            yield scrapy.Request(
                url=full_url,
                callback=self.parse_section_threads,
                meta={'section_url': full_url, 'section_title': sub_title}
            )

        # Wyciągnij wszystkie wątki z aktualnej strony - struktura HTML zchrystusem.pl
        # Wątki znajdują się w elementach li.row z klasą bg1 lub bg2
        threads = response.css('li.row.bg1, li.row.bg2')
        
        # Filtruj tylko wiersze z wątkami (nie sticky, nie announcement/global-announce)
        valid_threads = []
        for thread in threads:
            classes = thread.attrib.get('class', '') if hasattr(thread, 'attrib') else (thread.css('::attr(class)').get() or '')
            if re.search(r'\b(sticky|announce|announcement|global-announce)\b', classes):
                continue
            # Dodatkowo pomiń, jeśli w obrębie elementu są znaczniki typowe dla ogłoszeń
            if thread.css('.global-announce, .announce, .announcement, .sticky'):
                continue
            valid_threads.append(thread)
        
        self.logger.info(f"Znaleziono {len(valid_threads)} wątków w sekcji")
        
        for thread in valid_threads:
            # Wyciągnij dane wątku
            thread_data = self._extract_thread_data(thread, response)
            if thread_data:
                self.logger.debug(f"Yielding thread data: {thread_data['title']}")
                # Dodaj informację o sekcji do thread_data
                thread_data['section_url'] = response.meta.get('section_url')
                thread_data['section_title'] = response.meta.get('section_title')
                yield thread_data
                
                # Wyślij request do scrapowania postów w wątku
                yield scrapy.Request(
                    url=thread_data['url'],
                    callback=self.parse_thread_posts,
                    meta={'thread_url': thread_data['url'], 'thread_title': thread_data['title'], 'thread_id': None}
                )
        
        # Znajdź linki do następnych stron (paginacja)
        next_page_links = self._extract_pagination_links(response)
        self.logger.info(f"Znaleziono {len(next_page_links)} linków paginacji dla wątków w sekcji: {response.meta.get('section_title')}")
        
        # Sprawdź czy nie ma duplikatów w linkach paginacji
        seen_urls = set()
        for next_page_url in next_page_links:
            if next_page_url not in seen_urls:
                seen_urls.add(next_page_url)
                self.logger.debug(f"Wysyłam request do następnej strony wątków: {next_page_url}")
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse_section_threads,
                    meta={'section_url': response.meta.get('section_url'), 
                          'section_title': response.meta.get('section_title')}
                )
            else:
                self.logger.debug(f"Pominięto duplikat URL: {next_page_url}")

    def _extract_thread_data(self, thread, response):
        """
        Wyciąga dane wątku z elementu HTML
        
        Args:
            thread: Element HTML reprezentujący wątek
            response: Obiekt odpowiedzi Scrapy
            
        Returns:
            ForumThreadItem: Item z danymi wątku lub None jeśli błąd
        """
        try:
            # Tytuł i URL wątku - struktura HTML zchrystusem.pl
            title_element = thread.css('a.topictitle')
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
            
            # Autor wątku - struktura HTML zchrystusem.pl
            author_element = thread.css('.topic-poster a.username, .topic-poster a.username-coloured')
            author = None
            if author_element:
                author_text = author_element.css('::text').get()
                if author_text:
                    author = author_text.strip()
            
            # Liczba odpowiedzi - struktura HTML zchrystusem.pl
            replies_element = thread.css('dd.posts')
            replies = 0
            if replies_element:
                replies_text = replies_element.css('::text').get()
                if replies_text:
                    try:
                        # Wyciągnij liczbę z tekstu "Odpowiedzi: 14"
                        replies_match = re.search(r'Odpowiedzi:\s*(\d+)', replies_text)
                        if replies_match:
                            replies = int(replies_match.group(1))
                    except (ValueError, AttributeError):
                        replies = 0
            
            # Liczba wyświetleń - struktura HTML zchrystusem.pl
            views_element = thread.css('dd.posts')
            views = 0
            if views_element:
                views_text = views_element.css('::text').get()
                if views_text:
                    try:
                        # Wyciągnij liczbę z tekstu "Odsłony:4573"
                        views_match = re.search(r'Odsłony:\s*(\d+)', views_text)
                        if views_match:
                            views = int(views_match.group(1))
                    except (ValueError, AttributeError):
                        views = 0
            
            # Ostatni post - data - struktura HTML zchrystusem.pl
            last_post_date_element = thread.css('dd.lastpost time')
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
            
            # Ostatni post - autor - struktura HTML zchrystusem.pl
            last_post_author_element = thread.css('dd.lastpost a.username, dd.lastpost a.username-coloured')
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
        """
        Wyciąga linki do następnych stron z paginacji
        
        Args:
            response: Obiekt odpowiedzi Scrapy
            
        Returns:
            list: Lista URL-i do następnych stron
        """
        pagination_links = []
        
        # Znajdź wszystkie linki z parametrem start= w URL - struktura HTML zchrystusem.pl
        # Linki paginacji znajdują się w div.pagination ul li a
        all_links = response.css('div.pagination ul li a[href*="start="]::attr(href)').getall()
        
        # Jeśli nie znaleziono linków, spróbuj alternatywny selektor
        if not all_links:
            all_links = response.css('div.pagination a[href*="start="]::attr(href)').getall()
            self.logger.debug(f"Użyto alternatywnego selektora, znaleziono {len(all_links)} linków")
        
        self.logger.debug(f"Znaleziono {len(all_links)} linków z parametrem start= w paginacji wątków")
        
        for link in all_links:
            full_url = urljoin(response.url, link)
            
            # Sprawdź czy to link do następnej strony (nie do konkretnego postu)
            if 'viewforum.php' in full_url and 'start=' in full_url:
                # Wyciągnij wartość parametru start
                parsed_url = urlparse(full_url)
                query_params = parse_qs(parsed_url.query)
                start_value = query_params.get('start', [0])[0]
                
                # Dodaj tylko jeśli to nowa strona (nie aktualna)
                current_start = self._get_current_start_from_url(response.url)
                self.logger.debug(f"Porównuję start_value='{start_value}' z current_start='{current_start}'")
                if start_value != current_start:
                    pagination_links.append(full_url)
                    self.logger.debug(f"Dodano link paginacji wątków: {full_url} (start={start_value})")
                else:
                    self.logger.debug(f"Pominięto link do aktualnej strony: {full_url}")
        
        # Usuń duplikaty i zwróć unikalne linki
        unique_links = list(set(pagination_links))
        self.logger.debug(f"Zwracam {len(unique_links)} unikalnych linków paginacji wątków")
        return unique_links

    def _get_current_start_from_url(self, url):
        """
        Wyciąga wartość parametru start z aktualnego URL
        
        Args:
            url (str): URL do sprawdzenia
            
        Returns:
            str: Wartość parametru start lub '0' jeśli nie znaleziono
        """
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        start_value = query_params.get('start', ['0'])[0]
        self.logger.debug(f"Wyciągnięto start_value '{start_value}' z URL: {url}")
        return start_value

    def parse_thread_posts(self, response):
        """
        Parsuje posty z wątku forum
        
        Args:
            response: Obiekt odpowiedzi Scrapy
            
        Yields:
            ForumUserItem: Itemy reprezentujące użytkowników
            ForumPostItem: Itemy reprezentujące posty
        """
        thread_url = response.meta.get('thread_url')
        thread_title = response.meta.get('thread_title')
        
        # Upewnij się, że sekcja (f=) jest zarejestrowana w bazie dla mapowania thread -> section
        try:
            section_href = response.css('a[href*="viewforum.php?f="]::attr(href)').get()
            if section_href:
                section_abs = urljoin(response.url, section_href)
                section_item = ForumSectionItem()
                section_item['title'] = response.meta.get('section_title') or 'manual'
                section_item['url'] = section_abs
                yield section_item
        except Exception:
            pass

        # Znajdź thread_id na podstawie URL
        thread_id = self._get_thread_id_from_url(thread_url)
        if not thread_id:
            # Fallback: jeśli URL zawiera tylko p=, spróbuj znaleźć link do wątku z t=
            candidate = response.css('a[href*="viewtopic.php?t="]::attr(href)').get()
            if candidate:
                abs_thread_url = urljoin(response.url, candidate)
                # Ustal parametry t i f
                try:
                    parsed = urlparse(abs_thread_url)
                    params = parse_qs(parsed.query)
                    f_param = params.get('f', [None])[0]
                except Exception:
                    f_param = None

                # Utwórz minimalny wątek i (ewentualnie) sekcję dla pipeline
                thread_item = ForumThreadItem()
                thread_item['title'] = thread_title or 'manual'
                thread_item['url'] = abs_thread_url

                section_url = None
                if f_param:
                    section_url = urljoin(f"{parsed.scheme}://{parsed.netloc}/", f"viewforum.php?f={f_param}")
                    section_item = ForumSectionItem()
                    section_item['title'] = 'manual'
                    section_item['url'] = section_url
                    yield section_item

                if section_url:
                    thread_item['section_url'] = section_url

                yield thread_item
        
        # Wyciągnij wszystkie posty z aktualnej strony - struktura HTML zchrystusem.pl
        # Posty znajdują się w elementach div z id="p..." i klasą "post"
        posts = response.css('div[id^="p"].post')
        self.logger.info(f"Znaleziono {len(posts)} postów w wątku {thread_title}")
        
        for post in posts:
            # Sprawdź czy to post z profilem użytkownika
            if not post.css('.postprofile'):
                continue
                
            # Sprawdź czy post zawiera treść
            if not post.css('.postbody'):
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
        self.logger.info(f"Znaleziono {len(next_page_links)} linków paginacji dla postów w wątku: {thread_title}")
        
        # Sprawdź czy nie ma duplikatów w linkach paginacji
        seen_urls = set()
        for next_page_url in next_page_links:
            if next_page_url not in seen_urls:
                seen_urls.add(next_page_url)
                self.logger.debug(f"Wysyłam request do następnej strony postów: {next_page_url}")
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse_thread_posts,
                    meta={'thread_url': thread_url, 'thread_title': thread_title, 'thread_id': thread_id}
                )
            else:
                self.logger.debug(f"Pominięto duplikat URL postów: {next_page_url}")

    def _extract_post_data(self, post, response, thread_id):
        """
        Wyciąga dane postu z elementu HTML
        
        Args:
            post: Element HTML reprezentujący post
            response: Obiekt odpowiedzi Scrapy
            thread_id: ID wątku
            
        Returns:
            ForumPostItem: Item z danymi postu lub None jeśli błąd
        """
        try:
            # Autor postu - struktura HTML zchrystusem.pl
            author_element = post.css('.postprofile .username-coloured, .postprofile .username')
            if not author_element:
                self.logger.debug("Nie znaleziono elementu autora w post")
                return None
                
            author_text = author_element.css('::text').get()
            if not author_text:
                self.logger.debug("Nie znaleziono tekstu autora w post")
                return None
            author = author_text.strip()
            
            # URL i numer postu (z URL) - struktura HTML zchrystusem.pl
            post_link = post.css('p.author a::attr(href)').get()
            post_number = None
            post_url = None
            if post_link:
                # Konwertuj względny URL na pełny URL
                post_url = urljoin(response.url, post_link)
                # Wyciągnij numer postu z URL np. viewtopic.php?p=1087395
                match = re.search(r'p=(\d+)', post_link)
                if match:
                    post_number = int(match.group(1))
            
            # Zawartość postu - struktura HTML zchrystusem.pl
            content_element = post.css('.postbody .content')
            content = ""
            if content_element:
                # Wyciągnij HTML zawartości
                content_html = content_element.get()
                # Usuń cytaty i ekstrahuj URL-e tylko z oryginalnej treści
                content_html_no_quotes = strip_quotes_from_html(content_html)
                content_urls = extract_urls_from_html(content_html_no_quotes, base_url=response.url)
                # Wyczyść zawartość używając standardowej funkcji NA PODSTAWIE HTML BEZ CYTATÓW
                content = clean_post_content(content_html_no_quotes)
            
            # Data postu - struktura HTML zchrystusem.pl
            post_date = None
            # 1) Preferowany tag <time>
            date_element = post.css('p.author time')
            if date_element:
                date_text = date_element.css('::text').get()
                if date_text:
                    raw_post_date = date_text.strip()
                    post_date = parse_polish_date(raw_post_date) or raw_post_date
            # 2) Fallback: surowy tekst w <p class="author">, np. "2022-07-11, 09:07"
            if not post_date:
                try:
                    author_text_all = ' '.join([t.strip() for t in post.css('p.author ::text').getall() if t and t.strip()])
                    # Wyciągnij wzorzec daty YYYY-MM-DD, HH:MM lub YYYY-MM-DD HH:MM
                    import re as _re
                    m = _re.search(r'(\d{4}-\d{2}-\d{2})(?:,)?\s+(\d{1,2}:\d{2})', author_text_all)
                    if m:
                        post_date = f"{m.group(1)} {m.group(2)}"
                except Exception:
                    pass
            
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
        """
        Wyciąga dane użytkownika z postu
        
        Args:
            post: Element HTML reprezentujący post
            
        Returns:
            ForumUserItem: Item z danymi użytkownika lub None jeśli błąd
        """
        try:
            # Username - struktura HTML zchrystusem.pl
            author_element = post.css('.postprofile .username-coloured, .postprofile .username')
            if not author_element:
                self.logger.debug("Nie znaleziono elementu autora")
                return None
                
            author_text = author_element.css('::text').get()
            if not author_text:
                self.logger.debug("Nie znaleziono tekstu autora")
                return None
            username = author_text.strip()
            
            # Dane użytkownika z profilu - struktura HTML zchrystusem.pl
            join_date = None
            posts_count = None
            religion = None
            gender = None
            localization = None
            
            # Data dołączenia - struktura HTML zchrystusem.pl
            join_element = post.css('.profile-joined')
            if join_element:
                join_text = join_element.get()
                join_match = re.search(r'<strong>Rejestracja:</strong>\s*([^<]+)', join_text)
                if join_match:
                    raw_join_date = join_match.group(1).strip()
                    # Konwertuj polską datę na standardowy format
                    join_date = parse_polish_date(raw_join_date)
                    if join_date is None:
                        # Jeśli nie udało się przekonwertować, użyj oryginalnej daty
                        join_date = raw_join_date
                    self.logger.debug(f"Wyciągnięto join_date: {raw_join_date} -> przekonwertowano: {join_date}")
            
            # Liczba postów - struktura HTML zchrystusem.pl
            posts_element = post.css('.profile-posts')
            if posts_element:
                posts_text = posts_element.get()
                posts_match = re.search(r'<strong>Posty:</strong>\s*<a[^>]*>(\d+)</a>', posts_text)
                if posts_match:
                    posts_count = int(posts_match.group(1))
                    self.logger.debug(f"Wyciągnięto posts_count: {posts_count}")
            
            # Religia - pole specyficzne dla Z Chrystusem
            religion_element = post.css('.profile-religia')
            if religion_element:
                religion_text = religion_element.get()
                religion_match = re.search(r'<strong>Wyznanie:</strong>\s*([^<]+)', religion_text)
                if religion_match:
                    religion = religion_match.group(1).strip()
                    self.logger.debug(f"Wyciągnięto religion: {religion}")
            
            # Płeć - pole specyficzne dla Z Chrystusem (może nie być dostępne)
            # W forum Z Chrystusem może nie być tego pola, więc ustawiamy None
            gender = None
            
            # Lokalizacja - struktura HTML zchrystusem.pl
            localization_element = post.css('.profile-phpbb_location')
            if localization_element:
                localization_text = localization_element.get()
                localization_match = re.search(r'<strong>Lokalizacja:</strong>\s*([^<]+)', localization_text)
                if localization_match:
                    localization = localization_match.group(1).strip()
                    self.logger.debug(f"Wyciągnięto localization: {localization}")
            
            # Utwórz item użytkownika
            user_item = ForumUserItem()
            user_item['username'] = username
            user_item['join_date'] = join_date
            user_item['posts_count'] = posts_count
            user_item['religion'] = religion
            user_item['gender'] = gender
            user_item['localization'] = localization
            
            self.logger.debug(f"Utworzono item użytkownika: {username}, join_date: {join_date}, posts_count: {posts_count}, religion: {religion}, gender: {gender}, localization: {localization}")
            return user_item
            
        except Exception as e:
            self.logger.error(f"Błąd podczas wyciągania danych użytkownika: {e}")
            return None

    def _extract_thread_pagination_links(self, response):
        """
        Wyciąga linki do następnych stron postów w wątku
        
        Args:
            response: Obiekt odpowiedzi Scrapy
            
        Returns:
            list: Lista URL-i do następnych stron postów
        """
        pagination_links = []
        
        # Znajdź linki paginacji - struktura HTML zchrystusem.pl
        # Linki paginacji znajdują się w div.pagination ul li a
        pagination_elements = response.css('div.pagination ul li a[href*="viewtopic.php"]')
        
        # Jeśli nie znaleziono elementów, spróbuj alternatywny selektor
        if not pagination_elements:
            pagination_elements = response.css('div.pagination a[href*="viewtopic.php"]')
            self.logger.debug(f"Użyto alternatywnego selektora dla postów, znaleziono {len(pagination_elements)} elementów")
        
        self.logger.debug(f"Znaleziono {len(pagination_elements)} elementów paginacji postów")
        
        for element in pagination_elements:
            href = element.css('::attr(href)').get()
            if href:
                full_url = urljoin(response.url, href)
                
                # Sprawdź czy to link do następnej strony (nie do konkretnego postu)
                if 'start=' in full_url:
                    # Wyciągnij wartość parametru start
                    parsed_url = urlparse(full_url)
                    query_params = parse_qs(parsed_url.query)
                    start_value = query_params.get('start', [0])[0]
                    
                    # Dodaj tylko jeśli to nowa strona (nie aktualna)
                    current_start = self._get_current_start_from_url(response.url)
                    self.logger.debug(f"Porównuję start_value='{start_value}' z current_start='{current_start}' dla postów")
                    if start_value != current_start:
                        pagination_links.append(full_url)
                        self.logger.debug(f"Dodano link paginacji postów: {full_url} (start={start_value})")
                    else:
                        self.logger.debug(f"Pominięto link do aktualnej strony postów: {full_url}")
        
        # Usuń duplikaty i zwróć unikalne linki
        unique_links = list(set(pagination_links))
        self.logger.debug(f"Zwracam {len(unique_links)} unikalnych linków paginacji postów")
        return unique_links

    def _get_thread_id_from_url(self, thread_url):
        """
        Znajduje thread_id na podstawie URL wątku
        
        Args:
            thread_url (str): URL wątku
            
        Returns:
            str: ID wątku lub None jeśli nie znaleziono
        """
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
