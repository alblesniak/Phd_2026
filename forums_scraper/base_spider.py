import re
from typing import Iterable, Optional, List
from urllib.parse import urljoin, urlparse, parse_qs

import scrapy

from .items import ForumItem, ForumSectionItem, ForumThreadItem


class BaseForumSpider(scrapy.Spider):
    """Wspólna logika dla spiderów forów.

    Zapewnia obsługę parametru ``only_thread_url`` oraz pomocnicze metody
    do wyciągania identyfikatorów z URL-i i budowania minimalnych itemów.
    Konkretne spidery powinny nadpisać selektory w ``parse``/
    ``parse_section_threads``/``parse_thread_posts``.
    """

    forum_title: str = ""

    def __init__(self, only_thread_url: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.only_thread_url = only_thread_url

    # --- helpers wspólne ---

    def _get_thread_id_from_url(self, url: Optional[str]) -> Optional[int]:
        """Wyciąga ID wątku z URL-a (parametr ``t`` lub ``p``)."""
        if not url:
            return None
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for key in ("t", "p"):
            val_list = params.get(key)
            if val_list:
                val = val_list[0]
                if val and val.isdigit():
                    return int(val)
        # fallback: spróbuj z końcówki ścieżki
        m = re.search(r"(\d+)$", parsed.path or "")
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
        return None

    def _extract_forum_pagination_links(
        self,
        response: scrapy.http.Response,
        view_type: str = "viewforum.php",
    ) -> List[str]:
        """Wspólny helper do wyciągania linków paginacji sekcji.

        Szuka odnośników zawierających zarówno ``view_type`` jak i parametr
        ``start=`` i odfiltrowuje link do bieżącej strony (na podstawie
        wartości ``start`` w aktualnym URL).
        """

        current_parsed = urlparse(response.url)
        current_params = parse_qs(current_parsed.query)
        current_start = current_params.get("start", ["0"])[0]

        links: List[str] = []
        hrefs = response.css(f'a[href*="{view_type}"][href*="start="]::attr(href)').getall()
        if not hrefs:
            # fallback: dowolne linki z parametrem start=
            hrefs = response.css('a[href*="start="]::attr(href)').getall()

        for href in hrefs:
            full_url = urljoin(response.url, href)
            parsed = urlparse(full_url)
            if view_type not in parsed.path:
                continue
            params = parse_qs(parsed.query)
            start_val = params.get("start", ["0"])[0]
            if start_val == current_start:
                continue
            links.append(full_url)

        # usuń duplikaty zachowując kolejność
        seen = set()
        unique_links: List[str] = []
        for url in links:
            if url in seen:
                continue
            seen.add(url)
            unique_links.append(url)
        return unique_links

    def _build_forum_item(self) -> ForumItem:
        item = ForumItem()
        item["spider_name"] = self.name
        item["title"] = self.forum_title or self.name
        return item

    def _build_minimal_section_item(self, section_url: str, title: str = "manual") -> ForumSectionItem:
        section_item = ForumSectionItem()
        section_item["title"] = title
        section_item["url"] = section_url
        section_item["forum_id"] = self.name
        return section_item

    def _build_minimal_thread_item(self, thread_url: str, section_url: Optional[str] = None, title: str = "manual") -> ForumThreadItem:
        thread_item = ForumThreadItem()
        thread_item["title"] = title
        thread_item["url"] = thread_url
        if section_url:
            thread_item["section_url"] = section_url
        return thread_item

    # --- tryb only_thread_url ---

    def _start_requests_only_thread(self) -> Iterable[scrapy.Request]:
        """Wspólna implementacja trybu ``only_thread_url``.

        - emituje minimalne Forum/Section/Thread
        - wywołuje ``parse_thread_posts`` na podanym URL.
        Konkretny spider może nadpisać tę metodę, jeśli potrzebuje więcej
        metadanych związanych z HTML.
        """

        thread_url = self.only_thread_url
        if not thread_url:
            return []

        try:
            parsed = urlparse(thread_url)
            params = parse_qs(parsed.query)
            f_param = params.get("f", [None])[0]
            section_url = (
                urljoin(f"{parsed.scheme}://{parsed.netloc}/", f"viewforum.php?f={f_param}")
                if f_param
                else None
            )
        except Exception:
            section_url = None

        # Forum
        yield self._build_forum_item()

        # Sekcja (minimalna)
        if section_url:
            yield self._build_minimal_section_item(section_url)

        # Wątek (minimalny)
        yield self._build_minimal_thread_item(thread_url, section_url=section_url)

        thread_id = self._get_thread_id_from_url(thread_url)
        yield scrapy.Request(
            url=thread_url,
            callback=self.parse_thread_posts,
            meta={"thread_url": thread_url, "thread_title": "manual", "thread_id": thread_id},
        )

    def start_requests(self) -> Iterable[scrapy.Request]:  # type: ignore[override]
        """Domyślna obsługa trybu ``only_thread_url``.

        Jeśli ``only_thread_url`` jest ustawione, używa `_start_requests_only_thread`,
        w przeciwnym razie deleguje do standardowego startu po `start_urls`.
        """

        if getattr(self, "only_thread_url", None):
            yield from self._start_requests_only_thread()
            return

        for url in getattr(self, "start_urls", []):
            yield scrapy.Request(url=url, callback=self.parse)
