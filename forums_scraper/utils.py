import re
from datetime import datetime
from urllib.parse import urljoin, urlparse


def _truncate(text, width):
    if text is None:
        return ""
    s = str(text)
    if len(s) <= width:
        return s
    if width <= 1:
        return s[:width]
    return s[: width - 1] + "…"


def columns(
    spider_name: str,
    phase: str,
    col2: str | None = None,
    col3: str | None = None,
    col4: str | None = None,
    *,
    w_spider: int = 16,
    w_phase: int = 10,
    w2: int = 22,
    w3: int = 22,
    w4: int = 22,
):
    ts = datetime.now().strftime("%H:%M:%S")
    parts = [
        f"{ts}",
        f"{_truncate(spider_name, w_spider):<{w_spider}}",
        f"{_truncate(phase, w_phase):<{w_phase}}",
    ]
    if col2 is not None:
        parts.append(f"{_truncate(col2, w2):<{w2}}")
    if col3 is not None:
        parts.append(f"{_truncate(col3, w3):<{w3}}")
    if col4 is not None:
        parts.append(f"{_truncate(col4, w4):<{w4}}")
    return " | ".join(parts)


def clean_post_content(content):
    """
    Czyści zawartość postu z cytatów, URL-i, obrazów i podpisów
    
    Args:
        content (str): Surowa zawartość HTML postu
        
    Returns:
        str: Wyczyściona zawartość tekstowa postu
    """
    if not content:
        return ""
    
    # Usuń cytaty (div class="quotewrapper") i inne możliwe kontenery cytatów
    content = re.sub(r'<div class="quotewrapper">.*?</div>', '', content, flags=re.DOTALL)
    content = re.sub(r'<blockquote[^>]*>.*?</blockquote>', '', content, flags=re.IGNORECASE | re.DOTALL)
    content = re.sub(r"<div[^>]+class=\"[^\"]*quote[^\"]*\"[^>]*>.*?</div>", '', content, flags=re.IGNORECASE | re.DOTALL)
    
    # Usuń podpisy (span class="postbody signature")
    content = re.sub(r'<span class="postbody signature">.*?</span>', '', content, flags=re.DOTALL)

    # Usuń sekcję notice (np. "Ostatnio zmieniony ...")
    content = re.sub(r'<div class="notice">.*?</div>', '', content, flags=re.IGNORECASE | re.DOTALL)

    # Usuń informator "Dodano po ..." (często w zagnieżdżonych spanach)
    content = re.sub(r'<span[^>]*>\s*<span[^>]*>\s*Dodano po[\s\S]*?</span>\s*</span>', '', content, flags=re.IGNORECASE)
    
    # Usuń obrazy
    content = re.sub(r'<img[^>]*>', '', content)
    
    # Usuń linki wraz z ich tekstem (link tekst nie jest zachowywany)
    content = re.sub(r'<a[^>]*>.*?</a>', '', content, flags=re.IGNORECASE | re.DOTALL)
    
    # Usuń adresy URL w tekście (http/https/www)
    content = re.sub(r'https?://[^\s<>"]+', '', content)
    content = re.sub(r'www\.[^\s<>"]+', '', content)
    # Usuń frazy "link z opisem on-line" i podobne
    content = re.sub(r'link z opisem on-line', '', content, flags=re.IGNORECASE)
    
    # Zamień tagi <br> na znaki nowej linii
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    
    # Zamień tagi <div> i <span> na znaki nowej linii (aby rozdzielić bloki tekstu)
    content = re.sub(r'</div>', '\n', content)
    content = re.sub(r'</span>', '\n', content)
    content = re.sub(r'<div[^>]*>', '', content)
    content = re.sub(r'<span[^>]*>', '', content)
    
    # Usuń inne tagi HTML, zachowaj tekst
    content = re.sub(r'<[^>]+>', '', content)
    
    # Usuń nadmiarowe białe znaki i znaki nowej linii
    content = re.sub(r'\n\s*\n', '\n', content)  # Usuń puste linie
    content = re.sub(r'\s+', ' ', content)  # Zamień wielokrotne spacje na pojedyncze
    content = re.sub(r'\n\s+', '\n', content)  # Usuń spacje na początku linii
    content = re.sub(r'\s+\n', '\n', content)  # Usuń spacje na końcu linii
    
    # Dodaj spacje między słowami, które zostały połączone po usunięciu tagów HTML
    # Znajdź wzorce typu: mała litera + wielka litera (np. "ETYKAtom" -> "ETYKA tom")
    content = re.sub(r'([a-z])([A-Z])', r'\1 \2', content)
    
    # Dodaj spacje między cyframi a literami (np. "ISSN0014" -> "ISSN 0014")
    content = re.sub(r'([A-Z])(\d)', r'\1 \2', content)
    content = re.sub(r'(\d)([A-Z])', r'\1 \2', content)
    
    # Specjalne przypadki - rozdziel konkretne połączenia
    content = re.sub(r'ETYKAtom', 'ETYKA tom', content)
    content = re.sub(r'SPIS TREŚCIOd', 'SPIS TREŚCI Od', content)
    
    # Usuń nadmiarowe spacje po dodaniu nowych
    content = re.sub(r'\s+', ' ', content)

    # Usuń resztki w stylu "Dodano po ...:" jeżeli pozostały po zdjęciu tagów
    content = re.sub(r'Dodano po\s+[^:]+:', '', content, flags=re.IGNORECASE)
    
    return content.strip()


def strip_quotes_from_html(content: str) -> str:
    """
    Usuwa sekcje cytatów z surowego HTML, tak aby ekstrakcja linków
    dotyczyła wyłącznie oryginalnej treści posta (bez cytatów).

    Obsługiwane przypadki (częste w phpBB i podobnych forach):
    - <blockquote> ... </blockquote>
    - <div class="quotewrapper"> ... </div>
    - <div class="quote"> ... </div> i warianty z klasami zawierającymi "quote"
    - <div class="quotetitle"> ... </div> (nagłówki cytatów)
    - <table class="quote"> ... </table>
    """
    if not content:
        return ""

    # Usuń <blockquote> i ich zawartość (iteracyjnie, gdy są zagnieżdżone)
    prev = None
    while prev != content:
        prev = content
        content = re.sub(r'<blockquote[^>]*>.*?</blockquote>', '', content, flags=re.IGNORECASE | re.DOTALL)

    # Usuń segmenty "Dodano po ..." (również gdy zawierają br)
    content = re.sub(r'<span[^>]*>\s*<span[^>]*>\s*Dodano po[\s\S]*?</span>\s*</span>', '', content, flags=re.IGNORECASE)
    content = re.sub(r'<span[^>]*>\s*Dodano po[\s\S]*?</span>', '', content, flags=re.IGNORECASE)

    # Usuń kontenery cytatów (div z klasą zawierającą 'quote') - iteracyjnie, różne warianty klas/atributów
    patterns = [
        r'<div[^>]+class="[^"]*quote[^"]*"[^>]*>.*?</div>',
        r"<div[^>]+class='[^']*quote[^']*'[^>]*>.*?</div>",
        r'<div[^>]*class=[^>]*quote[^>]*>.*?</div>',
        r'<div[^>]*id=[^>]*quote[^>]*>.*?</div>'
    ]
    changed = True
    while changed:
        changed = False
        for pat in patterns:
            new_content = re.sub(pat, '', content, flags=re.IGNORECASE | re.DOTALL)
            if new_content != content:
                content = new_content
                changed = True

    # Usuń nagłówki cytatów
    content = re.sub(r'<div[^>]*class="quotetitle"[^>]*>.*?</div>', '', content, flags=re.IGNORECASE | re.DOTALL)
    # Usuń <cite> i jego zawartość
    content = re.sub(r'<cite[^>]*>.*?</cite>', '', content, flags=re.IGNORECASE | re.DOTALL)

    # Usuń wiodące bloki cytatów, jeśli mimo powyższego coś pozostało na początku
    content = re.sub(r'^(\s*(<blockquote[^>]*>.*?</blockquote>\s*)+)', '', content, flags=re.IGNORECASE | re.DOTALL)
    content = re.sub(r'^(\s*(<div[^>]+class=["\'][^"\']*quote[^"\']*["\'][^>]*>.*?</div>\s*)+)', '', content, flags=re.IGNORECASE | re.DOTALL)

    # Usuń tabele cytatów (rzadsze, starsze motywy)
    content = re.sub(r'<table[^>]+class="[^"]*quote[^"]*"[^>]*>.*?</table>', '', content, flags=re.IGNORECASE | re.DOTALL)

    return content


def normalize_gender(gender):
    """
    Normalizuje wartość płci do jednolitych oznaczeń
    
    Args:
        gender (str): Surowa wartość płci z forum
        
    Returns:
        str: Znormalizowana wartość płci ('M', 'K' lub None)
    """
    if not gender:
        return None
    
    gender_lower = gender.strip().lower()
    
    if gender_lower in ['mężczyzna', 'mezczyzna', 'm']:
        return 'M'
    elif gender_lower in ['kobieta', 'k']:
        return 'K'
    else:
        # Jeśli nie rozpoznajemy wartości, zwracamy None
        return None


def parse_polish_date(date_string):
    """
    Konwertuje polską datę z formatu "Śr mar 16, 2005 11:07 pm" na format "YYYY:MM:DD HH:MM"
    
    Obsługiwane formaty:
    - "Śr mar 16, 2005 11:07 pm" (Radio Katolik)
    - "27 lip 2025, 16:46" (Dolina Modlitwy)
    - "dzisiaj, 8:12"
    - "wczoraj, 17:30"
    
    Args:
        date_string (str): Data w polskim formacie
        
    Returns:
        str: Data w formacie "YYYY-MM-DD HH:MM:SS" lub None jeśli nie udało się sparsować
    """
    if not date_string:
        return None
    
    # Mapowanie polskich nazw miesięcy na numery
    month_mapping = {
        'sty': 1, 'lut': 2, 'mar': 3, 'kwi': 4, 'kwie': 4, 'maj': 5, 'maja': 5, 'cze': 6,
        'lip': 7, 'sie': 8, 'wrz': 9, 'paź': 10, 'lis': 11, 'gru': 12
    }
    
    try:
        date_clean = date_string.strip()
        
        # Sprawdź czy to "dzisiaj" lub "wczoraj"
        if date_clean.startswith('dzisiaj'):
            # Dla "dzisiaj" używamy aktualnej daty
            today = datetime.now()
            time_match = re.search(r'(\d{1,2}):(\d{2})', date_clean)
            if time_match:
                hour, minute = int(time_match.group(1)), int(time_match.group(2))
                return today.replace(hour=hour, minute=minute).strftime('%Y-%m-%d %H:%M:%S')
            return today.strftime('%Y-%m-%d %H:%M:%S')
        elif date_clean.startswith('wczoraj'):
            # Dla "wczoraj" używamy wczorajszej daty
            from datetime import timedelta
            yesterday = datetime.now() - timedelta(days=1)
            time_match = re.search(r'(\d{1,2}):(\d{2})', date_clean)
            if time_match:
                hour, minute = int(time_match.group(1)), int(time_match.group(2))
                return yesterday.replace(hour=hour, minute=minute).strftime('%Y-%m-%d %H:%M:%S')
            return yesterday.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format Radio Katolik: "Śr mar 16, 2005 11:07 pm"
        # Usuń skróty dni tygodnia na początku (Śr, Pt, Pn, So, N, Cz, Wt)
        date_clean = re.sub(r'^(Śr|Pt|Pn|So|N|Cz|Wt)\s+', '', date_clean)
        
        # Parsuj datę używając regex
        # Format: "mar 16, 2005 11:07 pm" lub "maja 08, 2009 5:05 pm"
        pattern = r'(\w+)\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s+(am|pm)'
        match = re.match(pattern, date_clean)
        
        if match:
            month_name, day, year, hour, minute, ampm = match.groups()
            
            # Konwertuj nazwę miesiąca na numer
            month_name_lower = month_name.lower()
            if month_name_lower not in month_mapping:
                return None
            
            month = month_mapping[month_name_lower]
            day = int(day)
            year = int(year)
            hour = int(hour)
            minute = int(minute)
            
            # Konwertuj format 12-godzinny na 24-godzinny
            if ampm.lower() == 'pm' and hour != 12:
                hour += 12
            elif ampm.lower() == 'am' and hour == 12:
                hour = 0
            
            # Utwórz obiekt datetime
            dt = datetime(year, month, day, hour, minute)
            
            # Zwróć w formacie "YYYY-MM-DD HH:MM:SS"
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format Dolina Modlitwy: "27 lip 2025, 16:46" lub "21 maja 2022, 17:58"
        pattern2 = r'(\d{1,2})\s+(\w+)\s+(\d{4}),\s+(\d{1,2}):(\d{2})'
        match = re.match(pattern2, date_clean)
        
        if match:
            day, month_name, year, hour, minute = match.groups()
            
            # Konwertuj nazwę miesiąca na numer
            month_name_lower = month_name.lower()
            if month_name_lower not in month_mapping:
                return None
            
            month = month_mapping[month_name_lower]
            day = int(day)
            year = int(year)
            hour = int(hour)
            minute = int(minute)
            
            # Utwórz obiekt datetime
            dt = datetime(year, month, day, hour, minute)
            
            # Zwróć w formacie "YYYY-MM-DD HH:MM:SS"
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Jeśli żaden wzorzec nie pasuje, zwróć None
        return None
        
    except (ValueError, AttributeError, KeyError) as e:
        # Jeśli wystąpi błąd podczas parsowania, zwróć None
        return None 


def clean_dolina_modlitwy_post_content(content):
    """
    Czyści zawartość postu z forum Dolina Modlitwy z cytatów, URL-i, obrazów i podpisów
    
    Args:
        content (str): Surowa zawartość HTML postu
        
    Returns:
        str: Wyczyściona zawartość tekstowa postu
    """
    if not content:
        return ""
    
    # Usuń cytaty (blockquote z atrybutem cite)
    content = re.sub(r'<blockquote[^>]*>.*?</blockquote>', '', content, flags=re.DOTALL)
    
    # Usuń podpisy (div z id="sig..." i class="signature")
    content = re.sub(r'<div id="sig[^"]*" class="signature">.*?</div>', '', content, flags=re.DOTALL)
    
    # Usuń elementy notice (informacje o edycji postu)
    content = re.sub(r'<div class="notice">.*?</div>', '', content, flags=re.DOTALL)
    
    # Usuń elementy biblia (specjalne formatowanie cytatów biblijnych)
    # Usuń wszystko od <span class="biblia"> do końca (niepoprawny HTML)
    content = re.sub(r'<span class="biblia"[^>]*>.*$', '', content, flags=re.DOTALL)
    
    # Usuń obrazy
    content = re.sub(r'<img[^>]*>', '', content)
    
    # Usuń linki (zachowaj tekst)
    content = re.sub(r'<a[^>]*>(.*?)</a>', r'\1', content)
    
    # Usuń adresy URL w tekście (http/https/www)
    content = re.sub(r'https?://[^\s<>"]+', '', content)
    content = re.sub(r'www\.[^\s<>"]+', '', content)
    
    # Zamień tagi <br> na znaki nowej linii
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    
    # Zamień tagi <div> i <span> na znaki nowej linii (aby rozdzielić bloki tekstu)
    content = re.sub(r'</div>', '\n', content)
    content = re.sub(r'</span>', '\n', content)
    content = re.sub(r'<div[^>]*>', '', content)
    content = re.sub(r'<span[^>]*>', '', content)
    
    # Usuń inne tagi HTML, zachowaj tekst
    content = re.sub(r'<[^>]+>', '', content)
    
    # Usuń nadmiarowe białe znaki i znaki nowej linii
    content = re.sub(r'\n\s*\n', '\n', content)  # Usuń puste linie
    content = re.sub(r'\s+', ' ', content)  # Zamień wielokrotne spacje na pojedyncze
    content = re.sub(r'\n\s+', '\n', content)  # Usuń spacje na początku linii
    content = re.sub(r'\s+\n', '\n', content)  # Usuń spacje na końcu linii
    
    return content.strip() 


def extract_urls_from_html(content: str, base_url: str = None):
    """
    Ekstrahuje listę adresów URL z surowego HTML posta.
    - Zwraca listę unikalnych URL-i w kolejności pierwszego wystąpienia
    - Obsługuje href w znacznikach <a> oraz nagie adresy (http/https/www)
    - Jeśli podany jest base_url, względne href-y są rozwijane do absolutnych

    Args:
        content (str): Surowa zawartość HTML
        base_url (str, optional): Bazowy URL do rozwijania linków względnych

    Returns:
        list[str]: Lista URL-i znalezionych w treści
    """
    if not content:
        return []

    urls = []
    seen = set()
    base_host = None
    try:
        if base_url:
            base_host = urlparse(base_url).netloc.lower()
    except Exception:
        base_host = None

    # 1) href-y z <a>
    try:
        hrefs = re.findall(r'<a[^>]+href=["\'](.*?)["\']', content, flags=re.IGNORECASE)
        for href in hrefs:
            link = href.strip()
            if base_url and (link.startswith('/') or link.startswith('./') or link.startswith('../')):
                try:
                    link = urljoin(base_url, link)
                except Exception:
                    pass
            # Pomiń kotwice i schematy nie-HTTP
            if not link or link.startswith('#') or link.startswith('mailto:'):
                continue
            # Pomiń linki wewnętrzne (ten sam host co base_url)
            try:
                parsed_link = urlparse(link)
                link_host = parsed_link.netloc.lower()
                if base_host and link_host == base_host:
                    continue
            except Exception:
                pass
            if link and link not in seen:
                seen.add(link)
                urls.append(link)
    except Exception:
        pass

    # 2) Nagie URL-e (http/https/www)
    try:
        naked = re.findall(r'(https?://[^\s<>"\)\]]+|www\.[^\s<>"\)\]]+)', content, flags=re.IGNORECASE)
        for link in naked:
            link = link.strip()
            if not link:
                continue
            # Rozwiń względne (dla porządku) i odfiltruj wewnętrzne
            try:
                parsed_link = urlparse(link)
                link_host = parsed_link.netloc.lower()
                if base_host and link_host == base_host:
                    continue
            except Exception:
                pass
            # Pomiń duplikaty będące prefiksami/skrótami istniejących hrefów
            is_prefix_duplicate = False
            for existing in urls:
                if existing.startswith(link) or link.startswith(existing):
                    is_prefix_duplicate = True
                    break
            if is_prefix_duplicate:
                continue
            if link not in seen:
                seen.add(link)
                urls.append(link)
    except Exception:
        pass

    return urls
