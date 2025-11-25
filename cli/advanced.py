"""Minimalne CLI do scrapowania forów i zapisu do SQLite.

Bez analiz językowych/statystycznych i bez tygodników.
"""

from __future__ import annotations

import os
import subprocess
from enum import Enum
from pathlib import Path
from typing import Annotated, Dict, List, Optional
from datetime import datetime

import sqlite3
import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table


console = Console()
app = typer.Typer(
    name="forums-scraper",
    help="🕷️ Minimalny scraper forów religijnych (bez analiz)",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
)


class ForumName(str, Enum):
    """Dostępne fora do scrapowania."""
    DOLINA_MODLITWY = "dolina_modlitwy"
    RADIO_KATOLIK = "radio_katolik"
    WIARA = "wiara"
    Z_CHRYSTUSEM = "z_chrystusem"
    ALL = "all"


FORUM_SPIDER_MAP: Dict[ForumName, str] = {
    ForumName.DOLINA_MODLITWY: "dolina_modlitwy",
    ForumName.RADIO_KATOLIK: "radio_katolik",
    ForumName.WIARA: "wiara",
    ForumName.Z_CHRYSTUSEM: "z_chrystusem",
}


def _parse_key_value_args(arg_str: Optional[str]) -> List[str]:
    """Parsuje łańcuch "k=v,k2=v2" na listę argumentów scrapy ["-a","k=v",...]."""
    args: List[str] = []
    if not arg_str:
        return args
    for part in arg_str.split(','):
        part = part.strip()
        if not part or '=' not in part:
            continue
        k, v = part.split('=', 1)
        args.extend(["-a", f"{k.strip()}={v.strip()}"])
    return args


def display_forum_summary(forums: List[ForumName]) -> None:
    """Wyświetla podsumowanie wybranych forów."""
    table = Table(title="🏛️ Wybrane fora", show_header=True, header_style="bold blue")
    table.add_column("Forum", style="cyan")
    table.add_column("Spider", style="white")
    table.add_column("Opis", style="green")

    forum_descriptions = {
        ForumName.DOLINA_MODLITWY: "Forum katolickie - Dolina Modlitwy",
        ForumName.RADIO_KATOLIK: "Forum Radia Katolik",
        ForumName.WIARA: "Forum Wiara.pl",
        ForumName.Z_CHRYSTUSEM: "Forum Z Chrystusem",
    }

    for forum in forums:
        if forum == ForumName.ALL:
            continue
        spider = FORUM_SPIDER_MAP[forum]
        desc = forum_descriptions.get(forum, "Brak opisu")
        table.add_row(forum.value, spider, desc)

    console.print(table)


@app.command(name="scrape")
def scrape_forums(
    forums: Annotated[List[ForumName], typer.Option(
        "--forum", "-f",
        help="Wybierz fora do scrapowania (można wybrać wiele)"
    )] = [ForumName.ALL],
    output_dir: Annotated[Path, typer.Option(
        "--output", "-o",
        help="Katalog wyjściowy dla baz danych"
    )] = Path("data/databases"),
    concurrent_requests: Annotated[int, typer.Option(
        "--concurrent",
        help="Liczba równoległych żądań",
        min=1, max=64
    )] = 16,
    download_delay: Annotated[float, typer.Option(
        "--delay",
        help="Opóźnienie między żądaniami (sekundy)",
        min=0.0, max=10.0
    )] = 0.5,
    autothrottle: Annotated[Optional[bool], typer.Option(
        "--autothrottle/--no-autothrottle",
        help="Włącz/wyłącz AutoThrottle (domyślnie wg ustawień Scrapy)"
    )] = None,
    spider_args: Annotated[Optional[str], typer.Option(
        "--spider-args",
        help="Dodatkowe argumenty spidera w formacie 'k=v,k2=v2' (np. only_thread_url=...)"
    )] = None,
    dry_run: Annotated[bool, typer.Option(
        "--dry-run",
        help="Tylko pokaż co zostanie wykonane, nie uruchamiaj"
    )] = False,
    verbose: Annotated[bool, typer.Option(
        "--verbose", "-v",
        help="Szczegółowe logowanie"
    )] = False,
    yes: Annotated[bool, typer.Option(
        "--yes", "-y",
        help="Nie pytaj o potwierdzenie"
    )] = False,
):
    """🕷️ Scrapuj wybrane fora i zapisz do wspólnej bazy SQLite."""

    console.print(Panel.fit(
        "🕷️ [bold blue]Forums Scraper[/bold blue] - Scrapowanie forów religijnych",
        style="blue",
    ))

    # Rozwiń 'all' na wszystkie fora
    if ForumName.ALL in forums:
        forums = [f for f in ForumName if f != ForumName.ALL]

    # Wyświetl podsumowanie forów
    display_forum_summary(forums)

    # Utwórz katalog wyjściowy
    output_dir.mkdir(parents=True, exist_ok=True)

    # Wyświetl plan działania
    unified_db_path = output_dir / "forums_unified.db"
    console.print("\n🎯 [bold]Plan działania:[/bold]")
    console.print(f"📊 [bold]Wspólna baza danych:[/bold] [yellow]{unified_db_path}[/yellow]")
    for i, forum in enumerate(forums, 1):
        spider_name = FORUM_SPIDER_MAP[forum]
        console.print(f"  {i}. [cyan]{forum.value}[/cyan] (spider: {spider_name})")

    if dry_run:
        console.print("\n[yellow]🔍 Tryb dry-run - nie wykonuję scrapowania[/yellow]")
        return

    # Potwierdź wykonanie
    if not yes and not typer.confirm("\n❓ Czy chcesz kontynuować?"):
        console.print("[red]❌ Anulowano[/red]")
        return

    console.print("\n🚀 [bold green]Rozpoczynam scrapowanie...[/bold green]")

    total_forums = len(forums)
    failed_forums: List[str] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        main_task = progress.add_task("Scrapowanie forów", total=total_forums)

        for idx, forum in enumerate(forums, start=1):
            spider_name = FORUM_SPIDER_MAP[forum]

            progress.update(
                main_task,
                description=f"Forum: [cyan]{forum.value}[/cyan] ({idx}/{total_forums})",
            )

            try:
                scrapy_args = [
                    "scrapy",
                    "crawl",
                    spider_name,
                    "-s",
                    f"SQLITE_DATABASE_PATH={unified_db_path}",
                    "-s",
                    f"CONCURRENT_REQUESTS={concurrent_requests}",
                    "-s",
                    f"DOWNLOAD_DELAY={download_delay}",
                ]

                if autothrottle is not None:
                    scrapy_args.extend([
                        "-s",
                        f"AUTOTHROTTLE_ENABLED={'1' if autothrottle else '0'}",
                    ])

                scrapy_args += _parse_key_value_args(spider_args)

                scrapy_args.extend([
                    "-s",
                    f"LOG_LEVEL={'INFO' if verbose else 'WARNING'}",
                ])

                console.print(f"▶️  Uruchamiam: [dim]{' '.join(scrapy_args)}[/dim]")

                project_root = Path(__file__).resolve().parents[1]
                env = os.environ.copy()
                env.setdefault("SCRAPY_SETTINGS_MODULE", "forums_scraper.settings")

                rc = subprocess.call(scrapy_args, cwd=str(project_root), env=env)
                if rc != 0:
                    raise subprocess.CalledProcessError(rc, scrapy_args)

                console.print(f"✅ [green]{forum.value} - zakończono pomyślnie[/green]")

            except subprocess.CalledProcessError as e:
                console.print(f"❌ [red]{forum.value} - błąd: {e}[/red]")
                failed_forums.append(forum.value)
            except KeyboardInterrupt:
                console.print("\n[yellow]⏹️  Przerwano przez użytkownika[/yellow]")
                break

            progress.advance(main_task)

    console.print("\n🎉 [bold green]Scrapowanie zakończone![/bold green]")

    successful_count = total_forums - len(failed_forums)
    console.print(f"✅ Pomyślnie: [green]{successful_count}/{total_forums}[/green]")

    if failed_forums:
        console.print(f"❌ Niepowodzenia: [red]{len(failed_forums)}[/red]")
        for name in failed_forums:
            console.print(f"   • {name}")

    if unified_db_path.exists():
        size_mb = unified_db_path.stat().st_size / 1024 / 1024
        console.print("\n📊 [bold]Wspólna baza danych:[/bold]")
        console.print(
            f"   📁 [cyan]{unified_db_path.name}[/cyan] ([yellow]{size_mb:.1f} MB[/yellow])"
        )


@app.command(name="list-forums")
def list_forums() -> None:
    """📋 Wyświetl dostępne fora (spidery forów)."""
    console.print("🏛️  [bold]Dostępne fora:[/bold]")
    for forum, spider in FORUM_SPIDER_MAP.items():
        if forum == ForumName.ALL:
            continue
        console.print(f"   • [cyan]{forum.value}[/cyan] (spider: {spider})")


@app.command(name="status")
def show_status(
    database_path: Annotated[Path, typer.Option(
        "--database", "-d",
        help="Ścieżka do bazy danych"
    )] = Path("data/databases/forums_unified.db")
):
    """📊 Pokaż status wspólnej bazy danych i statystyki."""
    
    console.print("📊 [bold]Status wspólnej bazy danych[/bold]")
    
    if not database_path.exists():
        console.print(f"[yellow]Baza danych nie istnieje: {database_path}[/yellow]")
        console.print("💡 Uruchom scrapowanie: [cyan]uv run cli scrape[/cyan]")
        return
    
    # Podstawowe informacje o pliku
    size_mb = database_path.stat().st_size / 1024 / 1024
    mtime = datetime.fromtimestamp(database_path.stat().st_mtime)
    
    console.print(f"📁 [cyan]{database_path.name}[/cyan]")
    console.print(f"📏 Rozmiar: [yellow]{size_mb:.1f} MB[/yellow]")
    console.print(f"🕒 Ostatnia modyfikacja: [green]{mtime.strftime('%Y-%m-%d %H:%M:%S')}[/green]")
    
    # Statystyki z bazy danych
    try:
        with sqlite3.connect(database_path) as conn:
            cursor = conn.cursor()

            # Statystyki główne
            cursor.execute("SELECT COUNT(*) FROM forums")
            forums_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM posts")
            posts_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users")
            users_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM threads")
            threads_count = cursor.fetchone()[0]

            main_table = Table(title="📈 Główne statystyki", show_header=True)
            main_table.add_column("Kategoria", style="cyan")
            main_table.add_column("Liczba", style="yellow")

            main_table.add_row("Fora", str(forums_count))
            main_table.add_row("Posty", str(posts_count))
            main_table.add_row("Użytkownicy", str(users_count))
            main_table.add_row("Wątki", str(threads_count))

            console.print(main_table)

            # Statystyki per forum
            cursor.execute(
                """
                SELECT f.spider_name, f.title,
                       COUNT(DISTINCT p.id) AS posts_count,
                       COUNT(DISTINCT u.id) AS users_count,
                       COUNT(DISTINCT t.id) AS threads_count
                FROM forums f
                LEFT JOIN sections s ON f.id = s.forum_id
                LEFT JOIN threads t ON s.id = t.section_id
                LEFT JOIN posts p ON t.id = p.thread_id
                LEFT JOIN users u ON p.user_id = u.id
                GROUP BY f.id, f.spider_name, f.title
                ORDER BY posts_count DESC
                """
            )
            forum_results = cursor.fetchall()

            if forum_results:
                forum_table = Table(title="📊 Statystyki per forum", show_header=True)
                forum_table.add_column("Forum", style="cyan")
                forum_table.add_column("Posty", style="yellow")
                forum_table.add_column("Użytkownicy", style="green")
                forum_table.add_column("Wątki", style="magenta")

                for spider_name, title, posts, users, threads in forum_results:
                    forum_table.add_row(
                        spider_name or "Nieznane",
                        str(posts),
                        str(users),
                        str(threads),
                    )

                console.print(forum_table)

    except sqlite3.Error as e:
        console.print(f"[red]Błąd podczas odczytu bazy danych: {e}[/red]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Analiza postów", total=total_to_do)
        offset = 0
        while processed < total_to_do:
            remaining = total_to_do - processed
            take = min(batch_size, remaining)
            batch_sql = f"""
                SELECT p.id, p.content, p.content_urls, p.url
                FROM posts p
                LEFT JOIN threads t ON t.id = p.thread_id
                LEFT JOIN sections s ON s.id = t.section_id
                LEFT JOIN forums f ON f.id = s.forum_id
                WHERE {where_missing}{where_forum}
                ORDER BY p.id
                LIMIT ?
            """
            cur.execute(batch_sql, ([take] if not forum else [forum, take]))
            rows = cur.fetchall()
            if not rows:
                break
            fetched += len(rows)
            # Przygotuj itemy
            items: List[Dict[str, any]] = []
            id_by_idx: List[int] = []
            for r in rows:
                cid = int(r["id"])  # post_id
                content = r["content"] or ""
                try:
                    cu = r["content_urls"]
                    if isinstance(cu, str):
                        content_urls = _json.loads(cu)
                    else:
                        content_urls = cu or []
                except Exception:
                    content_urls = []
                items.append({
                    "id": cid,
                    "content": content,
                    "content_urls": content_urls,
                    "url": r["url"],
                })
                id_by_idx.append(cid)

            # Uruchom analizy na batchu
            results_list = _aio.run(runner.run_all_batch(items))

            # Zapisz wyniki (liniowo; bulk gdzie się da)
            write_cur = conn.cursor()
            import re as _re_tok
            for item, results in zip(items, results_list):
                post_id = item["id"]
                # tokens (pomiń jeśli zapisujemy lingwistykę, żeby nie duplikować)
                tokens = results.get('tokens')
                token_stats = results.get('token_stats')
                if isinstance(token_stats, dict):
                    write_cur.execute(
                        'INSERT OR REPLACE INTO post_token_stats (post_id, total_tokens, unique_tokens, avg_token_length) VALUES (?, ?, ?, ?)',
                        (post_id, token_stats.get('total_tokens'), token_stats.get('unique_tokens'), token_stats.get('avg_token_length'))
                    )
                # Fallback: jeśli brak wyników tokenów, zrób prostą tokenizację
                if not isinstance(tokens, list) and not isinstance(token_stats, dict):
                    _toks = _re_tok.findall(r'\b\w+\b', (item.get('content') or '').lower())
                    if _toks:
                        write_cur.execute('DELETE FROM post_tokens WHERE post_id=?', (post_id,))
                        write_cur.executemany(
                            'INSERT INTO post_tokens (post_id, token, position) VALUES (?, ?, ?)',
                            [(post_id, str(tok), i) for i, tok in enumerate(_toks)]
                        )
                        _uniq = set(_toks)
                        _avg = sum(len(t) for t in _toks) / len(_toks) if _toks else 0
                        write_cur.execute(
                            'INSERT OR REPLACE INTO post_token_stats (post_id, total_tokens, unique_tokens, avg_token_length) VALUES (?, ?, ?, ?)',
                            (post_id, len(_toks), len(_uniq), _avg)
                        )
                # linguistic
                ling = results.get('linguistic')
                if isinstance(ling, list):
                    import json as _json2
                    write_cur.execute('DELETE FROM post_linguistic_analysis WHERE post_id=?', (post_id,))
                    for tok in ling:
                        write_cur.execute('''
                            INSERT INTO post_linguistic_analysis 
                            (post_id, token, lemma, pos, tag, dep, morph_features, is_alpha, is_stop, is_punct)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                                post_id, tok.get('token'), tok.get('lemma'), tok.get('pos'), tok.get('tag'), tok.get('dep'),
                                _json2.dumps(tok.get('morph_features', {})), tok.get('is_alpha'), tok.get('is_stop'), tok.get('is_punct')
                            ))
                    # Jeśli mamy lingwistykę – nie zapisuj post_tokens
                    tokens = None
                ling_stats = results.get('linguistic_stats')
                if isinstance(ling_stats, dict):
                    write_cur.execute('''
                        INSERT OR REPLACE INTO post_linguistic_stats 
                        (post_id, sentence_count, word_count, char_count, avg_sentence_length, readability_score, sentiment_polarity, sentiment_subjectivity, language_detected)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                            post_id,
                            ling_stats.get('sentence_count'), ling_stats.get('word_count'), ling_stats.get('char_count'), ling_stats.get('avg_sentence_length'),
                            ling_stats.get('readability_score'), ling_stats.get('sentiment_polarity'), ling_stats.get('sentiment_subjectivity'), ling_stats.get('language_detected')
                        ))
                # URL analysis
                url_analysis = results.get('url_analysis')
                if isinstance(url_analysis, dict):
                    categorized = url_analysis.get('categorized_urls', []) or []
                    domain_categories = url_analysis.get('domain_categories', {}) or {}
                    write_cur.execute('DELETE FROM post_urls WHERE post_id=?', (post_id,))
                    # domeny cache
                    domain_cache: Dict[str, int] = {}
                    for u in categorized:
                        dom = u.get('domain')
                        if not dom:
                            continue
                        if dom not in domain_cache:
                            write_cur.execute('SELECT id FROM domains WHERE domain=?', (dom,))
                            r = write_cur.fetchone()
                            if r:
                                domain_cache[dom] = int(r[0])
                                write_cur.execute('UPDATE domains SET last_seen=CURRENT_TIMESTAMP, total_references=total_references+1 WHERE id=?', (domain_cache[dom],))
                            else:
                                info = domain_categories.get(dom, {})
                                write_cur.execute('''INSERT INTO domains (domain, category, is_religious, is_media, is_social, is_educational, trust_score, total_references)
                                                     VALUES (?, ?, ?, ?, ?, ?, ?, 1)''', (
                                    dom, info.get('category', 'unknown'), info.get('is_religious', False), info.get('is_media', False),
                                    info.get('is_social', False), info.get('is_educational', False), info.get('trust_score', 0.5)
                                ))
                                domain_cache[dom] = write_cur.lastrowid
                    if categorized:
                        write_cur.executemany('''INSERT INTO post_urls (post_id, url, domain_id, url_type, is_external)
                                                 VALUES (?, ?, ?, ?, ?)''', [
                            (post_id, u.get('url'), domain_cache.get(u.get('domain')), u.get('url_type', 'unknown'), u.get('is_external', True))
                            for u in categorized if u.get('domain')
                        ])
                    dstats = url_analysis.get('domain_stats', {}) or {}
                    write_cur.execute('''
                        INSERT OR REPLACE INTO post_url_stats (post_id, total_urls, unique_domains, religious_urls, media_urls, social_urls, educational_urls, unknown_urls)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
                            post_id, url_analysis.get('total_urls', 0), dstats.get('total_domains', 0), dstats.get('religious_domains', 0), dstats.get('media_domains', 0),
                            dstats.get('social_domains', 0), dstats.get('educational_domains', 0), dstats.get('unknown_domains', 0)
                        ))
                # NER
                ents = results.get('named_entities')
                if isinstance(ents, list):
                    write_cur.execute('DELETE FROM post_named_entities WHERE post_id=?', (post_id,))
                    counts = {'total': 0, 'person': 0, 'org': 0, 'gpe': 0, 'event': 0, 'other': 0}
                    for ent in ents:
                        if not isinstance(ent, dict):
                            continue
                        text = (ent.get('text') or '').strip()
                        if not text:
                            continue
                        label = ent.get('label', 'OTHER')
                        write_cur.execute('''INSERT INTO post_named_entities (post_id, entity_text, entity_label, entity_description, start_char, end_char)
                                             VALUES (?, ?, ?, ?, ?, ?)''', (
                            post_id, text, label, ent.get('description', ''), ent.get('start', 0), ent.get('end', 0)
                        ))
                        counts['total'] += 1
                        if label in ['PERSON', 'PER']:
                            counts['person'] += 1
                        elif label in ['ORG', 'ORGANIZATION']:
                            counts['org'] += 1
                        elif label in ['GPE', 'LOC', 'LOCATION']:
                            counts['gpe'] += 1
                        elif label in ['EVENT']:
                            counts['event'] += 1
                        else:
                            counts['other'] += 1
                    write_cur.execute('''INSERT OR REPLACE INTO post_ner_stats (post_id, total_entities, person_entities, org_entities, gpe_entities, event_entities, other_entities)
                                         VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                        post_id, counts['total'], counts['person'], counts['org'], counts['gpe'], counts['event'], counts['other']
                    ))

            conn.commit()
            processed += len(rows)
            progress.update(task, completed=processed)

    # Zamknij runner
    _aio.run(runner.close())
    console.print("✅ [green]Analiza DB zakończona[/green]")


def run():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    run()
