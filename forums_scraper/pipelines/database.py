"""
SQLite pipeline dla zapisywania danych forum wraz z analizami, ze spójnymi
kluczami numerycznymi i kluczami obcymi między tabelami.
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime

import scrapy


class SQLitePipeline:
    """Minimalny pipeline SQLite – tylko tabele danych forum.

    Przechowuje fora, sekcje, wątki, użytkowników i posty. Wszystkie
    dawne tabele analityczne (tokeny, lingwistyka, URL-e, NER) zostały
    usunięte ze schematu i z logiki zapisu.
    """

    def __init__(self) -> None:
        self.connection: Optional[sqlite3.Connection] = None
        self.db_path: Optional[Path] = None
        # cache do szybszego mapowania URL-i na ID
        self.forum_url_to_id: Dict[str, int] = {}
        self.forum_name_to_id: Dict[str, int] = {}
        self.section_url_to_id: Dict[str, int] = {}
        self.thread_url_to_id: Dict[str, int] = {}
        self.user_name_to_id: Dict[str, int] = {}

    @classmethod
    def from_crawler(cls, crawler: scrapy.crawler.Crawler) -> "SQLitePipeline":
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    # --- lifecycle ---

    def open_spider(self, spider: scrapy.Spider) -> None:
        """Otwiera połączenie SQLite i tworzy tabele, jeśli trzeba."""
        settings = getattr(self.crawler, "settings", {})
        db_path_str = settings.get("SQLITE_DATABASE_PATH", "data/databases/forums_unified.db")
        self.db_path = Path(db_path_str)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(str(self.db_path))
        self.connection.execute("PRAGMA foreign_keys = ON;")
        self._create_tables()
        logging.getLogger(__name__).info("Połączono z bazą SQLite: %s", self.db_path)

    def close_spider(self, spider: scrapy.Spider) -> None:
        if self.connection is not None:
            self.connection.commit()
            self.connection.close()
            self.connection = None

    # --- schema ---

    def _create_tables(self) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        # Tabela forów
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS forums (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spider_name TEXT NOT NULL,
                title TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Tabela sekcji
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forum_id INTEGER,
                title TEXT,
                url TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Tabela wątków
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_id INTEGER NOT NULL,
                title TEXT,
                url TEXT UNIQUE,
                author TEXT,
                replies INTEGER,
                views INTEGER,
                last_post_date TEXT,
                last_post_author TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (section_id) REFERENCES sections (id) ON DELETE CASCADE
            )
            """
        )

        # Tabela użytkowników
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE,
                join_date TEXT,
                posts_count INTEGER,
                religion TEXT,
                gender TEXT,
                localization TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Tabela postów
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER,
                user_id INTEGER,
                post_number INTEGER,
                content TEXT,
                content_urls TEXT,
                post_date TEXT,
                url TEXT,
                username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- Relacje FK do threads/users są świadomie pominięte,
                -- żeby nie blokować zapisu postów, gdy brak wątku/użytkownika.
                UNIQUE (thread_id, post_number)
            )
            """
        )

        # Indeksy
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_thread_id ON posts(thread_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)")

        self.connection.commit()

    # --- processing ---

    def process_item(self, item: Any, spider: scrapy.Spider) -> Any:
        if self.connection is None:
            raise RuntimeError("SQLite connection is not initialized")

        # Import lokalny, żeby uniknąć cyklu przy imporcie modułów
        from forums_scraper.items import (
            ForumItem,
            ForumSectionItem,
            ForumThreadItem,
            ForumUserItem,
            ForumPostItem,
        )

        if isinstance(item, ForumItem):
            self._save_forum(item)
        elif isinstance(item, ForumSectionItem):
            self._save_section(item)
        elif isinstance(item, ForumThreadItem):
            self._save_thread(item)
        elif isinstance(item, ForumUserItem):
            self._save_user(item)
        elif isinstance(item, ForumPostItem):
            self._save_post(item)

        return item

    # --- helpers ---

    def _save_forum(self, item: Any) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        cursor.execute(
            """
            INSERT INTO forums (spider_name, title, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                item.get("spider_name"),
                item.get("title"),
                item.get("created_at") or datetime.utcnow().isoformat(),
                item.get("updated_at") or datetime.utcnow().isoformat(),
            ),
        )
        forum_id = cursor.lastrowid
        self.connection.commit()

        # cache po nazwie spidera i ewentualnie URL, jeśli spidery przekazują identyfikujący URL
        spider_name = item.get("spider_name")
        if spider_name:
            self.forum_name_to_id[str(spider_name)] = int(forum_id)

        forum_url = item.get("url")
        if forum_url:
            self.forum_url_to_id[str(forum_url)] = int(forum_id)

    def _save_section(self, item: Any) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        forum_id = item.get("forum_id")
        if isinstance(forum_id, str):
            # Najpierw spróbuj potraktować to jako nazwę spidera
            mapped = self.forum_name_to_id.get(forum_id)
            if mapped is not None:
                forum_id = mapped
            else:
                # ewentualnie jako URL forum
                forum_id = self.forum_url_to_id.get(forum_id)

        cursor.execute(
            """
            INSERT OR IGNORE INTO sections (forum_id, title, url, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                forum_id,
                item.get("title"),
                item.get("url"),
                item.get("created_at") or datetime.utcnow().isoformat(),
                item.get("updated_at") or datetime.utcnow().isoformat(),
            ),
        )
        # jeśli istniała, pobierz id
        if cursor.lastrowid:
            section_id = cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM sections WHERE url=?", (item.get("url"),))
            row = cursor.fetchone()
            section_id = row[0] if row else None

        self.connection.commit()

        if item.get("url") and section_id is not None:
            self.section_url_to_id[str(item.get("url"))] = int(section_id)

    def _save_thread(self, item: Any) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        # Ustal section_id. Preferuj bezpośrednie ID, w przeciwnym razie użyj section_url
        section_id = item.get("section_id")
        if not section_id:
            section_url = item.get("section_url")
            section_title = item.get("section_title")

            if section_url:
                # Spróbuj znaleźć istniejącą sekcję po URL
                cursor.execute("SELECT id FROM sections WHERE url=?", (section_url,))
                row = cursor.fetchone()
                if row:
                    section_id = row[0]
                else:
                    # Utwórz minimalną sekcję z NULL forum_id
                    cursor.execute(
                        """
                        INSERT INTO sections (forum_id, title, url, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            None,
                            section_title,
                            section_url,
                            item.get("created_at") or datetime.utcnow().isoformat(),
                            item.get("updated_at") or datetime.utcnow().isoformat(),
                        ),
                    )
                    section_id = cursor.lastrowid

            # Zaktualizuj cache URL->ID jeśli mamy dane
            if section_url and section_id:
                self.section_url_to_id[str(section_url)] = int(section_id)
        elif isinstance(section_id, str):
            # Jeśli section_id jest stringiem, potraktuj go jako URL i spróbuj zmapować
            mapped = self.section_url_to_id.get(section_id)
            if mapped is not None:
                section_id = mapped

        cursor.execute(
            """
            INSERT OR IGNORE INTO threads (
                section_id, title, url, author, replies, views,
                last_post_date, last_post_author, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                section_id,
                item.get("title"),
                item.get("url"),
                item.get("author"),
                item.get("replies"),
                item.get("views"),
                item.get("last_post_date"),
                item.get("last_post_author"),
                item.get("created_at") or datetime.utcnow().isoformat(),
                item.get("updated_at") or datetime.utcnow().isoformat(),
            ),
        )

        if cursor.lastrowid:
            thread_id = cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM threads WHERE url=?", (item.get("url"),))
            row = cursor.fetchone()
            thread_id = row[0] if row else None

        self.connection.commit()

        if item.get("url") and thread_id is not None:
            self.thread_url_to_id[str(item.get("url"))] = int(thread_id)

    def _save_user(self, item: Any) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        cursor.execute(
            """
            INSERT OR IGNORE INTO users (
                username, join_date, posts_count, religion, gender, localization,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.get("username"),
                item.get("join_date"),
                item.get("posts_count"),
                item.get("religion"),
                item.get("gender"),
                item.get("localization"),
                item.get("created_at") or datetime.utcnow().isoformat(),
                item.get("updated_at") or datetime.utcnow().isoformat(),
            ),
        )

        if cursor.lastrowid:
            user_id = cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM users WHERE username=?", (item.get("username"),))
            row = cursor.fetchone()
            user_id = row[0] if row else None

        self.connection.commit()

        if item.get("username") and user_id is not None:
            self.user_name_to_id[str(item.get("username"))] = int(user_id)

    def _save_post(self, item: Any) -> None:
        assert self.connection is not None
        cursor = self.connection.cursor()

        # thread_id może być ID (jako int lub string)
        thread_id = item.get("thread_id")
        if isinstance(thread_id, str) and thread_id.isdigit():
            thread_id = int(thread_id)

        # user_id może być ID lub username – spróbuj zmapować
        user_id = item.get("user_id")
        username = item.get("username")
        if not user_id and username:
            user_id = self.user_name_to_id.get(str(username))

        content_urls = item.get("content_urls")
        if isinstance(content_urls, (list, tuple)):
            content_urls_str = json.dumps(list(content_urls), ensure_ascii=False)
        else:
            content_urls_str = None

        cursor.execute(
            """
            INSERT OR REPLACE INTO posts (
                thread_id, user_id, post_number, content, content_urls,
                post_date, url, username, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                user_id,
                item.get("post_number"),
                item.get("content"),
                content_urls_str,
                item.get("post_date"),
                item.get("url"),
                username,
                item.get("created_at") or datetime.utcnow().isoformat(),
                item.get("updated_at") or datetime.utcnow().isoformat(),
            ),
        )

        self.connection.commit()

