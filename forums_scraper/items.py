# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime


class ForumItem(scrapy.Item):
    """Model dla forum"""
    id = scrapy.Field()
    spider_name = scrapy.Field()
    title = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ForumSectionItem(scrapy.Item):
    """Model dla sekcji forum"""
    id = scrapy.Field()
    forum_id = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ForumThreadItem(scrapy.Item):
    """Model dla wątków forum"""
    id = scrapy.Field()
    section_id = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    author = scrapy.Field()
    replies = scrapy.Field()
    views = scrapy.Field()
    last_post_date = scrapy.Field()
    last_post_author = scrapy.Field()
    section_url = scrapy.Field()  # URL sekcji (używane w pipeline)
    section_title = scrapy.Field()  # Tytuł sekcji (używane w pipeline)
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ForumUserItem(scrapy.Item):
    """Model dla użytkowników forum"""
    id = scrapy.Field()
    username = scrapy.Field()
    join_date = scrapy.Field()
    posts_count = scrapy.Field()
    religion = scrapy.Field()  # Religia użytkownika
    gender = scrapy.Field()    # Płeć użytkownika
    localization = scrapy.Field()  # Lokalizacja (miasto/kraj) użytkownika
    created_at = scrapy.Field()
    updated_at = scrapy.Field()


class ForumPostItem(scrapy.Item):
    """Model dla postów forum"""
    id = scrapy.Field()
    thread_id = scrapy.Field()
    user_id = scrapy.Field()
    post_number = scrapy.Field()
    content = scrapy.Field()
    content_urls = scrapy.Field()  # Lista URL-i występujących w treści (po czyszczeniu usuwane z content)
    post_date = scrapy.Field()
    url = scrapy.Field()  # URL bezpośredni do posta
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
    username = scrapy.Field() # Używane tylko w pipeline do mapowania na user_id
