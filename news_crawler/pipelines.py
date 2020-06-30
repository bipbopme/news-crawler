from elasticsearch import Elasticsearch
import logging
import uuid
import datetime
import hashlib

class NewsCrawlerPipeline:
    def __init__(self):
        self.es = Elasticsearch()
        self.batch_id = uuid.uuid4().hex
        self.batch_started_at = datetime.datetime.now()
        self.link_count_by_source = {}
        self.link_count = 0
        
    def close_spider(self, spider):
        batch = {
            'id': self.batch_id,
            'started_at': self.batch_started_at,
            'ended_at': datetime.datetime.now(),
            'link_count': self.link_count,
            'link_count_by_source': self.link_count_by_source,
            'stats': spider.crawler.stats.get_stats()
        }

        self.es.index(index="batches", id=self.batch_id, body=batch)

    def process_item(self, item, spider):
        link_id = uuid.uuid4().hex
        article_id = hashlib.sha384((item['canonical_url'] or item['url']).encode()).hexdigest()
        source_id = item['source_id']
        crawled_at = datetime.datetime.now()

        link = {
            'id': link_id,
            'articleId': article_id,
            'sourceId': source_id,
            'categoryId': item['category_id'],
            'batchId': self.batch_id,
            'batchStartedAt': self.batch_started_at,
            'crawledAt': crawled_at,
            'position': item['position'],
            'title': item['title'],
            'description': item['description'],
            'url': item['url']
        }

        article = {
            'id': article_id,
            'sourceId': source_id,
            'categoryIds': [item['category_id']],
            'title': item['title'],
            'authors': item['authors'],
            'description': item['description'],
            'text': item['text'],
            'image': item['image'],
            'publishDate': item['publish_date'],
            'url': item['url'],
            'canonicalUrl': item['canonical_url'],
            'ampUrl': item['amp_url'],
            'firstCrawledAt': crawled_at,
            'lastCrawledAt': crawled_at,
        }

        article_update = {
            'script': {
                'source': """
                    if (!ctx._source.categoryIds.contains(params.categoryId)) { 
                        ctx._source.categoryIds.add(params.categoryId)
                    }
                    ctx._source.lastCrawledAt = params.lastCrawledAt
                """,
                'lang': 'painless',
                'params': {
                    'categoryId': item['category_id'],
                    'lastCrawledAt': crawled_at
                }
            },
            'upsert': article
        }

        self.link_count += 1

        if source_id in self.link_count_by_source:
            self.link_count_by_source[source_id] = self.link_count_by_source[source_id] + 1
        else:
            self.link_count_by_source[source_id] = 1

        self.es.index(index="links", id=link_id, body=link)
        self.es.update(index="articles", id=article_id, body=article_update)

        return item
