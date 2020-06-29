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
        story_id = hashlib.sha384((item['canonical_url'] or item['url']).encode()).hexdigest()
        source_id = item['source_id']
        crawled_at = datetime.datetime.now()

        link = {
            'id': link_id,
            'story_id': story_id,
            'source_id': source_id,
            'category': item['category'],
            'batch_id': self.batch_id,
            'batch_started_at': self.batch_started_at,
            'crawled_at': crawled_at,
            'position': item['position']
        }

        story = {
            'id': story_id,
            'source_id': source_id,
            'categories': [item['category']],
            'title': item['title'],
            'authors': item['authors'],
            'description': item['description'],
            'text': item['text'],
            'image': item['image'],
            'publish_date': item['publish_date'],
            'url': item['url'],
            'canonical_url': item['canonical_url'],
            'amp_url': item['amp_url'],
            'last_crawled_at': crawled_at,
        }

        story_update = {
            'script': {
                'source': """
                    if (!ctx._source.categories.contains(params.category)) { 
                        ctx._source.categories.add(params.category)
                    }
                    ctx._source.last_crawled_at = params.last_crawled_at
                """,
                'lang': 'painless',
                'params': {
                    'category': item['category'],
                    'last_crawled_at': crawled_at
                }
            },
            'upsert': story
        }

        self.link_count += 1

        if source_id in self.link_count_by_source:
            self.link_count_by_source[source_id] = self.link_count_by_source[source_id] + 1
        else:
            self.link_count_by_source[source_id] = 1

        self.es.index(index="links", id=link_id, body=link)
        self.es.update(index="stories", id=story_id, body=story_update)

        return item
