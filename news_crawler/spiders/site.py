import scrapy
import uuid
import datetime
import yaml
from newspaper import Article
from newspaper import urls

class SiteSpider(scrapy.Spider):
    name = 'site'

    def start_requests(self):
        batch_id = uuid.uuid4().hex
        batch_started_at = datetime.datetime.now()

        with open('sites.yaml') as f:
            sites = yaml.safe_load(f)

        for site in sites:
            for section_type, url in site['sections'].items():
                yield scrapy.Request(url=url, callback=self.parse, meta={'site_id': site['id'], 'section_type': section_type, 'batch_id': batch_id, 'batch_started_at': batch_started_at})

    def parse(self, response):
        for i, href in enumerate(response.css('a::attr(href)').extract()):
            url = response.urljoin(href)
            
            if urls.valid_url(url):
                yield scrapy.Request(url=url, callback=self.parse_story, meta={**response.meta, 'position': i})

    def parse_story(self, response):
        og_type = response.css('meta[property="og:type"]::attr(content)').extract_first()

        if og_type == "article":
            article = Article(url=response.url, language='en')
            article.download(response.body)
            article.parse()

            amp_url = response.css('link[rel="amphtml"]::attr(href)').extract_first()
            og_description = response.css('meta[property="og:description"]::attr(content)').extract_first()

            yield {
                'id': uuid.uuid4().hex,
                'site_id': response.meta['site_id'],
                'section_type': response.meta['section_type'],
                'batch_id': response.meta['batch_id'],
                'batch_started_at': response.meta['batch_started_at'],
                'crawled_at': datetime.datetime.now(),
                'position': response.meta['position'],
                'title': article.title,
                'authors': article.authors,
                'description': og_description,
                'text': article.text,
                'image': article.top_image,
                'publish_date': article.publish_date,
                'url': response.url,
                'canonical_url': article.canonical_link,
                'amp_url': amp_url
            }