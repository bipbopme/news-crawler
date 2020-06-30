import scrapy
import uuid
import datetime
import urllib.request
import json
import re
from scrapy_splash import SplashRequest
from newspaper import Article
from newspaper import urls
import logging
import urltools
import base64

class SiteSpider(scrapy.Spider):
    name = 'site'

    def start_requests(self):
        for source in self.fetch_sources():
            for section in source['sections']:
                use_desktop = source.get('useDesktop', False)

                meta = {
                    'dont_cache': True,
                    'source_id': source['id'],
                    'category_id': section['categoryId'],
                    'custom_url_pattern': source.get('customUrlPattern', None)
                }

                if use_desktop:
                    yield scrapy.Request(
                        url=section['url'],
                        callback=self.parse,
                        meta=meta
                    )
                else:
                    yield SplashRequest(
                        url=section['url'],
                        callback=self.parse,
                        endpoint='execute',
                        args={'lua_source': self.get_lua_source(), 'timeout': 60},
                        meta=meta,
                        splash_headers={'Authorization': 'Basic ' + self.get_splash_auth()}
                    )

    def parse(self, response):
        if response.meta.get('custom_url_pattern', None):
            custom_url_pattern = re.compile(response.meta['custom_url_pattern'])
        else:
            custom_url_pattern = None

        i = 0
        for a in response.css('a'):
            text = " ".join(a.css('*::text').getall())
            href = a.css('::attr(href)').get()
            url = response.urljoin(href)
            
            if self.is_valid_url(response, url, text, custom_url_pattern):
                i += 1
                yield scrapy.Request(url=url, callback=self.parse_article, meta={**response.meta, 'dont_cache': False, 'position': i})

    def parse_article(self, response):
        url = response.request.url
        og_type = response.css('meta[property="og:type"]::attr(content)').extract_first()

        # Allow custom url pattern to override article detection
        if og_type == "article" or 'custom_url_pattern' in response.meta:
            article = Article(url=url, language='en')
            article.download(response.body)
            article.parse()

            amp_url = response.css('link[rel="amphtml"]::attr(href)').extract_first()
            og_description = response.css('meta[property="og:description"]::attr(content)').extract_first()

            yield {
                'source_id': response.meta['source_id'],
                'category_id': response.meta['category_id'],
                'position': response.meta['position'],
                'title': article.title,
                'authors': article.authors,
                'description': og_description,
                'text': article.text,
                'image': article.top_image,
                'publish_date': article.publish_date,
                'url': url,
                'canonical_url': article.canonical_link,
                'amp_url': amp_url
            }
    
    def is_valid_url(self, response, url, text, custom_url_pattern):
        word_count = len(re.split('\\s+', text.strip())) if text else 0

        if custom_url_pattern:
            return word_count >= 5 and custom_url_pattern.search(url)
        else:
            site_domain = urltools.parse(response.url).domain
            url_domain = urltools.parse(url).domain

            return word_count >= 5 and url_domain == site_domain and urls.valid_url(url)

    def fetch_sources(self):   
        with urllib.request.urlopen("http://localhost:5500/api/sources") as url:
            return json.loads(url.read().decode())
    
    def get_lua_source(self):
        return """
            function main(splash)
                splash.images_enabled = false
                splash.resource_timeout = 10.0

                local wait = 1.0
                local num_scrolls = 5
                local scroll_delay = 0.5

                local scroll_to = splash:jsfunc("window.scrollTo")
                local get_body_height = splash:jsfunc(
                    "function() {return document.documentElement.scrollHeight || 0;}"
                )

                splash:set_viewport_size(411, 823)
                assert(splash:go{splash.args.url, headers={["User-Agent"] = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36"}})
                splash:wait(wait)

                for _ = 1, num_scrolls do
                    scroll_to(0, get_body_height())
                    splash:wait(scroll_delay)
                end

                return splash:html()
            end
        """
    
    def get_splash_auth(self):
        credentials = 'user:userpass'
        return base64.b64encode(credentials.encode('ascii')).decode('ascii')