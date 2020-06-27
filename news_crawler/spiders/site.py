import scrapy
import uuid
import datetime
import yaml
from scrapy_splash import SplashRequest
from newspaper import Article
from newspaper import urls

class SiteSpider(scrapy.Spider):
    name = 'site'

    def start_requests(self):
        lua_source = """
            function main(splash)
                local wait = 0.5
                local num_scrolls = 5
                local scroll_delay = 0.5

                local scroll_to = splash:jsfunc("window.scrollTo")
                local get_body_height = splash:jsfunc(
                    "function() {return document.body.scrollHeight;}"
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

        batch_id = uuid.uuid4().hex
        batch_started_at = datetime.datetime.now()

        with open('sites.yaml') as f:
            sites = yaml.safe_load(f)

        for site in sites:
            for section_type, url in site['sections'].items():
                yield SplashRequest(
                    url=url, 
                    callback=self.parse,
                    endpoint='execute',
                    args={'lua_source': lua_source},
                    meta={
                        'site_id': site['id'], 
                        'section_type': section_type, 
                        'batch_id': batch_id, 
                        'batch_started_at': batch_started_at
                    }
                )

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