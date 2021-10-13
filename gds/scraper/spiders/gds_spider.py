import scrapy
#command: scrapy crawl gds


class GDSSpider(scrapy.Spider):
    name = 'gds'
    filename = ''

    def __init__(self, outputfile=None, *args, **kwargs):
        super(GDSSpider, self).__init__(*args, **kwargs)
        self.filename = outputfile
        print(f'__init__ filename = {outputfile}')

    def start_requests(self):
        # start with a clean file every time request is done
        print(f'filename={self.filename}')
        with open(self.filename, 'w') as f:
            print('init datasets.html')
        urls = [
            'https://ftp.ncbi.nlm.nih.gov/geo/datasets/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        with open(self.filename, 'a') as f:
            # Get all the <a> tags
            a_selectors = response.xpath("//a")
            # Loop on each tag
            for selector in a_selectors:
                # Extract the link text
                text = selector.xpath("text()").extract_first()
                print('text=' + text)
                # Extract the link href
                link = selector.xpath("@href").extract_first()
                # Create a new Request object
                request = response.follow(link, callback=self.parse)
                # Return it thanks to a generator
                if text != 'Parent Directory':
                    f.write(text+'\n')
                    if '.gz' not in text:
                        yield request
        self.log(f'Saved file {self.filename}')
