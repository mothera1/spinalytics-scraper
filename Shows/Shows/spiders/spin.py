import scrapy

class SongItem(scrapy.Item):
    show = scrapy.Field()
    dj = scrapy.Field()
    timestamp = scrapy.Field()
    date = scrapy.Field()
    artist = scrapy.Field()
    title = scrapy.Field()
    genre = scrapy.Field()
    

class SpinSpider(scrapy.Spider):
    name = "spin"
    allowed_domains = ["spinitron.com"]
    start_urls = ["file:///C:/Users/mroth/OneDrive/Personal%20Projects/Radio_Scraper/The%20Holdsworth%20Dilemma%20with%20DJ%20Lore%20on%20WZBC%2090.3%20FM%20Newton.html"]
    def parse(self, response):
      links = response.css("div.details a::attr(href)")
      for link in links:
        yield response.follow(link, callback=self.parse_product)
     

    def parse_product(self, response):
        song = SongItem()

        show = response.css("h3.show-title a::text").get() 
        date = response.css('p.timeslot::text').get().strip()
        date = date.split(' ')[0:3]
        date = ' '.join(date)
        dj = response.css('p.dj-name a::text').get()

        song["show"] = show
        song["date"] = date
        song["dj"] = dj

        table = response.xpath('//*[@class="table table-striped table-bordered"]')
        rows = table.xpath('.//tr')
        for row in rows:
          time = row.xpath('td[1]//text()').extract()
          spin_data = row.xpath('td[3]//text()').extract()
          clean = [item.strip() for item in spin_data if item.strip()]
          print(clean)
          if len(clean) < 7:
            song["genre"] = "Unknown"
          else:
             song["genre"] = clean[-1]
          song["artist"] = clean[0]
          song["title"] = clean[1]
          song["timestamp"] = time
          yield song


#table = response.xpath('//*[@class="table table-striped table-bordered"]')
#rows = table.xpath('//tr')
#row = rows[6]
#row.xpath('td//text()')[2].extract() //don't need
#spin_data = row.xpath('td[3]//text()').extract()
#first_p = response.css('div.head-playlist div.data h3.show-title + p.timeslot::text').get()
# date = response.css("div.data h3:first_child::text").get()
#date = response.xpath('//*[@class="data"]')
#date = date.xpath('//*[@class="timeslot"]//

#links = response.css("div.list-container a::attr(href)").getall()
