import scrapy
from scrapy.http import Response
from typing import Any

class OfadaaDotComSpider(scrapy.Spider):
    name = "ofadaadotcom"
    allowed_domains = ["ofadaa.com"]
    start_urls = ["https://ofadaa.com"]
    cities: list[str] = ["abuja", "ibadan", "lagos", "kaduna", "port-harcourt"]
    RESTAURANTS = "restaurants"
    city_area_map: dict[str, list[str]] = {
        "abuja": [
            "asokoro", "cbd", "garki", "garki-2", "gudu", "gwagwalada", "gwarinpa", "gwaska",
            "jabi", "kado", "karu", "katampe", "kaura", "kubwa", "kuje", "lugbe", "mabushi",
            "maitama", "maraba", "sauka", "utako", "wuse-1", "wuse-2", "wuye"
        ],
        "ibadan": [
            "agbowo", "agodi", "akobo", "basorun", "bodija", "challenge", "dugbe", "eleyele",
            "iwo-road", "jericho", "mokola-hill", "new-bodija", "new-gra", "oke-ado", "oke-aremo",
            "old-bodija", "oluyole", "onireke", "samonda", "sango"
        ],
        "lagos":[
            "agege", "akowonjo", "alimosho", "amuwo-odofin", "apapa", "ebute-metta", "festac-town",
            "ifako-ijaiye", "ikeja", "ikorodu", "ikoyi", "ilupeju", "ketu", "kosofe", "lagos-island",
            "lekki", "lekki-phase-1", "magodo", "maryland", "mushin", "ogudu", "ojo", "ojodu",
            "oshodi", "oshodi-isolo", "somolu", "surulere", "victoria-island", "yaba"
        ],
        "kaduna": [
            "barnawa", "kaduna-north", "kaduna-south", "kakuri", "malali", "sabon-gari",
            "unguwar-rimi", "unguwar-sarki"
        ],
        "port-harcourt": [
            "choba", "dline", "elechi", "gra-phase-1", "gra-phase-2", "gra-phase-3", "gra-phase-4",
            "mgbuoba", "old-gra", "old-township", "rumueme", "rumuibekwe", "rumuodara", "rumuogba",
            "rumuokoro", "rumuokwuta", "rumuomasi", "trans-amadi", "woji"
        ]
    }

    def parse(self, response: Response, **kwargs: Any):
        self.generic_restaurant_search(response)

    def city_search(self, response: Response):
        pass

    def area_search(self, response: Response):
        pass

    def generic_restaurant_search(self, response: Response):
        term = "Amala+Abula+Canteen"
        response.urljoin(f"/{self.RESTAURANTS}?term={term}")
        result_table = response.xpath("//div[contains(@class, 'result-table')]//a[@class='restaurant']/@href").getall()
        for result in result_table:
            yield response.follow(f"{self.start_urls[0]}/{result}", self.parse_restaurant_page)


    def parse_restaurant_page(self, response: Response):
        pass