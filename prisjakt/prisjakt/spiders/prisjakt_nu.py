import re
import json
from urllib.parse import urlparse, parse_qs
import scrapy
from datetime import datetime


def norm_ws(s: str | None) -> str | None:
    if not s:
        return None
    return re.sub(r"\s+", " ", s).strip()


def parse_price(text: str | None):
    if not text:
        return None, None, None
    text = text.replace("\xa0", " ").replace("\u202f", " ").replace("\u2009", " ")
    raw = norm_ws(text)

    curr = None
    m_curr = re.search(r"\b(SEK|NOK|DKK|EUR|USD|kr)\b", raw, re.IGNORECASE)
    if m_curr:
        curr = m_curr.group(1).upper()
        if curr == "KR":
            curr = "SEK"

    m_val = re.search(r"(\d[\d\s.,]*)(?:\s*(?:SEK|kr))?", raw, re.IGNORECASE)
    value = None
    if m_val:
        num = m_val.group(1).replace(" ", "")
        if "," in num and "." in num:
            if num.rfind(",") > num.rfind("."):
                num = num.replace(".", "").replace(",", ".")
            else:
                num = num.replace(",", "")
        else:
            if "," in num:
                num = num.replace(",", ".")
        try:
            value = float(num)
        except Exception:
            value = None
    return value, curr, raw


class PrisjaktSpider(scrapy.Spider):
    name = "prisjakt"
    allowed_domains = ["prisjakt.nu"]

    start_urls = [
        "https://www.prisjakt.nu/c/moderkort",
        "https://www.prisjakt.nu/c/grafikkort",
    ]

    custom_settings = {
        "CONCURRENT_REQUESTS": 16,
        "DOWNLOAD_DELAY": 0.3,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "sv-SE,sv;q=0.9,en-US;q=0.8,nl;q=0.7",
        },
        # ✅ Pipeline koppelen
        "ITEM_PIPELINES": {
            "prisjakt.pipelines.PrisjaktExportPipeline": 300,
        },
    }

    category_map = {
        "moderkort": "Motherboards",
        "vattenkylningssystem": "Water Cooling",
        "nataggregat": "Power Supply Units",
        "grafikkort": "Graphics Cards",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def parse(self, response):
        raw_category = response.url.split("/c/")[-1].split("?")[0]
        category = self.category_map.get(raw_category, raw_category)

        product_links = response.css('a[href^="/produkt.php?p="]::attr(href)').getall()
        for href in sorted(set(product_links)):
            yield response.follow(
                href,
                callback=self.parse_product,
                meta={"category": category},
            )

        next_page = response.css('a[data-test="PaginationNavigation-next"]::attr(href)').get()
        if not next_page:
            all_pages = response.css('a[data-test="PaginationLink"]::attr(href)').getall()
            if all_pages:
                candidate = all_pages[-1]
                if candidate and candidate != response.url:
                    next_page = candidate

        if next_page and next_page != response.url:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={"category": category},
            )

    def parse_product(self, response):
        title = norm_ws(response.css("h1::text").get())
        product_id = parse_qs(urlparse(response.url).query).get("p", [None])[0]
        category = response.meta.get("category")

        yield from self._extract_offers_from_html(response, product_id, title, category)
        yield from self._extract_offers_from_jsonld(response, product_id, title, category)
        yield from self._extract_offers_from_embedded_json(response, product_id, title, category)

    # ===== extractors =====
    def _extract_offers_from_html(self, response, product_id, title, category):
        offer_nodes = response.css('[class*="Offer"], [data-test*="Offer"], [data-test*="StoreRow"]')
        for row in offer_nodes:
            store = row.css('[data-test*="Store"]::text, a[title]::attr(title), img[alt]::attr(alt), a::text').getall()
            store = norm_ws(" ".join(store)) if store else None
            price_txt = row.css('[data-test*="Price"]::text, [class*="Price"]::text').getall()
            price_txt = norm_ws(" ".join(price_txt)) if price_txt else None

            if not store and not price_txt:
                continue

            value, curr, raw = parse_price(price_txt or "")
            yield {
                "timestamp": self.timestamp,
                "category": category,
                "product_id": product_id,
                "product_title": title,
                "product_url": response.url,
                "seller_name": store,
                "price_value": value,
                "price_currency": curr,
                "price_raw": raw,
                "source": "html",
            }

    def _extract_offers_from_jsonld(self, response, product_id, title, category):
        scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for s in scripts:
            try:
                data = json.loads(s.strip())
                yield from self._yield_offers_from_ld_obj(data, response, product_id, title, category)
            except Exception:
                continue

    def _yield_offers_from_ld_obj(self, data, response, product_id, title, category):
        if isinstance(data, list):
            for obj in data:
                yield from self._yield_offers_from_ld_obj(obj, response, product_id, title, category)
            return
        if not isinstance(data, dict):
            return
        if data.get("@type") in ("Product", "AggregateOffer", "Offer") or "offers" in data:
            offers = data.get("offers", [])
            if isinstance(offers, dict):
                offers = [offers]
            for off in offers:
                store = None
                if isinstance(off.get("seller"), dict):
                    store = off["seller"].get("name")
                elif isinstance(off.get("seller"), str):
                    store = off.get("seller")
                value, curr, raw = parse_price(str(off.get("price", "")))
                yield {
                    "timestamp": self.timestamp,
                    "category": category,
                    "product_id": product_id,
                    "product_title": title,
                    "product_url": response.url,
                    "seller_name": norm_ws(store),
                    "price_value": value,
                    "price_currency": curr or off.get("priceCurrency"),
                    "price_raw": raw,
                    "source": "jsonld",
                }

    def _extract_offers_from_embedded_json(self, response, product_id, title, category):
        for s in response.css("script::text").getall():
            if "offer" not in s.lower() and "price" not in s.lower():
                continue
            for m in re.finditer(r"\{.*?\}", s, flags=re.DOTALL):
                try:
                    obj = json.loads(m.group(0))
                except Exception:
                    continue
                offers = obj.get("offers", [])
                if isinstance(offers, dict):
                    offers = [offers]
                for off in offers:
                    store = off.get("store") or off.get("seller")
                    value, curr, raw = parse_price(str(off.get("price", "")))
                    yield {
                        "timestamp": self.timestamp,
                        "category": category,
                        "product_id": product_id,
                        "product_title": title,
                        "product_url": response.url,
                        "seller_name": norm_ws(store),
                        "price_value": value,
                        "price_currency": curr or off.get("priceCurrency"),
                        "price_raw": raw,
                        "source": "embedded_json",
                    }