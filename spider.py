# downloads data from yad.co.il
# scheme:
# https://gw.yad2.co.il/feed-search-legacy/realestate/rent?...
#   -> $id
#     https://gw.yad.co.il/feed-search-legacy/item?token=$id
#   -> image_urls
#
# tables:
# ads:
#    id: str
#    lat, lon: float
#    city: str
#    street: str
#    price: int
#    info_text, info_title, main_title: str
#    HouseCommittee: int?
#    property_tax: int?
#    payments_in_year: int?
#    furniture_info: str?
# images:
#    id: primary (str or increment int)
#    ad_id: ForeignKey(ads.id) str
#    url: str
#    data: varbinary
import asyncio, aiohttp
from aiohttp_retry import RetryClient, ExponentialRetry
import re
import uuid

import pandas as pd
from geopy.distance import geodesic
import sqlalchemy.exc
import sqlite3

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from tables import Ad, Image, Base


class AdsDownloader:
    def __init__(self, session, jobs=10, stations_csv="stations.csv"):
        self.aiohttp_session = session
        self.sem = asyncio.Semaphore(jobs)
        self.stations_df = pd.read_csv(stations_csv)
        self.engine = create_async_engine("sqlite+aiosqlite:///base.sqlite3", echo=True)
        self.session_maker = async_sessionmaker(bind=self.engine,
                                                expire_on_commit=False)
        self.db_semaphore = asyncio.Semaphore(1)

    async def fetch_json(self, url) -> dict:
        async with self.sem:
            retry_params = ExponentialRetry(attempts=5, start_timeout=1)
            retry_session = RetryClient(client_session=self.aiohttp_session, retry_options=retry_params)
            await asyncio.sleep(0.05)
            async with retry_session.get(url) as resp:
                js = await resp.json()
            return js

    async def fetch_bytes(self, url) -> (bytes, int):
        async with self.sem:
            async with self.aiohttp_session.get(url) as resp:
                content = await resp.read()
        return content, resp.status

    async def download_image_to_db(self, ad_id: str, url: str):
        data, status = await self.fetch_bytes(url)
        if status == 200:
            im = Image(ad_id=ad_id, data=data, id=uuid.uuid4())
            async with self.db_semaphore:
                async with self.session_maker() as session:
                    async with session.begin():
                        session.add(im)
                        await session.commit()
        else:
            pass

    async def download_images(self, ad_id: str, urls: [str]):
        async with asyncio.TaskGroup() as tg:
            for url in urls:
                tg.create_task(self.download_image_to_db(ad_id, url))

    @staticmethod
    def int_price(price: str | int) -> int | None:
        if isinstance(price, int):
            return price
        if "לא" in price or "גמיש" in price:
            return None
        else:
            # Remove the currency symbol and commas, then convert to an integer
            return int(re.sub(r'[^\d]', '', price))

    async def download_apartment(self, apartment_json: dict):
        ad_id = apartment_json["id"]
        urls = apartment_json["images_urls"]
        price = AdsDownloader.int_price(apartment_json["price"])
        if price is None:
            return
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.download_images(ad_id, urls))
            more_info = await self.fetch_json(f"https://gw.yad2.co.il/feed-search-legacy/item?token={ad_id}")
            more_data: dict = more_info["data"]
            lon = apartment_json["coordinates"]["longitude"]
            lat = apartment_json["coordinates"]["latitude"]
            station, distance = self.closest((lat, lon))
            async with self.db_semaphore:
                async with self.session_maker() as session:
                    async with session.begin():
                        ad = Ad(
                            id=ad_id,
                            lon=lon,
                            lat=lat,
                            city=apartment_json["city"],
                            info_text=more_data["info_text"],
                            info_title=more_data["info_title"],
                            main_title=more_data["main_title"],
                            price=price,
                            HouseCommittee=AdsDownloader.int_price(more_data["HouseCommittee"]),
                            property_tax=AdsDownloader.int_price(more_data["property_tax"]),
                            payments_in_year=AdsDownloader.int_price(more_data["payments_in_year"]),
                            furniture_info=more_data["furniture_info"],
                            street=more_data.get("street"),
                            nearest_station=station,
                            distance_to_station_km=distance
                        )
                        session.add(ad)
                        try:
                            await session.commit()
                        except sqlite3.IntegrityError:
                            await session.rollback()
                        except sqlalchemy.exc.IntegrityError:
                            await session.rollback()

    def closest(self, coordinates: (float, float)):
        nearest_station = None
        nearest_station_dist = float('inf')
        for row in self.stations_df.iterrows():
            row = row[1]
            station_coordinates = row["latitude"], row["longitude"]
            distance = geodesic(station_coordinates, coordinates).kilometers
            if distance < nearest_station_dist:
                nearest_station = row["name"]
                nearest_station_dist = distance
        return nearest_station, nearest_station_dist

    async def download_apartments_from_page(self, page: dict):
        apartments = page["data"]["feed"]["feed_items"]
        async with asyncio.TaskGroup() as tg:
            for apartment in apartments:
                if "images_count" not in apartment:
                    continue
                if apartment["images_count"] == 0:
                    continue
                if "coordinates" not in apartment:
                    continue
                if "latitude" not in apartment["coordinates"]:
                    continue
                coordinates = apartment["coordinates"]
                station, dist = self.closest((coordinates["latitude"], coordinates["longitude"]))
                if dist > 1:
                    continue
                tg.create_task(self.download_apartment(apartment))

    async def download_all_pages(self):
        async def download_parse_page(page_number):
            page = await self.fetch_json(
                    f"https://gw.yad2.co.il/feed-search-legacy/realestate/rent?price=-1-2000&propertyGroup=apartments"
                    f"&airConditioner=1&longTerm=1&page={page_number}&forceLdLoad=true")

            await self.download_apartments_from_page(page)
            return page

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        first_page = await download_parse_page(1)
        pages = first_page["data"]["pagination"]["last_page"]
        async with asyncio.TaskGroup() as tg:
            for i in range(2, pages + 1):
                tg.create_task(download_parse_page(i))


async def async_main():
    async with aiohttp.ClientSession() as session:
        downloader = AdsDownloader(session)
        await downloader.download_all_pages()

asyncio.run(async_main())
