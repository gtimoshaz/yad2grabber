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
import json

import pandas as pd
from geopy.distance import geodesic

from sqlalchemy.ext.asyncio import create_async_engine

from tables import Ad, Image, Base


class AdsDownloader:
    def __init__(self, session, jobs=10, stations_csv="stations.csv"):
        self.session = session
        self.sem = asyncio.Semaphore(jobs)
        self.stations_df = pd.read_csv(stations_csv)
    async def fetch(self, url) -> aiohttp.ClientResponse:
        async with self.sem:
            resp = await self.session.get(url)
            return resp

    async def download_image_to_db(ad_id: str, url: str):
        resp = await downloader.fetch(url)
        if resp.status == 200:
            data = await resp.read()
            im = Image(ad_id=ad_id, data=data)
            #await conn.execute()
        else:
            pass
        resp.close()

    async def download_images(self, ad_id: str, urls: [str]):
        async with asyncio.TaskGroup() as tg:
            for url in urls:
                tg.create_task(download_image_to_db(ad_id, url, downloader, conn))

    async def download_apartment(self, apartment_json: dict):
        ad_id = apartment_json["id"]
        urls = apartment_json["images_urls"]
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.download_images(ad_id, urls))
            more_info = await downloader.fetch(f"https://gw.yad2.co.il/feed-search-legacy/item?token={ad_id}")
            more_info_json = await more_info.json()
            # TODO write everything to db

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
                station, dist = stationsFinder.closest((coordinates["latitude"], coordinates["longitude"]))
                if dist > 1:
                    continue
                tg.create_task(download_apartment(apartment, downloader, conn))
                # TODO write apartment info to db


    async def download_all_pages(self):
        async def download_parse_page(page_number, downloader):
            page = await downloader.fetch(
                    f"https://gw.yad2.co.il/feed-search-legacy/realestate/rent?price=-1-2000&propertyGroup=apartments"
                    f"&airConditioner=1&longTerm=1&page={page_number}&forceLdLoad=true")
            page_json = json.loads(await page.text())
            page.close()
            await download_apartments_from_page(page_json, downloader, conn)
            return page_json

        async with aiohttp.ClientSession() as session:
            downloader = Downloader(session)
            first_page = await download_parse_page(1, downloader)
            pages = first_page["data"]["pagination"]["last_page"]
            async with asyncio.TaskGroup() as tg:
                for i in range(2, pages + 1):
                    tg.create_task(download_parse_page(i, downloader))


async def async_main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await download_all_pages(conn)


engine = create_async_engine("sqlite+aiosqlite:///base.sqlite3", echo=True)
asyncio.run(async_main())
