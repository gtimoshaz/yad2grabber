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

import pandas as pd
from geopy.distance import geodesic

import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine


async def download_image_to_db(ad_id: str, url: str, session: aiohttp.ClientSession):
    async with session.get(url) as resp:
        if resp.status == 200:
            data = resp.read()
            # TODO write `data` to db


async def download_images(ad_id: str, urls: [str], session: aiohttp.ClientSession):
    async with asyncio.TaskGroup() as tg:
        for url in urls:
            tg.create_task(download_image_to_db(ad_id, url, session))


async def download_apartment(apartment_json: dict, session: aiohttp.ClientSession):
    ad_id = apartment_json["id"]
    urls = apartment_json["image_urls"]
    async with asyncio.TaskGroup() as tg:
        tg.create_task(download_images(ad_id, urls, session))
        async with session.get(f"https://gw.yad.co.il/feed-search-legacy/item?token={ad_id}") as more_info:
            more_info_task = tg.create_task(more_info.json())
        more_info_json = await more_info_task.result().json()
        # TODO write everything to db


class StationsFinder:
    def __init__(self, filename="stations.csv"):
        self.df = pd.read_csv(filename)

    def closest(self, coordinates: (float, float)):
        nearest_station = None
        nearest_station_dist = float('inf')
        for row in self.df.iterrows():
            station_coordinates = row["latitude"], row["longitude"]
            distance = geodesic(station_coordinates, coordinates).kilometers
            if distance < nearest_station_dist:
                nearest_station = row["name"]
                nearest_station_dist = distance
        return nearest_station, nearest_station_dist


stationsFinder = StationsFinder()


async def download_apartments_from_page(page: dict, session: aiohttp.ClientSession):
    apartments = page["data"]["feed"]["feed_items"]
    async with asyncio.TaskGroup() as tg:
        for apartment in apartments:
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
            tg.create_task(download_apartment(apartment, session))
            # TODO write apartment info to db


async def download_all_pages():
    async def download_parse_page(page_number, session):
        async with session.get(
                f"https://gw.yad2.co.il/feed-search-legacy/realestate/rent?price=-1-2000&propertyGroup=apartments&airConditioner=1&longTerm=1&page={page_number}&forceLdLoad=true") as resp:
            page = await resp.json()
            await download_apartments_from_page(page, session)
        return page

    async with aiohttp.ClientSession() as session:
        first_page = await download_parse_page(1, session)
        pages = first_page["data"]["pagination"]["last_page"]
        async with asyncio.TaskGroup() as tg:
            for i in range(2, pages + 1):
                tg.create_task(download_parse_page(i, session))
