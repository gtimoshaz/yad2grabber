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

import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

