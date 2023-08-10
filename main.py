import asyncio
import math

from fastapi import FastAPI
from fastapi.responses import Response
import sqlalchemy.exc
import sqlite3

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import selectinload

from tables import Ad, Image, Base

import uuid

app = FastAPI()

engine = create_async_engine("sqlite+aiosqlite:///base.sqlite3", echo=True)
session_maker = async_sessionmaker(bind=engine,
                                   expire_on_commit=False)


# db_semaphore = asyncio.Semaphore(1)


def img_src_pair(im: Image) -> (str, str):
    """Returns a pair of images, one for
    navigation (carousel of images) and
    one for full-size image

    One should use the output as:
        <ul id="navigation">
            {for img_src_pair[0] // first str in tuple}
        </ul>
        <div id="full-picture">
            {for img_src_pair[1] // second str in tuple}
        </div>

    """
    return (
        f"<li>"
        f"<a href=\"#{im.id}\">"
        f"<img src=\"/image/{im.id}.jpg\" />"
        f"</a>"
        f"</li>",

        f"<div>"
        f"<img id=\"{im.id}\" src=\"/image/{im.id}.jpg\" />"
        f"</div>"
    )  # copied from https://www.w3docs.com/snippets/css/how-to-create-a-simple-css-gallery-without-using-javascript.html


def ad_html(ad: Ad) -> str:
    link = f"https://yad2.co.il/item/{ad.id}"
    gallery_carousel = ""
    gallery_image = ""
    for im in ad.images:
        gc, gi = img_src_pair(im)
        gallery_carousel += gc
        gallery_image += gi
    top = f"<div class=\"ad\">" \
          f"<h1><a href=\"{link}\">{ad.main_title}</a></h1>" \
          f"<h2>{ad.city}, {ad.nearest_station} ({math.ceil(ad.distance_to_station_km * 1000)} m)</h2>" \
          f"<p class=\"price\">Price: <span>{ad.price}</span></p>" \
          f"<p class=\"price\"> Arnona: <span>{ad.property_tax}</span></p>" \
          f"<p class=\"price\"> Vaad beit: <span>{ad.HouseCommittee}</span></p>" \
          f"<p>{ad.info_text}</p>" \
          f"<p><small>Furniture: </small>{ad.furniture_info}</p>" \

    gallery = f"<div class=\"gallery\">" \
              f"<ul class=\"navigation\">" \
              f"{gallery_carousel}" \
              f"</ul>" \
              f"<div class=\"full-picture\">" \
              f"{gallery_image}" \
              f"</div>" \
              f"</div>"

    return f"{top}{gallery}</div>"


@app.get("/")
async def root():
    css = """<style>
          .gallery {
            width: 600px;
            overflow: hidden;
            position: relative;
            z-index: 1;
            margin: 100px auto;
            border: 2px solid #003C72;
          }
          .navigation {
            list-style: none;
            padding: 0;
            margin: 0;
            display: flex;
            justify-content: space-between;
          }
          .navigation li {
            padding: 0;
            margin: 0;
            margin: 5px 0 20px;
          }
          .navigation li a img {
            display: block;
            border: none;
            height: 100px;
          }
          .navigation li a {
            display: block;
          }
          .full-picture {
            width: 600px;
            height: 600px;
            overflow: hidden;
            float: left;
          }
          .full-picture img {
            /*height: 600px;
            width: 375px;*/
          }
        </style>"""  # copied from https://www.w3docs.com/snippets/css/how-to-create-a-simple-css-gallery-without-using-javascript.html
    top = f"""<!DOCTYPE html>
        <head>
        <title>Apartments in Israel</title>
        {css}
        <meta charset="utf-8" />
        </head>
        <body>
        """
    listings = ""
    async with session_maker() as conn:
        ads_query = sqlalchemy.select(Ad).options(selectinload(Ad.images))
        ads_response = await conn.execute(ads_query)
        for ad in ads_response.scalars():
            listings += ad_html(ad)
    return Response(f"{top}{listings}</body></html>", )


@app.get("/image/{name}",
         responses={
             200: {
                 "content": {"image/jpeg": {}}
             },
             400: {
                 "content": {"text/html": {}}
             },
             404: {
                 "content": {"text/html": {}}
             }
         },
         response_class=Response)
async def get_image(name: str):
    uuid_str = name.split(".")[0]
    try:
        uuid_obj = uuid.UUID(uuid_str)
    except ValueError:
        return Response("<h1>Error 400: Bad Request</h1>\n<p>Image ID is not a valid UUID</p>", status_code=400,
                        media_type="text/html")
    async with session_maker() as conn:
        im_query = sqlalchemy.select(Image).where(Image.id == uuid_obj)
        im_response = await conn.execute(im_query)
        im_obj: Image = im_response.scalar()
        if im_obj is None:
            return Response("<h1>Error 404: Image not found</h1>", status_code=404)
        im_bytes = im_obj.data
        return Response(content=im_bytes, media_type="image/jpg")
