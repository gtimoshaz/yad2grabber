import uuid

import sqlalchemy
from typing import Optional, List

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, LargeBinary, Uuid


class Base(DeclarativeBase):
    pass


class Ad(Base):
    __tablename__ = "ad"

    id: Mapped[str] = mapped_column(primary_key=True)
    price: Mapped[int]
    lat: Mapped[float]
    lon: Mapped[float]
    city: Mapped[str]
    street: Mapped[Optional[str]]
    info_text: Mapped[str]
    info_title: Mapped[str]
    main_title: Mapped[str]
    HouseCommittee: Mapped[Optional[int]]
    property_tax: Mapped[Optional[int]]
    payments_in_year: Mapped[Optional[int]]
    furniture_info: Mapped[Optional[str]]
    images: Mapped[List["Image"]] = relationship(back_populates="ad")
    nearest_station: Mapped[str]
    distance_to_station_km: Mapped[float]


class Image(Base):
    __tablename__ = "images"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    ad_id: Mapped[str] = mapped_column(ForeignKey("ad.id"))
    ad: Mapped["Ad"] = relationship(back_populates="images")
    data: Mapped[bytes]