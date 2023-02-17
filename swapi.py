import asyncio
import datetime

import requests
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON, String
from more_itertools import chunked


PG_DSN = 'postgresql+asyncpg://user:secret@127.0.0.1:5431/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True, autoincrement=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


CHUNK_SIZE = 10


async def chunked_async(async_iter, size):

    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            if buffer:
                yield buffer
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_person(people_id: int, session: ClientSession):
    print(f'begin {people_id}')
    async with session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
    print(f'end {people_id}')
    return json_data


async def get_people():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 80), CHUNK_SIZE):
            coroutines = [get_person(people_id=i, session=session) for i in chunk]
            results = await asyncio.gather(*coroutines)
            for item in results:
                yield item


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(name=item['name'],
                                birth_year=item['birth_year'],
                                eye_color=item['eye_color'],
                                films=', '.join([film['title']
                                                 for film in [requests.get(ind).json()
                                                                  for ind in item['films']]]),
                                species=', '.join([specy['name']
                                                   for specy in [requests.get(ind).json()
                                                                 for ind in item['species']]]),
                                vehicles=', '.join([vehicle['name']
                                                   for vehicle in [requests.get(ind).json()
                                                    for ind in item['vehicles']]]),
                                starships=', '.join([starship['name']
                                                    for starship in [requests.get(ind).json()
                                                    for ind in item['starships']]]),
                                gender=item['gender'],
                                hair_color=item['hair_color'],
                                height=item['height'],
                                homeworld=item['homeworld'],
                                mass=item['mass'],
                                skin_color=item['skin_color']
                                ) for item in people_chunk if 'detail' not in item])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task

start = datetime.datetime.now()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
print(datetime.datetime.now() - start)
