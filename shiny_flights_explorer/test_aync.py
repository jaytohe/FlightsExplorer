import aiohttp
import asyncio

API_URL = "http://localhost:5000/part1/"

async def post_flights_isrange(years, isRange):
    async with aiohttp.ClientSession() as session:
         async with session.post(API_URL+"num_flights_took_place", json={"years" : years, "isRange" : isRange}) as resp:
             print(await resp.json())



asyncio.run(post_flights_isrange(years=[2015, 2018], isRange=False))
