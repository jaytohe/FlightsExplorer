import aiohttp
import asyncio
from shiny import reactive
from shiny.express import input, ui, render

ui.page_opts(title="FlightsExplorer")

API_URL = "http://localhost:5000/part1/"



with ui.nav_panel("Part 1"):
    @render.ui
    def desc1():
        return ui.markdown("""
## Display total number of flights in given year(s)

Enter a year or years below to retrieve the total number of flights present in the dataset for said year(s).

A range of years can be specified by: `<year1>-<year2>` e.g. `2017-2019`.

Multiple years can be specified by comma seperated valus i.e. `<year1>, <year2>, <year3>,...` e.g. `2000, 1989, 2014`
""")
    
    ui.input_text("dates", "Enter Date(s)", "1987")
    ui.input_task_button("search_flights_btn", "Search")

    @ui.bind_task_button(button_id="search_flights_btn")
    @reactive.extended_task
    async def post_flights_isrange(years, isRange):
        async with aiohttp.ClientSession() as session:
            async with session.post(API_URL+"num_flights_took_place", json={"years" : years, "isRange" : isRange}) as resp:
                decoded_resp = await resp.json()
                print(decoded_resp)
                return decoded_resp

    @reactive.effect
    @reactive.event(input.search_flights_btn)
    def btn_click():
        years = [int(date.strip()) for date in input.dates().split(",")]
        post_flights_isrange(years, False)

    @render.text
    def sum():
        return str(post_flights_isrange.result())