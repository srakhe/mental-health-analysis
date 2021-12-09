from utils.main import get_static_data
import plotly
import plotly.express as px
import json


def get_graph(params):
    questions, years, regions, incomes = get_static_data()
    for_quest = questions[params.get("question")]
    for_year = years[params.get("year")]
    for_region = regions[params.get("region")]
    for_income = incomes[params.get("income")]
    return generate_map()


def generate_map():
    fig = px.choropleth(locationmode="country names", locations=["Canada"], title=for_quest)
    fig.update_geos(fitbounds="locations", visible=False)
    graph_json = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graph_json
