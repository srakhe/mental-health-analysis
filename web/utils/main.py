import json


def get_static_data():
    with open("data/static/questions.json", "r") as q_file:
        questions = json.load(q_file)
    with open("data/static/years.json", "r") as y_file:
        years = json.load(y_file)
    with open("data/static/regions.json", "r") as r_file:
        regions = json.load(r_file)
    with open("data/static/incomes.json", "r") as i_file:
        incomes = json.load(i_file)
    return questions, years, regions, incomes
