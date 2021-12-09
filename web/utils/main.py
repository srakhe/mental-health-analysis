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


def set_question(q_value):
    with open("data/static/question.txt", "w+") as selected_question:
        selected_question.write(q_value)


def get_question():
    with open("data/static/question.txt", "r") as selected_question:
        q_value = selected_question.read()
    return q_value
