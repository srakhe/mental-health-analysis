from utils.main import get_static_data
from utils.emr import get_emr_status, start_emr, stop_emr
from utils.map import get_graph
from flask import Flask, render_template, request, redirect, url_for
import json

server = Flask(__name__)


@server.route("/", methods=["GET", "POST"])
def home():
    if request.method == "GET":
        questions, years, regions, incomes = get_static_data()
        params = {
            "questions": questions,
            "years": years,
            "regions": regions,
            "incomes": incomes,
            "emr_status": get_emr_status()
        }
        questions = dict(questions)
        questions.items()
        return render_template("index.html", params=params)
    else:
        if "start_emr" in request.form:
            start_emr()
            return redirect(url_for("home"))
        elif "stop_emr" in request.form:
            stop_emr()
            return redirect(url_for("home"))
        elif "refresh" in request.form:
            return redirect(url_for("home"))
        else:
            params = {
                "question": request.form["quesValue"],
                "year": request.form["yearValue"],
                "region": request.form["regionValue"],
                "income": request.form["incomeValue"]
            }
            return redirect(url_for("dashboard", params=json.dumps(params)))


@server.route("/dashboard")
def dashboard():
    if get_emr_status() == "Running":
        params = request.args["params"]
        params = json.loads(params)
        return render_template("dashboard.html", graph_json=get_graph(params))
    else:
        return redirect(url_for("home"))


if __name__ == "__main__":
    server.run(debug=True)
