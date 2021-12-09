from utils.main import get_static_data, set_question, get_question
from utils.emr import get_emr_status, start_emr, stop_emr
from flask import Flask, render_template, request, redirect, url_for

server = Flask(__name__)


@server.route("/", methods=["GET", "POST"])
def home():
    if request.method == "GET":
        set_question("")
        questions, years, regions, incomes = get_static_data()
        params = {
            "questions": questions,
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
            question = request.form["quesValue"]
            set_question(question)
            return redirect(url_for("loader"))


@server.route("/loader")
def loader():
    if get_emr_status() == "WAITING":
        return redirect(url_for("get_results"))
    else:
        return redirect(url_for("home"))


@server.route("/get_results", methods=["GET", "POST"])
def get_results():
    if request.method == "GET":
        if get_emr_status() == "COMPLETED":
            return render_template("get_results.html", show="")
        else:
            return render_template("get_results.html", show="disabled")
    else:
        return redirect(url_for("results"))


@server.route("/results")
def results():
    return get_question()


if __name__ == "__main__":
    server.run(debug=True)
