from fastapi import FastAPI
from fastapi.requests import Request
from os import path
from fastapi.responses import StreamingResponse
import ijson.backends.yajl2_c as ijson
import json
import time
from datetime import datetime, timezone

app = FastAPI()

FILE_USER_JSON = path.join(path.dirname(__file__), "users.json")


def get_request_datetime():
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# @app.post("/users")
# async def save_users(request: Request):
#     with open(FILE_USER_JSON, "wb") as f:
#         request_datetime = get_request_datetime()
#         time_start = time.time()
#         async for chunk in request.stream():
#             f.write(chunk)
#         execution_time_ms = int((time.time() - time_start) * 1000)
#     return {
#         "message": "Arquivo recebido com sucesso",
#         "timestamp": request_datetime,
#         "execution_time_ms": execution_time_ms,
#     }


@app.get("/superusers")
async def get_superusers():
    def stream_json():
        request_datetime = get_request_datetime()
        time_start = time.time()
        yield '"data":['
        with open(FILE_USER_JSON, "rb") as f:
            user_first = True
            users = ijson.items_gen(f, "item", use_float=True)
            for user in users:
                if user["score"] >= 900:
                    if user_first:
                        user_first = False
                        yield json.dumps(user)
                        continue
                    yield ","
                    yield json.dumps(user)
        yield "]"
        yield ',"execution_time_ms": {}'.format(int((time.time() - time_start) * 1000))
        yield ',"timestamp": "{}"'.format(request_datetime)

    return StreamingResponse(stream_json(), media_type="application/json")


@app.get("/top-countries")
async def get_top_countries():
    countries = {}
    request_datetime = get_request_datetime()
    time_start = time.time()
    with open(FILE_USER_JSON, "rb") as f:
        users = ijson.items_gen(f, "item", use_float=True)
        for user in users:
            country_name = user["pais"]
            if country_name not in countries:
                countries[country_name] = {}
                countries[country_name]["total"] = 1
                countries[country_name]["country"] = country_name
                continue
            countries[country_name]["total"] += 1

    return {
        "timestamp": request_datetime,
        "execution_time_ms": int((time.time() - time_start) * 1000),
        "countries": list(countries.values()),
    }


@app.get("/team-insights")
async def get_team_insights():
    teams = {}
    request_datetime = get_request_datetime()
    time_start = time.time()
    with open(FILE_USER_JSON, "rb") as f:
        users = ijson.items_gen(f, "item", use_float=True)
        for user in users:
            team = user["equipe"]
            team_name = team["nome"]

            if team_name not in teams:
                teams[team_name] = {}
                teams[team_name]["team"] = team_name
                teams[team_name]["total_members"] = 0
                teams[team_name]["leaders"] = 0
                teams[team_name]["total_project"] = 0
                teams[team_name]["completed_projects"] = 0
                teams[team_name]["active_percentage"] = 0

            teams[team_name]["total_members"] += 1
            if team["lider"]:
                teams[team_name]["leaders"] += 1

            for project in team["projetos"]:
                teams[team_name]["total_project"] += 1
                if project["concluido"]:
                    teams[team_name]["completed_projects"] += 1

            teams[team_name]["active_percentage"] = round(
                (
                    teams[team_name]["completed_projects"]
                    / teams[team_name]["total_project"]
                )
                * 100,
                1,
            )

    return {
        "timestamp": request_datetime,
        "execution_time_ms": int((time.time() - time_start) * 1000),
        "teams": list(teams.values()),
    }


@app.get("/active-users-per-day")
async def get_active_users_per_day():
    logins = {}
    request_datetime = get_request_datetime()
    time_start = time.time()
    with open(FILE_USER_JSON, "rb") as f:
        users = ijson.items_gen(f, "item", use_float=True)
        for user in users:
            logs = user["logs"]
            for log in logs:
                login_date = log["data"]
                login_acao = log["acao"]
                if login_acao != "login":
                    continue
                if login_date not in logins:
                    logins[login_date] = {}
                    logins[login_date]["date"] = login_date
                    logins[login_date]["total"] = 1
                    continue
                logins[login_date]["total"] += 1

    return {
        "timestamp": request_datetime,
        "execution_time_ms": int((time.time() - time_start) * 1000),
        "logins": list(logins.values()),
    }
