from flask import Flask, request, jsonify
import database

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['POST'])
def home():
    if request.is_json:
        data = request.json
        if 'database' not in data or isinstance(data['database'], str) == False:
            return 'Bad request. You must specify the database string field', 400
        if request.authorization:
            if isinstance(request.authorization.username, str) and isinstance(request.authorization.password, str):
                db = database.Database(data['database'], request.authorization.username, request.authorization.password)
                if db.tables == {}:
                    return 'Not Authorized', 403
        else:
            return 'Not Authorized', 403
        if 'query' not in data or isinstance(data['query'], str) == False:
            return 'Bad request. You must specify the query string field', 400
        res = db.sql(data['query'])
        if res == 'User have not enough privillages for this table':
            return 'Not Authorized. You don\'t have the privillages to access this table', 403
        return { "result": res }, 200
    else:
        return 'Bad request. Not JSON', 400
    

app.run()