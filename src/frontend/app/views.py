from app import app
from flask import render_template
from flask_mysqldb import MySQL
import MySQLdb
@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"
@app.route('/user')
def hello():
    conn = MySQLdb.connect(host="localhost", user="root",passwd="root",db="recommendation_system")
    cur = conn.cursor()
    cur.execute('''select user.name,user.id from user,recommendation where user.id = recommendation.user_id''')
    results = cur.fetchall()
    print results
    return render_template('username.html', results=results)
    #return render_template('hello.html')
@app.route('/results/<user_id>')
def results(user_id):
    conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="recommendation_system")
    cur = conn.cursor()
    aaa = [user_id]
    cur.execute("select rb1, rb2, rb3, rb4, rb5, rb6, rb7, rb8 from recommendation where user_id = %s", aaa)
    business = cur.fetchall()
    name = ()
    for i in range(0,8):
        param = [business[0][i]]
        cur.execute('select name from business where id = %s', param)
        business_name = cur.fetchall()
        print business_name[0][0]
        name = name + (business_name[0][0],)
    return render_template('results.html', business=name)
