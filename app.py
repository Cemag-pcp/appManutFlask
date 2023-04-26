#app.py
from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2 #pip install psycopg2 
import psycopg2.extras
import datetime

app = Flask(__name__)
app.secret_key = "manutencaoprojeto"
 
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"
 
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
 
@app.route('/')
def Index():
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = "SELECT * FROM students"
    cur.execute(s) # Execute the SQL
    list_users = cur.fetchall()
    return render_template('index.html', list_users = list_users)
 
@app.route('/add_student', methods=['POST'])
def add_student():
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if request.method == 'POST':
        setor = request.form['setor']
        maquina = request.form['maquina']
        risco = request.form['risco']
        problema = request.form['problema']
        dataAbertura = datetime.datetime.now()

        cur.execute("INSERT INTO students (setor, maquina, risco, problemaaparente, dataabertura) VALUES (%s,%s,%s,%s,%s)", (setor, maquina, risco, problema, dataAbertura))
        conn.commit()

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id) FROM students")
        maior_valor = cur.fetchone()[0]
        maior_valor = maior_valor+1

        flash('OS de número {} aberta com sucesso!'.format(maior_valor))
        return redirect(url_for('open_os'))
 
@app.route('/edit/<id>', methods = ['POST', 'GET'])
def get_employee(id):

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM students WHERE id = {}'.format(int(id)))
    data = cur.fetchall()
    cur.close()
    print(data[0])
    
    lista_opcoes = ['Em execução','Finalizada','Aguardando material']
    opcaoAtual = data[0][4]    
    opcoes = []
    opcoes.append(opcaoAtual)

    for opcao in lista_opcoes:
        opcoes.append(opcao)

    opcoes = list(set(opcoes))
    opcoes.remove(opcaoAtual)  # Remove o elemento 'c' da lista
    opcoes.insert(0, opcaoAtual)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM tb_ordens WHERE id_ordem = {}'.format(int(id)))
    data = cur.fetchall()
    cur.close()
    
    return render_template('edit.html', student = data[0], opcoes=opcoes)
 
@app.route('/update/<id>', methods=['POST'])
def update_student(id):
    if request.method == 'POST':
        
        setor = request.form['setor']
        maquina = request.form['maquina']
        risco = request.form['risco']
        status = request.form['statusLista']
        problema = request.form['problema']
     
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            UPDATE students
            SET setor = %s,
                maquina = %s,
                risco = %s,
                status = %s,
                problemaaparente = %s
            WHERE id = %s
        """, (setor, maquina, risco, status, problema, id))
        flash('OS de número {} atualizada com sucesso!'.format(int(id)))
        conn.commit()
        return redirect(url_for('Index'))
 
@app.route('/delete/<string:id>', methods = ['POST','GET'])
def delete_student(id):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
   
    cur.execute('DELETE FROM students WHERE id = {0}'.format(id))
    conn.commit()
    flash('Os removida com sucesso!')
    return redirect(url_for('Index'))
 
@app.route('/openOs')
def open_os():
    return render_template("openOs.html")

if __name__ == "__main__":
    app.run(debug=True)