#app.py
from flask import Flask, render_template, request, redirect, url_for, flash
import psycopg2 #pip install psycopg2 
import psycopg2.extras
import datetime
import pandas as pd
import numpy as np
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired
import json

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
    s = "SELECT * FROM tb_ordens"
    cur.execute(s) # Execute the SQL
    # list_users = cur.fetchall()

    df = pd.read_sql_query(s, conn)
    df = df.sort_values(by='n_ordem')
    
    df.reset_index(drop=True, inplace=True)
    df.replace(np.nan, '', inplace=True)

    # Loop para percorrer todas as linhas da coluna
    for i in range(len(df['dataabertura'])):
        if df['dataabertura'][i] == '':
            df['dataabertura'][i] = df['dataabertura'][i-1]

    df = df.drop_duplicates(subset=['id_ordem'], keep='last')
    df = df.sort_values(by='id_ordem')
    list_users = df.values.tolist()

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
        n_ordem = 0
        status = 'Em espera'
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id) FROM tb_ordens")
        maior_valor = cur.fetchone()[0]
        maior_valor = maior_valor+1

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id_ordem) FROM tb_ordens")
        ultima_os = cur.fetchone()[0]
        ultima_os = ultima_os+1

        try:
            cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco,status, problemaaparente, id_ordem, n_ordem ,dataabertura) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (maior_valor, setor, maquina, risco, status, problema, ultima_os, n_ordem, dataAbertura))
            conn.commit()
        except:
            id = 0
            cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco, status, problemaaparente, id_ordem,n_ordem ,dataabertura) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (id, setor, maquina, risco, status, problema,ultima_os, n_ordem, dataAbertura))
            conn.commit()

        cur.close()

        flash('OS de número {} aberta com sucesso!'.format(maior_valor))
        return redirect(url_for('open_os'))
 
@app.route('/edit/<id_ordem>', methods = ['POST', 'GET'])
def get_employee(id_ordem):

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ('SELECT * FROM tb_ordens WHERE id_ordem = {}'.format(int(id_ordem)))
    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)

    data1 = data1.sort_values(by='n_ordem')
    data1.reset_index(drop=True, inplace=True)
    data1.replace(np.nan, '', inplace=True)

    # Loop para percorrer todas as linhas da coluna
    for i in range(len(data1['dataabertura'])):
        if data1['dataabertura'][i] == '':
            data1['dataabertura'][i] = data1['dataabertura'][i-1]

    data1 = data1.drop_duplicates(subset=['id_ordem'], keep='last')
    data1 = data1.sort_values(by='id_ordem')
    data1 = data1.values.tolist()
    opcaoAtual = data1[0][4]

    cur.close()
    
    lista_opcoes = ['Em execução','Finalizada','Aguardando material']
    
    opcoes = []
    opcoes.append(opcaoAtual)

    for opcao in lista_opcoes:
        opcoes.append(opcao)
    
    opcoes = list(set(opcoes))
    opcoes.remove(opcaoAtual)  # Remove o elemento 'c' da lista
    opcoes.insert(0, opcaoAtual)


    
    return render_template('edit.html', ordem=data1[0], opcoes=opcoes)
 
@app.route('/update/<id_ordem>', methods=['POST'])
def update_student(id_ordem):

    if request.method == 'POST':
            
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(""" SELECT MAX(id) FROM tb_ordens
            """)
        
        ultimo_id = cur.fetchone()[0]
        try:
            ultimo_id = ultimo_id+1
        except:
            ultimo_id = 0
            
        setor = request.form['setor']
        maquina = request.form['maquina']
        risco = request.form['risco']
        status = request.form['statusLista']
        problema = request.form['problema']
        datainicio = request.form['datainicio']
        horainicio = request.form['horainicio']
        datafim = request.form['datafim']
        horafim = request.form['horafim']
        id_ordem = id_ordem
        n_ordem = request.form['n_ordem']
        descmanutencao = request.form['descmanutencao']
        operador = request.form.getlist('operador')
        operador = json.dumps(operador)
        operador
        print(ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador])

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            INSERT INTO tb_ordens (id, setor,maquina,risco,status,problemaaparente,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem, descmanutencao, operador) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador]))
        #flash('OS de número {} atualizada com sucesso!'.format(int(id)))
        conn.commit()

        return redirect(url_for('Index'))

@app.route('/openOs')
def open_os():
    return render_template("openOs.html")

@app.route('/edit_material/<id_ordem>', methods = ['POST', 'GET'])
def get_material(id_ordem):
    # Verifica se a requisição é um POST
    if request.method == 'POST':
        # Obtém os dados do formulário
        id_ordem = id_ordem
        codigo = request.form['codigo']
        quantidade = request.form['quantidade']
    
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("INSERT INTO tb_carrinho (id_ordem, codigo, quantidade) VALUES (%s,%s,%s)", (id_ordem, codigo, quantidade))
        conn.commit()

    # Obtém os dados da tabela
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ('SELECT * FROM tb_carrinho WHERE id_ordem = {}'.format(int(id_ordem)))
    cur.execute(s)
    data = cur.fetchall()
    
    return render_template('material.html', datas=data, id_ordem=id_ordem)

if __name__ == "__main__":
    app.run(debug=True)