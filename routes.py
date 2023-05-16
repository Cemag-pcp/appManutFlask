#app.py
from flask import Flask, render_template, request, redirect, url_for, flash,Blueprint, Response
import psycopg2 #pip install psycopg2 
import psycopg2.extras
import datetime
import pandas as pd
import numpy as np
import json
import plotly.graph_objs as go
import plotly.offline as opy
from funcoes import gerador_de_semanas_informar_manutencao, login_required
import warnings
from flask import session
from functools import wraps
import base64

routes_bp = Blueprint('routes', __name__)

warnings.filterwarnings("ignore")
 
# DB_HOST = "localhost"
DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"
 
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

@routes_bp.route('/')
@login_required
def inicio(): # Redirecionar para a página de login
    
    return render_template("login/login.html")

@routes_bp.route('/index')
@login_required
def Index(): # Página inicial (Página com a lista de ordens de serviço)
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    #s = "SELECT * FROM tb_ordens"
    s = (""" 
        SELECT DISTINCT t1.total, t2.* 
        FROM (
            SELECT tb_carrinho.id_ordem, SUM(tb_material.valor * tb_carrinho.quantidade) AS total
            FROM tb_carrinho
            JOIN tb_material ON tb_carrinho.codigo = tb_material.codigo
            GROUP BY tb_carrinho.id_ordem
        ) t1
        RIGHT JOIN tb_ordens t2 ON t1.id_ordem = t2.id_ordem;
    """)

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
    df.reset_index(drop=True, inplace=True)

    for i in range(len(df)):
        if df['total'][i] == '':
            df['total'][i] = 0

    df['total'] = df['total'].apply(lambda x: round(x, 2))

    list_users = df.values.tolist()

    return render_template('user/index.html', list_users = list_users)

@routes_bp.route('/add_student', methods=['POST']) 
@login_required
def add_student(): # Criar ordem de serviço
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    if request.method == 'POST':

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

        try:
            maior_valor = maior_valor+1
        except:
            maior_valor = 0

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id_ordem) FROM tb_ordens")
        ultima_os = cur.fetchone()[0]

        try:
            ultima_os = ultima_os+1
        except:
            ultima_os = 0

        try:

            cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco,status, problemaaparente, id_ordem, n_ordem ,dataabertura) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (maior_valor, setor, maquina, risco, status, problema, ultima_os, n_ordem, dataAbertura))

            if 'imagem' in request.files:
                imagem = request.files['imagem']
                # Ler os dados da imagem
                imagem_data = imagem.read()

                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("INSERT INTO tb_imagens (id_ordem, imagem) VALUES (%s,%s)", (ultima_os, imagem_data))
                conn.commit()

        except:
            id = 0
            cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco, status, problemaaparente, id_ordem,n_ordem ,dataabertura) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (id, setor, maquina, risco, status, problema,ultima_os, n_ordem, dataAbertura))
            
            if 'imagem' in request.files:
                imagem = request.files['imagem']
                # Ler os dados da imagem
                imagem_data = imagem.read()

                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cur.execute("INSERT INTO tb_imagens (id_ordem, imagem) VALUES (%s,%s)", (ultima_os, imagem_data))
                conn.commit()
                
            conn.commit()

        cur.close()

        flash('OS de número {} aberta com sucesso!'.format(ultima_os))
        return redirect(url_for('routes.open_os'))
 
@routes_bp.route('/edit/<id_ordem>', methods = ['POST', 'GET'])
@login_required
def get_employee(id_ordem): # Página para edição da ordem de serviço (Informar o andamento da ordem)

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

    return render_template('user/edit.html', ordem=data1[0], opcoes=opcoes)
 
@routes_bp.route('/update/<id_ordem>', methods=['POST'])
@login_required
def update_student(id_ordem): # Inserir as edições no banco de dados

    if request.method == 'POST':
            
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(""" 
            SELECT MAX(id) FROM tb_ordens
        """)
        
        ultimo_id = cur.fetchone()[0]
        
        try:
            ultimo_id = ultimo_id+1
        except:
            ultimo_id = 0
        
        print(ultimo_id)

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

        print(ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador])

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            INSERT INTO tb_ordens (id, setor,maquina,risco,status,problemaaparente,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem, descmanutencao, operador) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador]))
        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()

        return redirect(url_for('routes.Index'))

@routes_bp.route('/openOs')
def open_os(): # Página de abrir OS
    return render_template("user/openOs.html")

@routes_bp.route('/edit_material/<id_ordem>', methods = ['POST', 'GET'])
@login_required
def get_material(id_ordem): # Informar material que foi utilizado na ordem de serviço
    # Verifica se a requisição é um POST
    if request.method == 'POST':
        
        # Obtendo o ultimo id

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        s = ("""
            SELECT MAX(id) FROM tb_carrinho
        """)
        cur.execute(s)
        try:
            max_id = cur.fetchall()[0][0] + 1
        except:
            max_id = 0

        # Obtém os dados do formulário
        id_ordem = id_ordem
        codigo = request.form['codigo']
        quantidade = request.form['quantidade']
    
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("INSERT INTO tb_carrinho (id, id_ordem, codigo, quantidade) VALUES (%s,%s,%s,%s)", (max_id, id_ordem, codigo, quantidade))
        conn.commit()

    # Obtém os dados da tabela
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ("""
        SELECT tb_carrinho.id_ordem, tb_carrinho.codigo, tb_carrinho.quantidade, tb_material.descricao, tb_material.valor * tb_carrinho.quantidade AS total
        FROM tb_carrinho
        JOIN tb_material ON tb_carrinho.codigo = tb_material.codigo
        WHERE id_ordem = {}
    """).format(int(id_ordem))

    cur.execute(s)
    data = cur.fetchall()

    s = ("""
        SELECT SUM(valortotal.total) AS valor_total FROM
        (
        SELECT tb_carrinho.id_ordem, tb_carrinho.codigo, tb_carrinho.quantidade, tb_material.descricao, SUM(tb_material.valor * tb_carrinho.quantidade) AS total
        FROM tb_carrinho
        JOIN tb_material ON tb_carrinho.codigo = tb_material.codigo
        WHERE tb_carrinho.id_ordem = {}
        GROUP BY tb_carrinho.id_ordem, tb_carrinho.codigo, tb_carrinho.quantidade, tb_material.descricao
        ) AS valortotal; 
    """).format(int(id_ordem))
    
    cur.execute(s)
    valorTotal = cur.fetchall()

    #cur.close()

    return render_template('user/material.html', datas=data, id_ordem=id_ordem, valorTotal=valorTotal[0][0])

@routes_bp.route('/grafico')
@login_required
def grafico(): # Dashboard
    
    # lista com os gráficos a serem plotados
    plot_list = [] 

    ##### GRÁFICO 1 #####

    s = (""" 
        SELECT dataabertura, COUNT(id_ordem) as qt_os_abertas
        FROM tb_ordens
        WHERE dataabertura NOTNULL
        GROUP BY dataabertura
        """)

    grafico1 = pd.read_sql_query(s,conn)

    # Criação do gráfico
    trace = go.Bar(x=grafico1['dataabertura'], y=grafico1['qt_os_abertas'])
    data = [trace]
    #layout = go.Layout(title='Gráfico de Barras')
    fig = go.Figure(data=data)#, layout=layout)

    # Conversão do gráfico em HTML
    plot_div = opy.plot(fig, auto_open=False, output_type='div')

    plot_list.append(plot_div)

    ##### GRÁFICO 2 #####

    s = (""" 
        SELECT dataabertura, count(status)
        FROM tb_ordens
        WHERE dataabertura IS NOT NULL AND status = 'Em espera'
        GROUP BY dataabertura;
        """)

    grafico1 = pd.read_sql_query(s,conn)

    # Criação do gráfico
    trace = go.Bar(x=grafico1['dataabertura'], y=grafico1['count'])
    data = [trace]
    #layout = go.Layout(title='Gráfico de Barras')
    fig = go.Figure(data=data)#, layout=layout)

    # Conversão do gráfico em HTML
    plot_div = opy.plot(fig, auto_open=False, output_type='div')

    plot_list.append(plot_div)

    # Renderização do template com o gráfico
    return render_template('user/grafico.html', plot_list=plot_list)

@routes_bp.route('/timeline/<id_ordem>', methods=['POST', 'GET'])
@login_required
def timeline_os(id_ordem): # Mostrar o histórico daquela ordem de serviço

    # Obtém os dados da tabela
    s = ("""
        SELECT n_ordem, status, datainicio, datafim, operador,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE id_ordem = {}
    """).format(int(id_ordem))

    df_timeline = pd.read_sql_query(s, conn)

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)
    
    for i in range(len(df_timeline)):
        if df_timeline['fim'][i] == 'NaT':
            df_timeline['fim'][i] = 0
            df_timeline['inicio'][i] = 0
        else:
            pass

    df_timeline = df_timeline.replace(np.nan,'-')

    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        #df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

        for i in range(len(df_timeline)):
            df_timeline['operador'][i] = df_timeline['operador'][i].replace("{","").replace("[","").replace("\\","").replace('"', '').replace("]}","")
    except:
        df_timeline['diferenca'] = 0
    
    df_timeline = df_timeline.values.tolist()


    return render_template('user/timeline.html', id_ordem=id_ordem, df_timeline=df_timeline)

@routes_bp.route('/52semanas', methods=['POST','GET'])
@login_required
def plan_52semanas(): # Tabela com as 52 semanas
        
    if request.method == 'POST':
    
        # Obtendo o ultimo id

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        s = ("""
            SELECT MAX(id) FROM tb_maquinas
        """)
        cur.execute(s)
        try:
            max_id = cur.fetchall()[0][0] + 1
        except:
            max_id = 0

        # Obtém os dados do formulário
        codigo = request.form['codigo']
        descricao = request.form['descricao']
        setor = request.form['setor']
        criticidade = request.form['criticidade']
        manut_inicial = request.form['manut_inicial']
        periodicidade = request.form['periodicidade']
        
        s = ("""
            select codigo from tb_maquinas
        """.format(codigo))

        maquina_cadastrada = pd.read_sql_query(s,conn)

        if  len(maquina_cadastrada[maquina_cadastrada['codigo'] == codigo]) > 0:
            flash("Máquina ja cadastrada", category='danger')
        
            return redirect('/52semanas')
        
        else:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("INSERT INTO tb_maquinas (id, codigo, descricao, setor, criticidade, manut_inicial, periodicidade) VALUES (%s,%s,%s,%s,%s,%s,%s)", (max_id, codigo, descricao, setor, criticidade, manut_inicial, periodicidade))
            conn.commit()

            flash("Máquina cadastrada com sucesso", category='sucess')
            return redirect('/52semanas')

    s = (""" SELECT * FROM tb_maquinas """)

    df_maquinas = pd.read_sql_query(s, conn)
    
    df_final = pd.DataFrame()

    for i in range(len(df_maquinas)):    
        
        df_planejamento = gerador_de_semanas_informar_manutencao(df_maquinas['setor'][i], df_maquinas['codigo'][i], df_maquinas['descricao'][i], df_maquinas['criticidade'][i], df_maquinas['manut_inicial'][i], df_maquinas['periodicidade'][i])
        df_final = pd.concat([df_final, df_planejamento], axis=0)

    df_final = df_final.replace('','-')
    colunas = df_final.columns.tolist()
    df_final = df_final.values.tolist()

    return render_template('user/52semanas.html', data=df_final, colunas=colunas)

@routes_bp.route('/visualizar_imagem/<id_ordem>', methods=['GET'])
@login_required
def visualizar_imagem(id_ordem):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT imagem FROM tb_imagens WHERE id_ordem = %s", (id_ordem,))
    imagem_data = cur.fetchone()[0]

    imagem_base64 = base64.b64encode(imagem_data).decode('utf-8')

    return render_template('user/imagem.html', imagem_data=imagem_base64, id_ordem=id_ordem)