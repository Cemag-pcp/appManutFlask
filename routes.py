#app.py
from flask import Flask, jsonify, render_template, request, redirect, url_for, flash,Blueprint, Response
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
from datetime import datetime
from pandas.tseries.offsets import BMonthEnd

routes_bp = Blueprint('routes', __name__)

warnings.filterwarnings("ignore")
 
# DB_HOST = "localhost"
DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

def dias_uteis():
    
    # Obter a data atual
    data_atual = pd.Timestamp.now()
    
    # Obter o primeiro dia do mês atual
    primeiro_dia_mes = data_atual - pd.offsets.MonthBegin()

    # Obter o último dia útil do mês atual
    ultimo_dia_util_mes = primeiro_dia_mes + BMonthEnd()

    # Obter a sequência de datas úteis no mês atual
    datas_uteis = pd.bdate_range(primeiro_dia_mes, ultimo_dia_util_mes)

    # Contar o número de dias úteis
    qtd_dias_uteis = len(datas_uteis)

    return qtd_dias_uteis

def tempo_os():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    s = ("""
        SELECT datafim,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
    """)

    df_timeline = pd.read_sql_query(s, conn)

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)
    
    df_timeline = df_timeline.dropna()

    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        #df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0
    
    df_timeline = df_timeline[['datafim','diferenca']]
    df_agrupado = df_timeline.groupby('datafim')['diferenca'].sum().reset_index()

    # df_timeline = df_timeline.values.tolist()

    return df_agrupado

def calculo_indicadores():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    s = ("""
        SELECT datafim, maquina, n_ordem,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
    """)

    data_hoje = datetime.today()
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(s, conn)

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)
    
    df_timeline = df_timeline.dropna()

    df_timeline['datafim'] = pd.to_datetime(df_timeline['datafim'])
    df_timeline['mes'] = df_timeline['datafim'].dt.month

    df_timeline = df_timeline[df_timeline['mes'] == mes_hoje]

    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        #df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0
    
    # df_timeline = df_timeline[['datafim','diferenca']]
    
    df_timeline['maquina'] = df_timeline['maquina'].str.strip()
    df_agrupado_tempo = df_timeline.groupby(['maquina'])['diferenca'].sum().reset_index()

    df_agrupado_qtd = df_timeline[['maquina']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['maquina'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['maquina'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='maquina')

    s = ("""
    SELECT codigo FROM tb_maquinas
    """)

    df_maquinas = pd.read_sql_query(s, conn).drop_duplicates()
    df_maquinas = df_maquinas.rename(columns={'codigo':'maquina'})

    df_combinado = df_combinado.merge(df_maquinas, on='maquina')
    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 7
    
    df_combinado['MTBF'] = df_combinado['carga_trabalhada'] - df_combinado['diferenca'] / df_combinado['qtd_manutencao']
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    return df_combinado

def tempo_os2(query):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    s = (query)

    df_timeline = pd.read_sql_query(s, conn)

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)
    
    df_timeline = df_timeline.dropna()

    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        #df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0
    
    df_timeline = df_timeline[['datafim','diferenca']]
    df_agrupado = df_timeline.groupby('datafim')['diferenca'].sum().reset_index()

    # df_timeline = df_timeline.values.tolist()

    return df_agrupado

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
        dataAbertura = datetime.now()
        n_ordem = 0
        status = 'Em espera'

        maquina = maquina.split(" - ")
        maquina = maquina[0]
        
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

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

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
    tipo_manutencao = data1['tipo_manutencao'].values.tolist()[0]
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

    return render_template('user/edit.html', ordem=data1[0], opcoes=opcoes, tipo_manutencao=tipo_manutencao)
 
@routes_bp.route('/update/<id_ordem>', methods=['POST'])
@login_required
def update_student(id_ordem): # Inserir as edições no banco de dados

    # # Execute a instrução SQL para alterar o tipo da coluna
    # alter_query = "ALTER TABLE tb_ordens ALTER COLUMN tipo_manutencao TYPE TEXT;"
    # cur.execute(alter_query)
    # conn.commit()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

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
        
        setor = request.form['setor']
        maquina = request.form['maquina']
        risco = request.form['risco']
        status = request.form['statusLista']
        problema = request.form['problema']
        # datainicio = request.form['datainicio']
        # horainicio = request.form['horainicio']
        # datafim = request.form['datafim']
        # horafim = request.form['horafim']
        id_ordem = id_ordem
        n_ordem = request.form['n_ordem']
        descmanutencao = request.form['descmanutencao']
        operador = request.form.getlist('operador')
        operador = json.dumps(operador)
        tipo_manutencao = request.form['tipo_manutencao']
        datetimes = request.form['datetimes']
        # print(datetimes)

        # Divida a string em duas partes: data/hora inicial e data/hora final
        data_hora_inicial_str, data_hora_final_str = datetimes.split(" - ")

        # Faça o parsing das strings de data e hora
        data_inicial = datetime.strptime(data_hora_inicial_str, "%d/%m/%y %I:%M %p")
        data_final = datetime.strptime(data_hora_final_str, "%d/%m/%y %I:%M %p")

        # Formate as datas e horas no formato desejado
        datainicio = data_inicial.strftime("%Y-%m-%d")
        horainicio = data_inicial.strftime("%H:%M:%S")
        datafim = data_final.strftime("%Y-%m-%d")
        horafim = data_final.strftime("%H:%M:%S")

        # print(datainicio, horainicio, datafim, horafim)

        #print(ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador])

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            INSERT INTO tb_ordens (id, setor,maquina,risco,status,problemaaparente,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem, descmanutencao, operador, tipo_manutencao) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador],tipo_manutencao))
        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()

        return redirect(url_for('routes.Index'))

@routes_bp.route('/openOs')
def open_os(): # Página de abrir OS

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """
        SELECT * FROM tb_maquinas
    """

    lista_maquinas = pd.read_sql_query(query, conn)

    lista_maquinas['codigo_desc'] = lista_maquinas['codigo'] + " - " + lista_maquinas['descricao']
    lista_maquinas = lista_maquinas[['codigo_desc']].values.tolist()

    return render_template("user/openOs.html", lista_maquinas=lista_maquinas)

@routes_bp.route('/edit_material/<id_ordem>', methods = ['POST', 'GET'])
@login_required
def get_material(id_ordem): # Informar material que foi utilizado na ordem de serviço
    # Verifica se a requisição é um POST

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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

@routes_bp.route('/grafico', methods=['POST', 'GET'])
@login_required
def grafico(): # Dashboard
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT DISTINCT setor FROM tb_ordens")
    setores = cur.fetchall()

    cur.execute("SELECT DISTINCT CONCAT(codigo, ' - ', descricao) AS codigo_concatenado FROM tb_maquinas")
    maquinas = cur.fetchall()

    query = "SELECT * FROM tb_ordens"

    df_cards = pd.read_sql_query(query, conn)
    df_cards = df_cards.drop_duplicates(subset='id_ordem', keep='last')
    df_cards['status'] = df_cards['status'].apply(lambda x: x.split("  ")[0])

    espera = df_cards[df_cards['status'] == 'Em espera'].shape[0]
    material = df_cards[df_cards['status'] == 'Aguardando material'].shape[0]
    finalizado = df_cards[df_cards['status'] == 'Finalizado'].shape[0]
    execucao = df_cards[df_cards['status'] == 'Em execução'].shape[0]

    lista_qt = [espera,material,finalizado,execucao]

    df_tempos = calculo_indicadores()

    # df_tempos['datafim'] = df_tempos['datafim'].astype(str)

    grafico1_maquina = df_tempos['maquina'].tolist() # eixo x
    grafico1_mtbf = df_tempos['MTBF'].tolist() # eixo y gráfico 1
    grafico2_mttr = df_tempos['MTTR'].tolist() # eixo y grafico 2
    
    sorted_tuples = sorted(zip(grafico1_maquina, grafico1_mtbf), key=lambda x: x[0])

    # Desempacotar as tuplas classificadas em duas listas separadas
    grafico1_maquina, grafico1_mtbf = zip(*sorted_tuples)

    grafico1_maquina = list(grafico1_maquina)
    grafico1_mtbf = list(grafico1_mtbf)

    context = {'grafico1_maquina': grafico1_maquina, 'grafico1_mtbf': grafico1_mtbf,
               'grafico2_maquina':grafico1_maquina, 'grafico2_mttr':grafico2_mttr}

    if request.method == 'POST':
        setor_selecionado = request.form.get('setor')
        maquina_selecionado = request.form.get('maquina')

        # Monta a query base
        query = "SELECT * FROM tb_ordens WHERE 1=1"

        # Adiciona as condições de filtro se os campos não estiverem vazios
        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"

        # Executa a query
        cur.execute(query)
        itens_filtrados = cur.fetchall()
    
        # Criando cards

        df_cards = pd.read_sql_query(query, conn)
        df_cards = df_cards.drop_duplicates(subset='id_ordem', keep='last')
        df_cards['status'] = df_cards['status'].apply(lambda x: x.split("  ")[0])

        espera = df_cards[df_cards['status'] == 'Em espera'].shape[0]
        material = df_cards[df_cards['status'] == 'Aguardando material'].shape[0]
        finalizado = df_cards[df_cards['status'] == 'Finalizado'].shape[0]
        execucao = df_cards[df_cards['status'] == 'Em execução'].shape[0]

        lista_qt = [espera,material,finalizado,execucao]

        query = ("""
        SELECT datafim,
        TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
        TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE 1=1
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"

        df_tempos = tempo_os2(query)

        if len(df_tempos) == 0:
            df_tempos = tempo_os()

        df_tempos['datafim'] = df_tempos['datafim'].astype(str)

        return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, maquinas=maquinas, itens_filtrados=itens_filtrados,
                               setor_selecionado=setor_selecionado, maquina_selecionado=maquina_selecionado, **context)
    
    # Se o método for GET, exibe todos os itens
    cur.execute("SELECT * FROM tb_ordens")
    itens = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, maquinas=maquinas, itens=itens, **context)

@routes_bp.route('/timeline/<id_ordem>', methods=['POST', 'GET'])
@login_required
def timeline_os(id_ordem): # Mostrar o histórico daquela ordem de serviço
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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
    
    df_timeline = df_timeline.sort_values(by='n_ordem', ascending=True)

    df_timeline = df_timeline.values.tolist()

    return render_template('user/timeline.html', id_ordem=id_ordem, df_timeline=df_timeline)

@routes_bp.route('/52semanas', methods=['POST','GET'])
@login_required
def plan_52semanas(): # Tabela com as 52 semanas
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

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

    return jsonify(imagem_data=imagem_base64, id_ordem=id_ordem)