#app.py
from flask import Flask, current_app, send_file,jsonify, render_template, request, redirect, url_for, flash,Blueprint, Response, send_from_directory, send_file,current_app
import psycopg2 #pip install psycopg2 
import psycopg2.extras
import datetime
import pandas as pd
import numpy as np
import json
from funcoes import gerador_de_semanas_informar_manutencao, login_required
import warnings
from flask import session
import base64
from datetime import datetime, timedelta
from pandas.tseries.offsets import BMonthEnd
from psycopg2 import Error
import json
from PIL import Image
import io
from openpyxl import load_workbook

routes_bp = Blueprint('routes', __name__)

#routes_bp.config['UPLOAD_FOLDER'] = r'C:\Users\pcp2\projetoManutencao\appManutFlask-3\UPLOAD_FOLDER'

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

def calculo_indicadores(query):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    # query = ("""
    #     SELECT datafim, maquina, n_ordem,
    #         TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
    #         TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
    #     FROM tb_ordens
    #     WHERE 1=1 AND
    # """)

    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query, conn)

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
    SELECT * FROM tb_maquinas_preventivas
    """)

    df_maquinas = pd.read_sql_query(s, conn).drop_duplicates()
    df_maquinas = df_maquinas[['Código da máquina']]
    df_maquinas = df_maquinas.rename(columns={'Código da máquina':'maquina'})

    df_combinado = df_combinado.merge(df_maquinas, on='maquina')
    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 7
    
    df_combinado['MTBF'] = df_combinado['carga_trabalhada'] - df_combinado['diferenca'] / df_combinado['qtd_manutencao']
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    if len(df_combinado)> 0:

        grafico1_maquina = df_combinado['maquina'].tolist() # eixo x
        grafico1_mtbf = df_combinado['MTBF'].tolist() # eixo y gráfico 1
        grafico2_mttr = df_combinado['MTTR'].tolist() # eixo y grafico 2

        sorted_tuples = sorted(zip(grafico1_maquina, grafico1_mtbf), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_maquina, grafico1_mtbf = zip(*sorted_tuples)

        grafico1_maquina = list(grafico1_maquina)
        grafico1_mtbf = list(grafico1_mtbf)

        context = {'grafico1_maquina': grafico1_maquina, 'grafico1_mtbf': grafico1_mtbf,
                'grafico2_maquina':grafico1_maquina, 'grafico2_mttr':grafico2_mttr}
        
    else:

        grafico1_maquina = []
        grafico1_mtbf = []
        grafico2_mttr = []

        context = {'grafico1_maquina': grafico1_maquina, 'grafico1_mtbf': grafico1_mtbf,
            'grafico2_maquina':grafico1_maquina, 'grafico2_mttr':grafico2_mttr} 

    return context

def grafico_area(query):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    
    # query = ("""
    #     SELECT maquina, area_manutencao, datafim, id_ordem
    #     FROM tb_ordens
    # """)    

    df_area = pd.read_sql_query(query,conn)

    mes_atual = datetime.today().month

    df_area['mes'] = pd.to_datetime(df_area['datafim']).dt.month
    df_area = df_area[df_area['mes'] == mes_atual]
    df_area = df_area.drop_duplicates(subset='id_ordem', keep='last')
    df_area = df_area.dropna()
    df_area = df_area[['area_manutencao']]

    contagem = df_area.value_counts(subset='area_manutencao')
    df_area['qtde_area'] = df_area['area_manutencao'].map(contagem)
    
    area = df_area['area_manutencao'].values.tolist()
    quantidade_area = df_area['qtde_area'].values.tolist()

    pizza_context = {'pizza1_area': area, 'pizza1_quantidade':quantidade_area}        

    return pizza_context

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
    
def formulario_os(id_ordem):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT * FROM tb_ordens WHERE id_ordem = {}""".format(id_ordem)
    cur.execute(query)
    df.columns = pd.read_sql_query(query, conn)
    
    ultima_atualizacao = df['ultima_atualizacao'][0]

    wb = load_workbook('modelo_os_new.xlsx')
    ws = wb.active

    nova_hora_formatada = ultima_atualizacao.strftime('%H:%M')
    data_atual = ultima_atualizacao.strftime('%d/%m/%Y')

    ws['G8'] = data_atual
    ws['G9'] = nova_hora_formatada

    ws['B10'] = df['solicitante'][0]
    ws['B8'] = df['id_ordem'][0]
    ws['B9'] = df['setor'][0]
    ws['B11'] = df['maquina'][0]
    ws['B12'] = df['problemaaparente'][0]
    
    if df['maquina_parada'][0] == True:
        ws['G11'] = 'Sim'
    else:
        ws['G11'] = 'Não'
    
    df = df.drop_duplicates(subset=['id_ordem'], keep='last').reset_index()

    ws['G10'] = df['status'][0]

    wb.save('modelo_os_new.xlsx')

    # Caminho completo do arquivo gerado
    arquivo_gerado = 'modelo_os_new.xlsx'

    # Retorna o arquivo para download
    return send_file(arquivo_gerado, as_attachment=True)

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

    df = df[df['ordem_excluida'] != True]

    df['dataabertura'] = df['dataabertura'].fillna(method='ffill')
    df['dataabertura'] = df['dataabertura'].replace('', method='ffill')

    df = df.drop_duplicates(subset=['id_ordem'], keep='last')
    df = df.sort_values(by='id_ordem')
    df.reset_index(drop=True, inplace=True)

    for i in range(len(df)):
        if df['total'][i] == '':
            df['total'][i] = 0

    df['total'] = df['total'].apply(lambda x: round(x, 2))

    df = df.sort_values('ultima_atualizacao', ascending=False)

    df['ultima_atualizacao'] = pd.to_datetime(df['ultima_atualizacao'])
    df['ultima_atualizacao'] = df['ultima_atualizacao'].dt.strftime("%Y-%m-%d %H:%M:%S")

    list_users = df.values.tolist()

    return render_template('user/index.html', list_users = list_users)

@routes_bp.route('/add_student', methods=['POST', 'GET']) 
def add_student(): # Criar ordem de serviço
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    if request.method == 'POST':

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        setor = request.form['setor']
        maquina = request.form['maquina']
        problema = request.form['problema']
        solicitante = request.form['solicitante']
        dataAbertura = datetime.now()
        n_ordem = 0
        status = 'Em espera'

        try:
            risco = request.form['risco']
        except:
            risco = 'Baixo'

        try:
            maquina_parada = request.form['maquina-parada']

        except:
            maquina_parada = 'false'

        maquina = maquina.split(" - ")
        maquina = maquina[0]
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id) FROM tb_ordens")
        maior_valor = cur.fetchone()[0]

        try:
            maior_valor += 1
        except:
            maior_valor = 0

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT MAX(id_ordem) FROM tb_ordens")
        ultima_os = cur.fetchone()[0]

        try:
            ultima_os = ultima_os+1
        except:
            ultima_os = 0

        cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco,status, problemaaparente, id_ordem, n_ordem ,dataabertura, maquina_parada,solicitante) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (maior_valor, setor, maquina, risco, status, problema, ultima_os, n_ordem, dataAbertura, maquina_parada,solicitante))

        imagem = request.files['imagem']
        
        if imagem.filename != '':             
            
            # Ler os dados da imagem
            imagem_data = imagem.read()

            # Abrir a imagem usando a biblioteca Pillow
            image = Image.open(io.BytesIO(imagem_data))

            # Redimensionar a imagem para um tamanho desejado
            max_size = (800, 600)
            image.thumbnail(max_size)

            # Salvar a imagem com uma qualidade reduzida
            buffer = io.BytesIO()
            image.save(buffer, format='JPEG', quality=80)
            imagem_data_comprimida = buffer.getvalue()

            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("INSERT INTO tb_imagens (id_ordem, imagem) VALUES (%s,%s)", (ultima_os, imagem_data_comprimida))
            conn.commit()

        else:
            print('sem imagem')             
            conn.commit()

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
    area_manutencao = data1['area_manutencao'].values.tolist()[0]

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

    query = """SELECT * FROM tb_funcionario"""
    tb_funcionarios = pd.read_sql_query(query, conn)
    tb_funcionarios['matricula_nome'] = tb_funcionarios['matricula'] + " - " + tb_funcionarios['nome']
    tb_funcionarios = tb_funcionarios[['matricula_nome']].values.tolist()
    
    return render_template('user/edit.html', ordem=data1[0], tb_funcionarios=tb_funcionarios, opcoes=opcoes, tipo_manutencao=tipo_manutencao, area_manutencao=area_manutencao)
 
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
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        s = (""" 
            SELECT natureza FROM tb_ordens where id_ordem = {}
        """).format(id_ordem)
        
        df = pd.read_sql_query(s, conn)

        natureza = df['natureza'][0]

        setor = request.form['setor']
        maquina = request.form['maquina']
        risco = request.form['risco']
        status = request.form['statusLista']
        problema = request.form['problema']
        id_ordem = id_ordem
        n_ordem = request.form['n_ordem']
        descmanutencao = request.form['descmanutencao']
        operador = request.form.getlist('operador')
        operador = json.dumps(operador)
        tipo_manutencao = request.form['tipo_manutencao']
        datetimes = request.form['datetimes']
        area_manutencao = request.form['area_manutencao']
        natureza = natureza
        print(operador)

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
            INSERT INTO tb_ordens (id, setor,maquina,risco,status,problemaaparente,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem, descmanutencao, operador, natureza, tipo_manutencao, area_manutencao) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador], natureza, tipo_manutencao, area_manutencao))
        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()

        return redirect(url_for('routes.get_employee', id_ordem=id_ordem))

@routes_bp.route('/openOs')
def open_os(): # Página de abrir OS

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT * FROM tb_matriculas"""

    cur.execute(query)
    data = cur.fetchall()
    df_data = pd.DataFrame(data, columns=['id','matricula','nome'])

    df_data['matricula_nome'] = df_data['matricula'] + " - " + df_data['nome']

    solicitantes = df_data[['matricula_nome']].values.tolist()

    return render_template("user/openOs.html", solicitantes=solicitantes)

@routes_bp.route('/maquinas/<setor>')
def filtro_maquinas(setor):
   
    #setor = setor.upper()

    print(setor)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """
        SELECT * FROM tb_maquinas WHERE setor = {}
    """.format("'" + setor + "'")

    lista_maquinas = pd.read_sql_query(query, conn)
    
    lista_maquinas['codigo_desc'] = lista_maquinas['codigo'] + " - " + lista_maquinas['descricao']
    lista_maquinas = lista_maquinas.dropna(subset=['codigo_desc'])
    lista_maquinas = lista_maquinas.drop_duplicates(subset=['codigo'])
    
    lista_maquinas_ = []
    lista_maquinas_.insert(0, 'Outros')
    lista_maquinas_.extend(lista_maquinas[['codigo_desc']].values.tolist())

    return jsonify(lista_maquinas_)

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

    cur.execute("SELECT DISTINCT setor FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE;")
    setores = cur.fetchall()

    cur.execute("SELECT * FROM tb_maquinas_preventivas")
    name_cols = ['codigo','tombamento','setor','descricao','criticidade','periodicidade']
    df_maquinas = pd.DataFrame(cur.fetchall()).iloc[:,:6]
    df_maquinas = df_maquinas.rename(columns=dict(zip(df_maquinas.columns, name_cols)))
    maquinas = df_maquinas.values.tolist()

    query = "SELECT * FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE"

    df_cards = pd.read_sql_query(query, conn)
    df_cards = df_cards.drop_duplicates(subset='id_ordem', keep='last')
    df_cards['status'] = df_cards['status'].apply(lambda x: x.split("  ")[0])

    espera = df_cards[df_cards['status'] == 'Em espera'].shape[0]
    material = df_cards[df_cards['status'] == 'Aguardando material'].shape[0]
    finalizado = df_cards[df_cards['status'] == 'Finalizada'].shape[0]
    execucao = df_cards[df_cards['status'] == 'Em execução'].shape[0]

    lista_qt = [espera,material,finalizado,execucao]

    query = ("""
        SELECT datafim, maquina, n_ordem,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE
    """)

    context = calculo_indicadores(query)

    query = ("""
        SELECT maquina, area_manutencao, datafim, id_ordem
        FROM tb_ordens
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE
    """)  

    pizza_context = grafico_area(query)

    if request.method == 'POST':

        setor_selecionado = request.form.get('filtro_setor')
        maquina_selecionado = request.form.get('filtro_maquinas')
        area_manutencao = request.form.get('area_manutencao')

        if not setor_selecionado:
            setor_selecionado = ''
        if not maquina_selecionado:
            maquina_selecionado = ''
        if not area_manutencao:
            area_manutencao = ''
    
        try:
            maquina_selecionado = maquina_selecionado.split(" - ")[0]
        except:
            pass

        # Monta a query base
        query = "SELECT * FROM tb_ordens WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE"

        # Adiciona as condições de filtro se os campos não estiverem vazios
        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        # Executa a query
        cur.execute(query)
        itens_filtrados = cur.fetchall()
    
        # Criando cards

        df_cards = pd.read_sql_query(query, conn)
        df_cards = df_cards.drop_duplicates(subset='id_ordem', keep='last')
        df_cards['status'] = df_cards['status'].apply(lambda x: x.split("  ")[0])

        espera = df_cards[df_cards['status'] == 'Em espera'].shape[0]
        material = df_cards[df_cards['status'] == 'Aguardando material'].shape[0]
        finalizado = df_cards[df_cards['status'] == 'Finalizada'].shape[0]
        execucao = df_cards[df_cards['status'] == 'Em execução'].shape[0]

        lista_qt = [espera,material,finalizado,execucao]

        query = ("""
        SELECT datafim,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        df_tempos = tempo_os2(query)

        if len(df_tempos) == 0:
            df_tempos = tempo_os()

        df_tempos['datafim'] = df_tempos['datafim'].astype(str)

        query = ("""
        SELECT datafim, maquina, n_ordem,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens 
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        context = calculo_indicadores(query)

        query = ("""
        SELECT maquina, area_manutencao, datafim, id_ordem
        FROM tb_ordens 
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE
        """)    

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if maquina_selecionado:
            query += f" AND maquina = '{maquina_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        pizza_context = grafico_area(query)

        return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, maquinas=maquinas, itens_filtrados=itens_filtrados,
                               setor_selecionado=setor_selecionado, maquina_selecionado=maquina_selecionado, **context, **pizza_context,
                               area_manutencao=area_manutencao)
    
    # Se o método for GET, exibe todos os itens
    cur.execute("SELECT * FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE")
    itens = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, maquinas=maquinas, itens=itens, **context, **pizza_context,setor_selecionado='', maquina_selecionado='', area_manutencao='')

@routes_bp.route('/timeline/<id_ordem>', methods=['POST', 'GET'])
@login_required
def timeline_os(id_ordem): # Mostrar o histórico daquela ordem de serviço
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Obtém os dados da tabela
    s = ("""
        SELECT dataabertura, n_ordem, status, datainicio, datafim, operador, descmanutencao,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE id_ordem = {} AND ordem_excluida IS NULL OR ordem_excluida = FALSE
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

    if df_timeline['datainicio'][0] == '-':
        df_timeline['datainicio'][0] = df_timeline['dataabertura'][0]

    df_timeline = df_timeline.iloc[:,1:]

    df_timeline = df_timeline.values.tolist()

    return render_template('user/timeline.html', id_ordem=id_ordem, df_timeline=df_timeline)

@routes_bp.route('/52semanas', methods=['GET'])
@login_required
def plan_52semanas(): # Tabela com as 52 semanas
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    s = (""" SELECT * FROM tb_maquinas_preventivas """)

    df_maquinas = pd.read_sql_query(s, conn)
    
    colunas = df_maquinas.columns.tolist()
    df_maquinas = df_maquinas.values.tolist()

    return render_template('user/52semanas.html', data=df_maquinas, colunas=colunas)

@routes_bp.route('/cadastrar52', methods=['POST', 'GET'])
@login_required
def cadastro_preventiva():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == 'POST':
            
        try:
            togglePreventiva = request.form['cadastrar-preventiva']
            codigo = request.form['codigo']
            tombamento = request.form['tombamento']
            descricao = request.form['descricao']
            setor = request.form['setor']
            criticidade = request.form['criticidade']
            manut_inicial = request.form['manut_inicial']
            periodicidade = request.form['periodicidade']

            df = gerador_de_semanas_informar_manutencao(setor,codigo,descricao,tombamento,criticidade,manut_inicial,periodicidade)
        
            lista = df.values.tolist()
            lista = lista[0]

            s = ("""
                SELECT * FROM tb_maquinas_preventivas
                """)

            maquina_cadastrada = pd.read_sql_query(s,conn)

            if len(maquina_cadastrada[maquina_cadastrada['Código da máquina'] == codigo]) > 0:
                flash("Máquina ja cadastrada", category='danger')        
            
            else:

                if 'pdf' in request.files:
                    pdf = request.files['pdf']
                    # Ler os dados do arquivo PDF
                    pdf_data = pdf.read()

                    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    cur.execute("INSERT INTO tb_anexos (codigo_maquina, checklist) VALUES (%s,%s)", (codigo, pdf_data))
                    conn.commit()
                else:
                    pass

                if 'imagem' in request.files:
                    imagem = request.files['imagem']
                    # Ler os dados do arquivo PDF
                    imagem_data = imagem.read()

                    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    cur.execute("INSERT INTO tb_anexos (codigo_maquina, imagem) VALUES (%s,%s)", (codigo, imagem_data))
                    conn.commit()
                else:
                    pass

                try:
                    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
                    
                    # Consulta SQL para inserir os dados na tabela
                    sql_insert = "INSERT INTO tb_maquinas_preventivas VALUES ({})".format(','.join(['%s'] * len(lista)))

                    # Criar o cursor para executar a consulta SQL
                    cursor = conn.cursor()

                    # Executar a consulta SQL com a lista de dados
                    cursor.execute(sql_insert, lista)

                    # Confirmar a transação
                    conn.commit()

                    print("Dados inseridos com sucesso na tabela.")

                except Error as e:
                    print(f"Ocorreu um erro ao conectar ou executar a consulta no PostgreSQL: {e}")

                finally:
                    # Fechar o cursor e a conexão com o banco de dados
                    cursor.close()
                    conn.close()

                flash("Máquina cadastrada com sucesso", category='sucess')
            
            return render_template('user/cadastrar52.html')

        except:
            
            togglePreventiva = 'false'
            codigo = request.form['codigo']
            tombamento = request.form['tombamento']
            descricao = request.form['descricao']
            setor = request.form['setor']

            s = ("""
                SELECT * FROM tb_maquinas
                """)
            
            query_max = ("""SELECT max(id) FROM tb_maquinas""")
            cur.execute(query_max)
            id = cur.fetchall()
            id = id[0][0] + 1

            maquina_cadastrada = pd.read_sql_query(s,conn)

            if len(maquina_cadastrada[maquina_cadastrada['codigo'] == codigo]) > 0:
                flash("Máquina ja cadastrada", category='danger')        

            else:

                if 'pdf' in request.files:
                    pdf = request.files['pdf']
                    # Ler os dados do arquivo PDF
                    pdf_data = pdf.read()
                else:
                    pdf_data = None

                if 'imagem' in request.files:
                    imagem = request.files['imagem']
                    # Ler os dados do arquivo de imagem
                    imagem_data = imagem.read()
                else:
                    imagem_data = None

                if 'documento' in request.files:
                    documento = request.files['documento']
                    # Ler os dados do arquivo de imagem
                    documento_data = documento.read()
                else:
                    documento_data = None

                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                
                cur.execute("INSERT INTO tb_anexos (codigo_maquina, checklist, imagem, documento) VALUES (%s, %s, %s, %s)",
                             (codigo, pdf_data, imagem_data, documento_data))

                cur.execute("INSERT INTO tb_maquinas (id, setor, codigo, descricao, tombamento) VALUES (%s,%s, %s, %s, %s)",
                            (id, setor, codigo, descricao, tombamento))
                
                conn.commit()

                flash("Máquina cadastrada com sucesso", category='sucess')
                
            return render_template('user/cadastrar52.html')
        
    return render_template('user/cadastrar52.html')

@routes_bp.route('/visualizar_imagem/<id_ordem>', methods=['GET'])
@login_required
def visualizar_imagem(id_ordem):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT imagem FROM tb_imagens WHERE id_ordem = %s", (id_ordem,))
    imagem_data = cur.fetchone()[0]

    imagem_base64 = base64.b64encode(imagem_data).decode('utf-8')

    return jsonify(imagem_data=imagem_base64, id_ordem=id_ordem)

@routes_bp.route('/timeline-preventiva/<maquina>', methods=['POST', 'GET'])
@login_required
def timeline_preventiva(maquina): # Mostrar o histórico de preventiva daquela máquina
    
    print(maquina)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Obtém os dados da tabela
    s = ("""
        SELECT * 
        FROM tb_ordens
        WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE
        """)
    
    df = pd.read_sql_query(s, conn)
    df['maquina'] = df['maquina'].str.strip()

    df = df[df['maquina'] == maquina]
    df = df[df['natureza'] == 'Planejada'].reset_index(drop=True)

    df[['dataabertura','id_ordem']]

    # Limpar a coluna
    for i in range(len(df)):
        try:
            df['operador'][i] = df['operador'][i].replace("{","").replace("}","").replace("[","").replace("]","").replace("\\","").replace('"', '').replace("]}","").replace("}}","")
        except:
            pass

    # Criar um dicionário para mapear cada ID à sua respectiva data
    id_data_map = {}

    # Iterar sobre os IDs únicos e encontrar a data correspondente para cada um
    for id_ordem in df['id_ordem'].unique():
        data = df.loc[df['id_ordem'] == id_ordem, 'dataabertura'].iloc[0]
        id_data_map[id_ordem] = data

    # Atualizar os valores de dataabertura para cada ID
    df['dataabertura'] = df['id_ordem'].map(id_data_map)

    df = df.drop_duplicates(subset='id_ordem', keep='last')
    
    df = df.sort_values('id_ordem', ascending=True)

    data = df.values.tolist()

    return render_template('user/timeline_preventiva.html', data=data, maquina=maquina)
 
@routes_bp.route('/mostrar_pdf/<codigo_maquina>', methods=['GET'])
@login_required
def mostrar_pdf(codigo_maquina):
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT checklist FROM tb_anexos WHERE codigo_maquina = %s", (codigo_maquina,))
        pdf_data = cur.fetchone()

        if pdf_data:
            # Configurar o cabeçalho da resposta para indicar que é um arquivo PDF
            headers = {'Content-Type': 'application/pdf',
                       'Content-Disposition': 'inline; filename=arquivo.pdf'}

            return Response(pdf_data['checklist'], headers=headers)
        else:
            raise Exception('Arquivo PDF não encontrado.')
    except Exception as e:
        flash(str(e))
        return redirect(url_for('routes.plan_52semanas'))
    
@routes_bp.route('/lista_maquinas', methods=['GET'])
@login_required
def lista_maquinas():

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(""" SELECT "Código da máquina","Grupo", "Descrição da máquina" 
                FROM tb_maquinas_preventivas """)
    
    df_c_preventivas = pd.DataFrame(cur.fetchall(), columns=['codigo','setor','descricao'])
    df_c_preventivas['setor'] = df_c_preventivas['setor'].str.title() 
    df_c_preventivas['preventiva'] = 'Y'

    cur.execute(""" SELECT codigo, setor, descricao 
                FROM tb_maquinas """)
    
    df_s_preventivas = pd.DataFrame(cur.fetchall(), columns=['codigo','setor','descricao'])
    df_s_preventivas['setor'] = df_s_preventivas['setor'].str.title() 
    df_s_preventivas['preventiva'] = 'N'

    df_final = pd.concat([df_c_preventivas,df_s_preventivas]).drop_duplicates(subset='codigo',keep='first').reset_index(drop=True)
    data = df_final.values.tolist()

    df_final[df_final['setor'] == 'Pintura']

    return render_template('user/lista_maquinas.html', data=data)

@routes_bp.route('/excluir-ordem', methods=['POST'])
@login_required
def excluir_ordem():
    data = request.get_json()
    id_linha = data['id']
    texto = data['texto']
    
    query = """UPDATE tb_ordens
            SET ordem_excluida = 'yes', motivo_exclusao = %s
            WHERE id_ordem = %s
            """

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, [texto, id_linha])
    conn.commit()

    print(id_linha,texto)

    return 'Dados recebidos com sucesso!'

@routes_bp.route('/visualizar_pdf/<id_ordem>')
def visualizar_pdf(id_ordem):
    
    return formulario_os(id_ordem)