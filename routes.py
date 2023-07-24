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
# import convertapi

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
    datas_uteis = pd.bdate_range(primeiro_dia_mes, data_atual)

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

def mtbf_maquina(query_mtbf):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    df_timeline = pd.read_sql_query(query_mtbf, conn)

    df_timeline = df_timeline[df_timeline['n_ordem'] == 0]

    df_timeline['dataabertura'] = pd.to_datetime(df_timeline['dataabertura'])
    df_timeline['mes'] = df_timeline['dataabertura'].dt.month

    mes_hoje = datetime.today().month
    df_timeline = df_timeline[df_timeline['mes'] == mes_hoje]

    df_timeline = df_timeline.dropna()
            
    df_timeline['maquina'] = df_timeline['maquina']
    df_timeline['maquina'] = df_timeline['maquina'].str.split(' - ').str[0]
    
    df_timeline['maquina'].value_counts()
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_timeline['maquina'].value_counts()
    df_timeline['qtd_manutencao'] = df_timeline['maquina'].map(contagem)
    df_timeline = df_timeline.drop_duplicates(subset='maquina')

    qtd_dias_uteis = dias_uteis()

    df_timeline['carga_trabalhada'] = qtd_dias_uteis * 9
    
    df_timeline['MTBF'] = (df_timeline['carga_trabalhada']) / df_timeline['qtd_manutencao']

    if len(df_timeline)> 0:

        grafico1_maquina = df_timeline['maquina'].tolist() # eixo x
        grafico1_mtbf = df_timeline['MTBF'].tolist() # eixo y gráfico 1

        sorted_tuples = sorted(zip(grafico1_maquina, grafico1_mtbf), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_maquina, grafico1_mtbf = zip(*sorted_tuples)

        grafico1_maquina = list(grafico1_maquina)
        grafico1_mtbf = list(grafico1_mtbf)

        context_mtbf_maquina = {'labels_mtbf_maquina': grafico1_maquina, 'dados_mtbf_maquina': grafico1_mtbf}        
    else:

        grafico1_maquina = []
        grafico1_mtbf = []

        context_mtbf_maquina = {'labels_mtbf_maquina': grafico1_maquina, 'dados_mtbf_maquina': grafico1_mtbf}

    return context_mtbf_maquina

def mtbf_setor(query_mtbf):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    df_timeline = pd.read_sql_query(query_mtbf, conn)

    df_timeline = df_timeline[df_timeline['n_ordem'] == 0]

    df_timeline['dataabertura'] = pd.to_datetime(df_timeline['dataabertura'])
    df_timeline['mes'] = df_timeline['dataabertura'].dt.month

    mes_hoje = datetime.today().month
    df_timeline = df_timeline[df_timeline['mes'] == mes_hoje]

    df_timeline = df_timeline.dropna()
    
    df_timeline['setor'].value_counts()
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_timeline['setor'].value_counts()
    df_timeline['qtd_manutencao'] = df_timeline['setor'].map(contagem)
    df_timeline = df_timeline.drop_duplicates(subset='setor')

    qtd_dias_uteis = dias_uteis()

    df_timeline['carga_trabalhada'] = qtd_dias_uteis * 9
    
    df_timeline['MTBF'] = (df_timeline['carga_trabalhada']) / df_timeline['qtd_manutencao']

    if len(df_timeline)> 0:

        grafico1_maquina = df_timeline['setor'].tolist() # eixo x
        grafico1_mtbf = df_timeline['MTBF'].tolist() # eixo y gráfico 1

        sorted_tuples = sorted(zip(grafico1_maquina, grafico1_mtbf), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_maquina, grafico1_mtbf = zip(*sorted_tuples)

        grafico1_maquina = list(grafico1_maquina)
        grafico1_mtbf = list(grafico1_mtbf)

        context_mtbf_setor = {'labels_mtbf_setor': grafico1_maquina, 'dados_mtbf_setor': grafico1_mtbf}        
    else:

        grafico1_maquina = []
        grafico1_mtbf = []

        context_mtbf_setor = {'labels_mtbf_setor': grafico1_maquina, 'dados_mtbf_setor': grafico1_mtbf}

    return context_mtbf_setor

def mttr_maquina(query_mttr):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query_mttr, conn)

    df_timeline = df_timeline.sort_values(by='n_ordem')

    df_timeline = df_timeline.dropna()

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
    
    df_timeline['maquina'] = df_timeline['maquina']
    df_timeline['maquina'] = df_timeline['maquina'].str.split(' - ').str[0]
    
    df_agrupado_tempo = df_timeline.groupby(['maquina'])['diferenca'].sum().reset_index()
    df_agrupado_tempo['maquina'] = df_agrupado_tempo['maquina'].str.split(' - ').str[0]

    df_agrupado_qtd = df_timeline[['maquina']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['maquina'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['maquina'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='maquina')

    s = ("""
    SELECT * FROM tb_maquinas
    """)

    df_maquinas = pd.read_sql_query(s, conn).drop_duplicates()
    df_maquinas = df_maquinas[['codigo']]
    df_maquinas = df_maquinas.rename(columns={'codigo':'maquina'})

    df_combinado = df_combinado.merge(df_maquinas, on='maquina')
    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 9
    
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    if len(df_combinado)> 0:

        grafico1_maquina = df_combinado['maquina'].tolist() # eixo x
        grafico2_mttr = df_combinado['MTTR'].tolist() # eixo y grafico 2

        sorted_tuples = sorted(zip(grafico1_maquina, grafico2_mttr), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_maquina, grafico2_mttr = zip(*sorted_tuples)

        grafico1_maquina = list(grafico1_maquina)
        grafico2_mttr = list(grafico2_mttr)

        context_mtbf_maquina = {'labels_mttr_maquina':grafico1_maquina, 'dados_mttr_maquina':grafico2_mttr}
        
    else:

        grafico1_maquina = []
        grafico2_mttr = []

        context_mtbf_maquina = {'labels_mttr_maquina':grafico1_maquina, 'dados_mttr_maquina':grafico2_mttr} 

    return context_mtbf_maquina

def mttr_setor(query_mttr):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query_mttr, conn)

    df_timeline = df_timeline.sort_values(by='n_ordem')

    df_timeline = df_timeline.dropna()

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
    
    df_agrupado_tempo = df_timeline.groupby(['setor'])['diferenca'].sum().reset_index()
    
    df_agrupado_qtd = df_timeline[['setor']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['setor'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['setor'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='setor')

    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 9
    
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    if len(df_combinado)> 0:

        grafico1_maquina = df_combinado['setor'].tolist() # eixo x
        grafico2_mttr = df_combinado['MTTR'].tolist() # eixo y grafico 2

        sorted_tuples = sorted(zip(grafico1_maquina, grafico2_mttr), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_maquina, grafico2_mttr = zip(*sorted_tuples)

        grafico1_maquina = list(grafico1_maquina)
        grafico2_mttr = list(grafico2_mttr)

        context_mtbf_setor = {'labels_mttr_setor':grafico1_maquina, 'dados_mttr_setor':grafico2_mttr}
        
    else:

        grafico1_maquina = []
        grafico2_mttr = []

        context_mtbf_setor = {'labels_mttr_setor':grafico1_maquina, 'dados_mttr_setor':grafico2_mttr} 

    return context_mtbf_setor



    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
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
    
    df_timeline['setor'] = df_timeline['setor']
    df_timeline['setor'] = df_timeline['setor'].str.split(' - ').str[0]
    
    df_agrupado_tempo = df_timeline.groupby(['setor'])['diferenca'].sum().reset_index()
    df_agrupado_tempo['setor'] = df_agrupado_tempo['setor'].str.split(' - ').str[0]

    df_agrupado_qtd = df_timeline[['setor']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['setor'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['setor'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='setor')

    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 7
    
    df_combinado['MTBF'] = (df_combinado['carga_trabalhada'] - df_combinado['diferenca']) / df_combinado['qtd_manutencao']
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    if len(df_combinado)> 0:

        grafico1_setor = df_combinado['setor'].tolist() # eixo x
        grafico1_mtbf = df_combinado['MTBF'].tolist() # eixo y gráfico 1
        grafico2_mttr = df_combinado['MTTR'].tolist() # eixo y grafico 2

        sorted_tuples = sorted(zip(grafico1_setor, grafico1_mtbf, grafico2_mttr), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        grafico1_setor, grafico1_mtbf, grafico2_mttr = zip(*sorted_tuples)

        grafico1_setor = list(grafico1_setor)
        grafico1_mtbf = list(grafico1_mtbf)
        grafico2_mttr = list(grafico2_mttr)

        context_setor = {'grafico1_setor': grafico1_setor, 'grafico1_mtbf_setor': grafico1_mtbf,
                'grafico2_setor':grafico1_setor, 'grafico2_mttr_setor':grafico2_mttr}
        
    else:

        grafico1_setor = []
        grafico1_mtbf = []
        grafico2_mttr = []

        context_setor = {'grafico1_setor': grafico1_setor, 'grafico1_mtbf_setor': grafico1_mtbf,
            'grafico2_setor':grafico1_setor, 'grafico2_mttr_setor':grafico2_mttr} 

    return context_se

def calculo_indicadores_disponibilidade_maquinas(query_disponibilidade):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query_disponibilidade, conn)

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
    
    df_timeline['maquina'] = df_timeline['maquina']
    df_timeline['maquina'] = df_timeline['maquina'].str.split(' - ').str[0]
    
    df_agrupado_tempo = df_timeline.groupby(['maquina'])['diferenca'].sum().reset_index()
    df_agrupado_tempo['maquina'] = df_agrupado_tempo['maquina'].str.split(' - ').str[0]

    df_agrupado_qtd = df_timeline[['maquina']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['maquina'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['maquina'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='maquina')

    s = ("""
    SELECT * FROM tb_maquinas
    """)

    df_maquinas = pd.read_sql_query(s, conn).drop_duplicates()
    df_maquinas = df_maquinas[['codigo']]
    df_maquinas = df_maquinas.rename(columns={'codigo':'maquina'})

    df_combinado = df_combinado.merge(df_maquinas, on='maquina')
    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 9
    
    df_combinado['MTBF'] = (df_combinado['carga_trabalhada'] - df_combinado['diferenca']) / df_combinado['qtd_manutencao']
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    df_combinado['disponibilidade'] = (df_combinado['MTBF'] / (df_combinado['MTBF'] + df_combinado['MTTR'])) * 100

    if len(df_combinado)> 0:

        labels = df_combinado['maquina'].tolist() # eixo x
        dados_disponibilidade = df_combinado['disponibilidade'].tolist() # eixo y gráfico 1

        sorted_tuples = sorted(zip(labels, dados_disponibilidade), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        labels, dados_disponibilidade = zip(*sorted_tuples)

        labels = list(labels)
        dados_disponibilidade = list(dados_disponibilidade)

        context_disponibilidade = {'labels_disponibilidade_maquina': labels, 'dados_disponibilidade_maquina': dados_disponibilidade}        
    
    else:

        labels = []
        dados_disponibilidade = []

        context_disponibilidade = {'labels_disponibilidade_maquina': labels, 'dados_disponibilidade_maquina': dados_disponibilidade}        

    return context_disponibilidade

def calculo_indicadores_disponibilidade_setor(query_disponibilidade):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query_disponibilidade, conn)

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
    
    df_timeline['setor'] = df_timeline['setor']
    df_timeline['setor'] = df_timeline['setor'].str.split(' - ').str[0]
    
    df_agrupado_tempo = df_timeline.groupby(['setor'])['diferenca'].sum().reset_index()
    df_agrupado_tempo['setor'] = df_agrupado_tempo['setor'].str.split(' - ').str[0]

    df_agrupado_qtd = df_timeline[['setor']]
    
    # Contar a quantidade de manutenções por máquina
    contagem = df_agrupado_qtd['setor'].value_counts()
    df_agrupado_qtd['qtd_manutencao'] = df_agrupado_qtd['setor'].map(contagem)
    df_agrupado_qtd = df_agrupado_qtd.drop_duplicates()

    df_combinado = df_agrupado_qtd.merge(df_agrupado_tempo,on='setor')

    df_combinado['diferenca'] = df_combinado['diferenca'] / 60

    qtd_dias_uteis = dias_uteis()

    df_combinado['carga_trabalhada'] = qtd_dias_uteis * 7
    
    df_combinado['MTBF'] = (df_combinado['carga_trabalhada'] - df_combinado['diferenca']) / df_combinado['qtd_manutencao']
    df_combinado['MTTR'] = df_combinado['diferenca'] / df_combinado['qtd_manutencao']

    df_combinado['disponibilidade'] = ((df_combinado['MTBF'] / (df_combinado['MTBF'] + df_combinado['MTTR'])) * 100).round(2)

    if len(df_combinado)> 0:

        labels = df_combinado['setor'].tolist() # eixo x
        dados_disponibilidade = df_combinado['disponibilidade'].tolist() # eixo y gráfico 1

        sorted_tuples = sorted(zip(labels, dados_disponibilidade), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        labels, dados_disponibilidade = zip(*sorted_tuples)

        labels = list(labels)
        dados_disponibilidade = list(dados_disponibilidade)
        
        context_disponibilidade_setor = {'labels_disponibilidade_setor': labels, 'dados_disponibilidade_setor': dados_disponibilidade}

    else:

        labels = []
        dados_disponibilidade = []

        context_disponibilidade_setor = {'labels_disponibilidade_setor': labels, 'dados_disponibilidade_setor': dados_disponibilidade}

    return context_disponibilidade_setor

def tempo_fechamento_os(query_mttr):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    # Obtém os dados da tabela
    
    mes_hoje = datetime.today().month
    
    df_timeline = pd.read_sql_query(query_mttr, conn)

    df_timeline = df_timeline.sort_values(by='n_ordem')

    df_timeline = df_timeline.dropna()

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
    
    df_agrupado_tempo = df_timeline.groupby(['setor'])['diferenca'].sum().reset_index()

    df_agrupado_tempo['diferenca'] = df_agrupado_tempo['diferenca'] / 60    

    if len(df_agrupado_tempo)> 0:

        label = df_agrupado_tempo['setor'].tolist() # eixo x
        diferenca = df_agrupado_tempo['diferenca'].tolist() # eixo y grafico 2

        sorted_tuples = sorted(zip(label, diferenca), key=lambda x: x[0])

        # Desempacotar as tuplas classificadas em duas listas separadas
        label, diferenca = zip(*sorted_tuples)

        label = list(label)
        diferenca = list(diferenca)

        context_horas_por_setor = {'labels_horas_por_setor':label, 'dados_horas_por_setor':diferenca}
        
    else:

        label = []
        diferenca = []

        context_horas_por_setor = {'labels_horas_por_setor':label, 'dados_horas_por_setor':diferenca} 

    return context_horas_por_setor

def cards():
    
    mes_hoje = datetime.today().month

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = "SELECT * FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE"

    df_cards = pd.read_sql_query(query, conn)

    df_cards['ultima_atualizacao'] = pd.to_datetime(df_cards['ultima_atualizacao'])
    df_cards['mes'] = df_cards['ultima_atualizacao'].dt.month
    df_cards = df_cards[df_cards['mes'] == mes_hoje]
    
    df_cards = df_cards.drop_duplicates(subset='id_ordem', keep='last')
    df_cards['status'] = df_cards['status'].apply(lambda x: x.split("  ")[0])

    espera = df_cards[df_cards['status'] == 'Em espera'].shape[0]
    material = df_cards[df_cards['status'] == 'Aguardando material'].shape[0]
    finalizado = df_cards[df_cards['status'] == 'Finalizada'].shape[0]
    execucao = df_cards[df_cards['status'] == 'Em execução'].shape[0]

    lista_qt = [espera,material,finalizado,execucao]

    return lista_qt

# def calculo_media_os(query):

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
    df_area = df_area[['area_manutencao']].reset_index(drop=True)

    contagem = df_area['area_manutencao'].value_counts()
    #df_area['qtde_area'] = df_area['area_manutencao'].map(contagem)
    
    area = df_area['area_manutencao'].unique().tolist()
    quantidade_area = contagem.tolist()

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
    df = pd.read_sql_query(query, conn)
    
    ultima_atualizacao = df['ultima_atualizacao'][0] - timedelta(hours=3)

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

    # workbook = Workbook("modelo_os_new.xlsx")
    # workbook.save("Ordem de Serviço.pdf")

    # convertapi.api_secret = 'vkVdyOJxS8xz4uWq'
    # convertapi.convert('pdf', {
    #     'File': 'modelo_os_new.xlsx'
    # }, from_format = 'xlsx').save_files('modelo_os_new.pdf')
    
    # arquivo_final = 'modelo_os_new.pdf'

    # Retorna o arquivo para download
    return send_file("modelo_os_new.xlsx", as_attachment=True)

@routes_bp.route('/')
@login_required
def inicio(): # Redirecionar para a página de login
    
    return render_template("login/login.html")

@routes_bp.route('/index')
@login_required
def Index(): # Página inicial (Página com a lista de ordens de serviço)
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
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
    df = df.sort_values(by='id_ordem').reset_index(drop=True)
    
    for i in range(len(df)):
        try:
            if df['id_ordem'][i] == df['id_ordem'][i-1]:
                df['maquina_parada'][i] = df['maquina_parada'][i-1]
        except:
            pass
        
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
    df['ultima_atualizacao'] = df['ultima_atualizacao'] - timedelta(hours=3)
    df['ultima_atualizacao'] = df['ultima_atualizacao'].dt.strftime("%Y-%m-%d %H:%M:%S")

    list_users = df.values.tolist()

    return render_template('user/index.html', list_users=list_users)

@routes_bp.route('/add_student', methods=['POST', 'GET']) 
def add_student(): # Criar ordem de serviço
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    if request.method == 'POST':

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        setor = request.form.get('setor')
        maquina = request.form.get('maquina')
        problema = request.form.get('problema')
        solicitante = request.form.get('solicitante')
        dataAbertura = datetime.now()
        equipamento_em_falha = request.form.get('falha')
        setor_maquina_solda = request.form.get('solda_maquina')
        qual_ferramenta = request.form.get('ferramenta')
        cod_equipamento = request.form.get('codigo_equip')

        print(equipamento_em_falha,setor_maquina_solda,qual_ferramenta)

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

        if maquina != None:
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

        cur.execute("INSERT INTO tb_ordens (id, setor, maquina, risco,status, problemaaparente, id_ordem, n_ordem ,dataabertura, maquina_parada,solicitante,equipamento_em_falha,setor_maquina_solda,qual_ferramenta, cod_equipamento) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                     (maior_valor, setor, maquina, risco, status, problema, ultima_os, n_ordem, dataAbertura, maquina_parada,solicitante,equipamento_em_falha,setor_maquina_solda,qual_ferramenta, cod_equipamento))

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

    query = """SELECT DISTINCT CONCAT(codigo, ' - ', descricao) AS codigo_descricao
            FROM tb_ordens AS t1
            JOIN tb_maquinas AS t2 ON t1.maquina = t2.codigo
            WHERE t1.id_ordem = {}""".format((int(id_ordem)))
    cur.execute(query)

    maquinas = cur.fetchall()
    
    if len(maquinas) == 0:
        maquinas.append('Outros')
    else:
        maquinas = maquinas[0]

    return render_template('user/edit.html', ordem=data1[0], tb_funcionarios=tb_funcionarios, opcoes=opcoes, tipo_manutencao=tipo_manutencao, area_manutencao=area_manutencao, maquinas=maquinas)

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

        try:
            botao1 = request.form['maquina-parada-1']

        except:
            botao1 = 'false'
        try:
            botao2 = request.form['maquina-parada-2']

        except:
            botao2 = 'false'
        try:
            botao3 = request.form['maquina-parada-3']
        except:
            botao3 = 'false'

        if status == 'Finalizada':
            botao3 = 'true'



        print(botao1)
        print(botao2)
        print(botao3)

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

        # print(ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador])

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("""
            INSERT INTO tb_ordens ( id, setor,maquina,risco,status,problemaaparente,
                                    datainicio,horainicio,datafim,horafim,id_ordem,n_ordem,
                                    descmanutencao, operador, natureza, tipo_manutencao, area_manutencao) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio,
               datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador], natureza, tipo_manutencao, area_manutencao))

        cur.execute("""
            INSERT INTO tb_paradas (id_ordem,n_ordem, parada1, parada2, parada3) 
                    VALUES (%s,%s,%s,%s,%s)
        """, (id_ordem,n_ordem,botao1,botao2,botao3))
        
        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()

        return redirect(url_for('routes.get_employee', id_ordem=id_ordem))

@routes_bp.route('/editar_ordem/<id_ordem>/<n_ordem>', methods = ['POST', 'GET'])
@login_required
def editar_ordem(id_ordem,n_ordem):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ('SELECT * FROM tb_ordens WHERE id_ordem = {} AND n_ordem = {}'.format(int(id_ordem), int(n_ordem)))
    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)

    data1.reset_index(drop=True, inplace=True)
    data1.replace(np.nan, '', inplace=True)
    
    desc_manutencao = data1['descmanutencao'].values.tolist()[0]

    executante = data1['operador'].values.tolist()[0].replace("{","").replace("[","").replace("\\","").replace('"', '').replace("]}","")
    executante = [exec.strip() for exec in executante.split(',')]

    tipo_manutencao = data1['tipo_manutencao'].values.tolist()[0]
    area_manutencao = data1['area_manutencao'].values.tolist()[0]
    
    data_inicio = datetime.strptime(str(data1['datainicio'].values[0]), '%Y-%m-%d').strftime('%d/%m/%Y')
    hora_inicio = datetime.strptime(str(data1['horainicio'].values[0]), '%H:%M:%S').strftime('%H:%M')
    data_fim = datetime.strptime(str(data1['datafim'].values[0]), '%Y-%m-%d').strftime('%d/%m/%Y')
    hora_fim = datetime.strptime(str(data1['horafim'].values[0]), '%H:%M:%S').strftime('%H:%M')

    data_atual = f'{data_inicio} {hora_inicio} - {data_fim} {hora_fim}'

    print(data_atual)

    print(data_atual)

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
    
    return render_template('user/editar_ordem.html', ordem=data1[0], tb_funcionarios=tb_funcionarios, opcoes=opcoes, tipo_manutencao=tipo_manutencao,
                            area_manutencao=area_manutencao, executante=executante, desc_manutencao=desc_manutencao, data_atual=data_atual)

@routes_bp.route('/editar_ordem_inicial/<id_ordem>/<n_ordem>', methods = ['POST', 'GET'])
@login_required
def editar_ordem_inicial(id_ordem,n_ordem):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ('SELECT * FROM tb_ordens WHERE id_ordem = {} AND n_ordem = {}'.format(int(id_ordem), int(n_ordem)))
    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)

    data1.reset_index(drop=True, inplace=True)
    data1.replace(np.nan, '', inplace=True)
    maquina = data1['maquina'][0]

    try:
        maquina = maquina.split(" - ")[0]
    except:
        pass

    data1 = data1.values.tolist()

    s = ("""
        SELECT CONCAT(codigo , ' - ' , descricao) as codigo_descricao 
        FROM tb_maquinas
        WHERE codigo = '{}'
        """.format(maquina))
    cur.execute(s)
    
    maquina = cur.fetchall()
    
    try:
        maquina = maquina[0]
    except:
        maquina = maquina

    if len(maquina) == 0:
        maquina.append('Outros')

    return render_template('user/editar_ordem_inicial.html', ordem=data1[0], maquina=maquina, n_ordem=n_ordem)

@routes_bp.route('/update_ordem/<id_ordem>/<n_ordem>', methods=['POST'])
@login_required
def update_ordem(id_ordem, n_ordem): # Inserir as edições no banco de dados

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
        #setor = request.form['setor']
        #maquina = request.form['maquina']        
        #risco = request.form['risco']
        status = request.form['statusLista']
        #problema = request.form['problema']
        id_ordem = id_ordem
        n_ordem = n_ordem
        descmanutencao = request.form['descmanutencao']
        operador = request.form.getlist('operador')
        operador = json.dumps(operador)
        tipo_manutencao = request.form['tipo_manutencao1']
        datetimes = request.form['datetimes']
        area_manutencao = request.form['area_manutencao1']
        natureza = natureza

        # Divida a string em duas partes: data/hora inicial e data/hora final
        data_hora_inicial_str, data_hora_final_str = datetimes.split(" - ")

        data_inicial = datetime.strptime(data_hora_inicial_str, "%d/%m/%Y %H:%M")
        data_final = datetime.strptime(data_hora_final_str, "%d/%m/%Y %H:%M")
        
        # Formate as datas e horas no formato desejado
        datainicio = data_inicial.strftime("%Y-%m-%d")
        horainicio = data_inicial.strftime("%H:%M:%S")
        datafim = data_final.strftime("%Y-%m-%d")
        horafim = data_final.strftime("%H:%M:%S")

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
        UPDATE tb_ordens
        SET status=%s,datainicio=%s,horainicio=%s,datafim=%s,horafim=%s,id_ordem=%s,
            n_ordem=%s, descmanutencao=%s, operador=%s, natureza=%s, tipo_manutencao=%s, area_manutencao=%s

        WHERE n_ordem = %s and id_ordem = %s
        """, (status, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador], natureza, tipo_manutencao, area_manutencao, n_ordem, id_ordem))

        # cur.execute("""
        #     INSERT INTO tb_ordens (id, setor,maquina,risco,status,problemaaparente,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem, descmanutencao, operador, natureza, tipo_manutencao, area_manutencao) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        # """, (ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador], natureza, tipo_manutencao, area_manutencao))
        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()
        cur.close()

        return redirect(url_for('routes.timeline_os', id_ordem=id_ordem))

@routes_bp.route('/guardar_ordem_editada/<id_ordem>/<n_ordem>', methods=['POST'])
@login_required
def guardar_ordem_editada(id_ordem, n_ordem): # Inserir as edições no banco de dados

    if request.method == 'POST':
        
        id_ordem = id_ordem
        setor = request.form['setor']
        maquina = request.form.get('maquina')        
        equipamento_em_falha = request.form.get('falha')
        setor_maquina_solda = request.form.get('solda_maquina')
        qual_ferramenta = request.form.get('ferramenta')
        codigo_equipamento = request.form.get("codigo_equip")
        problema = request.form['problema']
        risco = request.form.get("risco")
        maquina_parada = request.form.get('maquina-parada')
        
        try:
            maquina = maquina.split(" - ")[0]
        except:
            pass

        if maquina_parada:
            maquina_parada = 'True'
        else:
            maquina_parada = 'False'

        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            UPDATE tb_ordens
            SET setor=%s,maquina=%s,risco=%s,maquina_parada=%s,equipamento_em_falha=%s,setor_maquina_solda=%s,
            qual_ferramenta=%s,cod_equipamento=%s,problemaaparente=%s
            WHERE id_ordem = %s
            """, (setor, maquina, risco, maquina_parada, equipamento_em_falha, setor_maquina_solda, qual_ferramenta, codigo_equipamento, problema, id_ordem))

        conn.commit()
        cur.close()
        
        return redirect(url_for('routes.Index'))

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

@login_required
@routes_bp.route('/maquinas/<setor>')
def filtro_maquinas(setor):
   
    #setor = setor.upper()
    if setor == 'Serralheria':
        setor = 'Solda'

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
    
    if request.method == 'POST':
      
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT DISTINCT setor FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE;")
        setores = cur.fetchall()

        cur.execute("SELECT * FROM tb_maquinas_preventivas")
        name_cols = ['codigo','tombamento','setor','descricao','criticidade','periodicidade']
        df_maquinas = pd.DataFrame(cur.fetchall()).iloc[:,:6]
        df_maquinas = df_maquinas.rename(columns=dict(zip(df_maquinas.columns, name_cols)))
        maquinas = df_maquinas.values.tolist()

        setor_selecionado = request.form.get('filtro_setor')
        maquina_selecionado = request.form.get('filtro_maquinas')
        area_manutencao = request.form.get('area_manutencao')

        """ Criando cards """

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
        query = "SELECT * FROM tb_ordens WHERE 1=1"

        # Adiciona as condições de filtro se os campos não estiverem vazios
        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        query += 'AND ordem_excluida IS NULL OR ordem_excluida = FALSE;'
        
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

        """ Finalizando cards """

        """Criando gráficos de barras MTBF por maquina"""

        query = (
        """
            SELECT maquina, n_ordem, id_ordem, dataabertura, setor
            FROM tb_ordens
            WHERE 1=1
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        query += " AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'" 

        context_mtbf_maquina = mtbf_maquina(query)
        context_mtbf_setor = mtbf_setor(query)

        """ Finalizando MTTR por máquina e setor"""

        """Criando gráficos de barras MTTR por maquina"""

        query = (
        """
            SELECT datafim, maquina, n_ordem, setor,
                TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
                TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
            FROM tb_ordens 
            WHERE 1=1
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        query += " AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'" 

        context_mttr_maquina = mttr_maquina(query)
        context_mttr_setor = mttr_setor(query)
        context_horas_por_setor = tempo_fechamento_os(query)

        """ Finalizando MTTR por máquina e setor"""

        query = ("""
            SELECT datafim, maquina, n_ordem, setor,
                TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
                TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
            FROM tb_ordens
            WHERE 1=1
        """)

        if setor_selecionado:
            query += f" AND setor = '{setor_selecionado}'"
        if area_manutencao:
            query += f" AND area_manutencao = '{area_manutencao}'"

        query += " AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'" 

        context_disponiblidade_maquina = calculo_indicadores_disponibilidade_maquinas(query)
        context_disponiblidade_setor = calculo_indicadores_disponibilidade_setor(query)


        return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, itens_filtrados=itens_filtrados,
                               setor_selecionado=setor_selecionado, maquina_selecionado=maquina_selecionado, **context_mtbf_maquina,
                                **context_mtbf_setor, **context_mttr_maquina, **context_mttr_setor, **context_disponiblidade_maquina,
                                **context_disponiblidade_setor, **context_horas_por_setor, area_manutencao=area_manutencao)
    
    lista_qt = cards()

    query_mtbf = (
    """
        SELECT maquina, n_ordem, id_ordem, dataabertura, setor
        FROM tb_ordens
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'
    """)

    context_mtbf_maquina = mtbf_maquina(query_mtbf)
    context_mtbf_setor = mtbf_setor(query_mtbf)

    query_mttr = (
    """
        SELECT datafim, maquina, n_ordem, setor,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens 
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'
    """)

    context_mttr_maquina = mttr_maquina(query_mttr)
    context_mttr_setor = mttr_setor(query_mttr)
    context_horas_por_setor = tempo_fechamento_os(query_mttr)
    
    query_disponibilidade = ("""
        SELECT datafim, maquina, n_ordem, setor,
            TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS') AS inicio,
            TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS') AS fim
        FROM tb_ordens
        WHERE 1=1 AND ordem_excluida IS NULL OR ordem_excluida = FALSE AND natureza = 'OS'
    """)

    context_disponiblidade_maquina = calculo_indicadores_disponibilidade_maquinas(query_disponibilidade)
    context_disponiblidade_setor = calculo_indicadores_disponibilidade_setor(query_disponibilidade)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cur.execute("SELECT DISTINCT setor FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE;")
    setores = cur.fetchall()

    cur.execute("SELECT * FROM tb_maquinas_preventivas")
    name_cols = ['codigo','tombamento','setor','descricao','criticidade','periodicidade']
    df_maquinas = pd.DataFrame(cur.fetchall()).iloc[:,:6]
    df_maquinas = df_maquinas.rename(columns=dict(zip(df_maquinas.columns, name_cols)))
    maquinas = df_maquinas.values.tolist()

    # Se o método for GET, exibe todos os itens
    cur.execute("SELECT * FROM tb_ordens WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE")
    itens = cur.fetchall()

    cur.close()
    conn.close()

    return render_template('user/grafico.html', lista_qt=lista_qt, setores=setores, maquinas=maquinas, itens=itens,
                            **context_mtbf_maquina, **context_mtbf_setor, **context_mttr_maquina, **context_mttr_setor,
                            **context_disponiblidade_maquina, **context_disponiblidade_setor, **context_horas_por_setor,
                            setor_selecionado='', maquina_selecionado='', area_manutencao='')

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
            togglePreventiva = request.form.get('cadastrar-preventiva')
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

            if len(maquina_cadastrada[maquina_cadastrada['codigo'] == codigo]) > 0:
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

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(""" SELECT codigo,setor, descricao 
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
@login_required
def visualizar_pdf(id_ordem):

    return formulario_os(id_ordem)

@routes_bp.route('/maquina-52-semanas/<codigo>')
@login_required
def transformar_maquina(codigo):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT * FROM tb_maquinas WHERE codigo = '{}'""".format(codigo)

    cur.execute(query)
    data = cur.fetchall()

    codigo = codigo
    setor = data[0][1]
    descricao = data[0][3]
    tombamento = data[0][4]

    if not tombamento:
        tombamento = ''

    return render_template('user/cadastrar52_existente.html', codigo=codigo,
                           setor=setor,descricao=descricao,tombamento=tombamento)

@routes_bp.route('/editar-maquina/<codigo>')
@login_required
def editar_maquina(codigo):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT * FROM tb_maquinas WHERE codigo = '{}'""".format(codigo)

    cur.execute(query)
    data = cur.fetchall()

    codigo = codigo
    setor = data[0][1]
    descricao = data[0][3]
    tombamento = data[0][4]

    if not tombamento:
        tombamento = ''

    return render_template('user/editar_maquina.html', codigo=codigo,
                           setor=setor,descricao=descricao,tombamento=tombamento)

@routes_bp.route('/editar-maquina-bd/<codigo>', methods=['POST'])
@login_required
def salvar_edicao_maquina(codigo):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    codigo_inicial = codigo
    codigo_novo = request.form['codigo']
    tombamento = request.form['tombamento']
    descricao = request.form['descricao']
    setor = request.form['setor']
    
    print(setor, codigo_novo, descricao, tombamento)
   
    if codigo_novo != codigo_inicial:
        query = """SELECT * FROM tb_maquinas WHERE codigo = '{}'""".format(codigo_novo)
        data = pd.read_sql_query(query, conn)

        if len(data) > 0:
            flash("Código já cadastrado.",category='error')
            codigo = codigo_novo
            conn.close()
        else:
            """Query para editar a linha do codigo escolhido"""
            cur.execute("""
                UPDATE tb_maquinas
                SET setor=%s,codigo=%s,descricao=%s,tombamento=%s
                WHERE codigo = %s
                """, (setor, codigo_novo, descricao, tombamento, codigo_inicial))
            
            cur.execute("""
                UPDATE tb_maquinas_preventivas
                SET codigo=%s,tombamento=%s,setor=%s,codigo=%s
                WHERE codigo = %s
                """, (codigo_novo, tombamento, setor, descricao, codigo_inicial))
            
            conn.commit()
            conn.close()
            codigo = codigo_novo
            """Enviar mensagem de sucesso"""
            flash("Código editado com sucesso", category='success')
    
    else:
        """Query para editar a linha do codigo escolhido"""
        cur.execute("""
            UPDATE tb_maquinas
            SET setor=%s,codigo=%s,descricao=%s,tombamento=%s
            WHERE codigo = %s
            """, (setor, codigo_novo, descricao, tombamento, codigo_inicial))

        cur.execute("""
            UPDATE tb_maquinas_preventivas
            SET codigo=%s,tombamento=%s,setor=%s,descricao=%s
            WHERE codigo = %s
            """, (codigo_novo, tombamento, setor, descricao, codigo_inicial))

        conn.commit()
        conn.close()
        
        """Enviar mensagem de sucesso"""
        flash("Código editado com sucesso", category='success')

    return render_template('user/editar_maquina.html', codigo=codigo, tombamento=tombamento, descricao=descricao, setor=setor)

@routes_bp.route('/excluir-maquina', methods=['POST'])
@login_required
def excluir_maquina():
    if request.method == 'POST':
        # Obter o código da máquina enviado pelo frontend
        codigo_maquina = request.form.get('codigo_maquina')
        
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """DELETE FROM tb_maquinas
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])
        
        query = """DELETE FROM tb_maquinas_preventivas
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])    
        
        conn.commit()
        conn.close()
        
        flash("Máquina excluída com sucesso", category='sucess')

        return 'Dados recebidos com sucesso!'
    
@routes_bp.route('/excluir-preventiva', methods=['POST'])
@login_required
def excluir_preventiva():
    
    if request.method == 'POST':
        # Obter o código da máquina enviado pelo frontend
        codigo_maquina = request.form.get('codigo_maquina')
        
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        query = """DELETE FROM tb_maquinas_preventivas
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])    
        
        conn.commit()
        conn.close()
        
        flash("Máquina excluída com sucesso", category='sucess')

        return 'Dados recebidos com sucesso!'