# app.py
from flask import Flask, current_app, send_file, jsonify, render_template, request, redirect, url_for, flash, Blueprint, Response, send_from_directory, send_file, current_app, make_response
import psycopg2  # pip install psycopg2
import psycopg2.extras
import datetime
import pandas as pd
import numpy as np
import json
from funcoes import gerador_de_semanas_informar_manutencao, login_required, gerador_de_semanas_informar_manutencao_diario,gerar_planejamento_maquinas_preventivas,calcular_proxima_data,calcular_planejamento_anual
import warnings
from flask import session
import base64
from datetime import datetime, timedelta, time, date, timezone
from pandas.tseries.offsets import BMonthEnd,MonthEnd,BDay
from psycopg2 import Error
import json
from PIL import Image
import io
from openpyxl import load_workbook
# import convertapi
from werkzeug.utils import secure_filename
import os
import zipfile
from io import BytesIO
import re
import requests
import copy
import calendar

routes_bp = Blueprint('routes', __name__)

# routes_bp.config['UPLOAD_FOLDER'] = r'C:\Users\pcp2\projetoManutencao\appManutFlask-3\UPLOAD_FOLDER'

# Configurar a pasta para salvar os vídeos
routes_bp.config = {}
routes_bp.config['UPLOAD_FOLDER'] = 'UPLOAD_FOLDER'

warnings.filterwarnings("ignore")

# DB_HOST = "localhost"
DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)

def dados_para_editar(id_ordem,n_ordem):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = "select o.*,func.nome,func.matricula,coalesce(status,'Em espera') as status_new from tb_ordens as o left join tb_funcionario as func on ',' || o.operador || ',' like '%%,' || func.matricula || ',%%' where id_ordem = %s and n_ordem = %s"
    sql_data_abertura =  """select dataabertura - INTERVAL '3 hours' as dataabertura,solicitante,
                            equipamento_em_falha,cod_equipamento,setor_maquina_solda,qual_ferramenta,status
                            from tb_ordens where id_ordem = %s and n_ordem = 0
                        """
    sql_tombamento = """select tombamento from tb_ordens
                        left join tb_maquinas as t2 on t2.codigo = maquina
                        where id_ordem = %s
                        limit 1
                    """
    # sql_maquina_preventiva = """select codigo from tb_maquinas_preventivas where codigo = %s"""
    
    # Informações gerais
    cur.execute(sql,(id_ordem,n_ordem))
    data = cur.fetchall()
    
    # dataabertura
    cur.execute(sql_data_abertura,(id_ordem,))
    dados_dataabertura = cur.fetchall()

    # Tombamento
    cur.execute(sql_tombamento,(id_ordem,))
    tombamento = cur.fetchall()

    setor_values = [row['setor'] for row in data][0]
    solicitante = dados_dataabertura[0]['solicitante']
    data_abertura = dados_dataabertura[0]['dataabertura']
    n_execucao = [row['n_ordem'] for row in data][0]
    maquina = [row['maquina'] for row in data][0]
    tombamento = [row['tombamento'] for row in tombamento][0]
    equipamento_em_falha = dados_dataabertura[0]['equipamento_em_falha']
    codigo_equipamento = dados_dataabertura[0]['cod_equipamento']
    setor_maquina_solda = dados_dataabertura[0]['setor_maquina_solda']
    qual_ferramenta = dados_dataabertura[0]['qual_ferramenta']
    risco = [row['risco'] for row in data][0]
    desc_usuario = [row['problemaaparente'] for row in data][0]
    status = [row['status_new'] for row in data][0]
    tipo_manutencao = [row['tipo_manutencao'] for row in data][0]
    area_manutencao = [row['area_manutencao'] for row in data][0]
    pvlye = [row['pvlye'] for row in data][0]
    paplus = [row['pa_plus'] for row in data][0]
    tratamento = [row['tratamento'] for row in data][0]
    phagua = [row['ph_agua'] for row in data][0]

    try:
        operador = [row['matricula'] + " - " + row['nome'] for row in data]
    except TypeError:
        operador = ''

    descmanutencao = [row['descmanutencao'] for row in data][0]
    # data_inicio_fim = [row['datainicio'] for row in data][0]
    datainicio = [row['datainicio'] for row in data][0]
    horainicio = [row['horainicio'] for row in data][0]
    datafim = [row['datafim'] for row in data][0]
    horafim = [row['horafim'] for row in data][0]

    # Formatando as datas e horas como strings
    try:
        data_inicio_str = datainicio.isoformat()
        hora_inicio_str = horainicio.strftime('%H:%M')
        data_fim_str = datafim.isoformat()
        hora_fim_str = horafim.strftime('%H:%M')
    except:
        data_inicio_str = datetime.now().date().isoformat()
        horainicio = "00:00"
        hora_inicio_str = horainicio
        data_fim_str = datetime.now().date().isoformat()
        horafim = "00:00"
        hora_fim_str = horafim

    dados = {
        'setor_values':setor_values,
        'solicitante':solicitante,
        'data_abertura':data_abertura,
        'n_execucao':n_execucao,
        'maquina':maquina,
        'tombamento':tombamento,
        'equipamento_em_falha':equipamento_em_falha,
        'codigo_equipamento':codigo_equipamento,
        'setor_maquina_solda':setor_maquina_solda,
        'qual_ferramenta':qual_ferramenta,
        'risco':risco,
        'desc_usuario':desc_usuario,
        'status':status,
        'tipo_manutencao':tipo_manutencao,
        'area_manutencao':area_manutencao,
        'operador':operador,
        'descmanutencao':descmanutencao,
        'pvlye':pvlye,
        'paplus':paplus,
        'tratamento':tratamento,
        'phagua':phagua,
        'data_inicio': data_inicio_str,
        'hora_inicio': hora_inicio_str,
        'data_fim': data_fim_str,
        'hora_fim': hora_fim_str
    }

    return dados

def buscar_dados_os(id_ordem):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = "select *, coalesce(status,'Em espera') as status_new, coalesce(dataabertura,ultima_atualizacao) as dataabertura_atualizada from tb_ordens where id_ordem = %s order by n_ordem desc limit 1"
    sql_data_abertura =  """select coalesce(dataabertura,ultima_atualizacao) as dataabertura,solicitante,
                            equipamento_em_falha,cod_equipamento,setor_maquina_solda,status
                            from tb_ordens where id_ordem = %s and n_ordem = 0
                        """
    sql_tombamento = """select tombamento from tb_ordens
                        left join tb_maquinas as t2 on t2.codigo = maquina
                        where id_ordem = %s
                        limit 1
                    """
    sql_maquina_preventiva = """select codigo from tb_maquinas_preventivas where codigo = %s"""
    sql_paradas = """select parada1,parada2,parada3 from public.tb_paradas tp where id_ordem = %s order by n_ordem desc limit 1"""

    # Informações gerais
    cur.execute(sql,(id_ordem,))
    data = cur.fetchall()
    
    # dataabertura
    cur.execute(sql_data_abertura,(id_ordem,))
    dados_dataabertura = cur.fetchall()

    # Tombamento
    cur.execute(sql_tombamento,(id_ordem,))
    tombamento = cur.fetchall()

    # Paradas
    cur.execute(sql_paradas,(id_ordem,))
    paradas = cur.fetchone()

    setor_values = [row['setor'] for row in data][0]
    solicitante = dados_dataabertura[0]['solicitante']
    data_abertura = dados_dataabertura[0]['dataabertura']
    n_execucao = [row['n_ordem'] for row in data][0]+1
    maquina = [row['maquina'] for row in data][0]
    tombamento = [row['tombamento'] for row in tombamento][0]
    equipamento_em_falha = dados_dataabertura[0]['equipamento_em_falha']
    codigo_equipamento = dados_dataabertura[0]['cod_equipamento']
    setor_maquina_solda = dados_dataabertura[0]['setor_maquina_solda']
    risco = [row['risco'] for row in data][0]
    desc_usuario = [row['problemaaparente'] for row in data][0]
    status = [row['status_new'] for row in data][0]
    tipo_manutencao = [row['tipo_manutencao'] for row in data][0]
    area_manutencao = [row['area_manutencao'] for row in data][0]

    try:
        maq_parada_real = paradas[0]
        exec_maq_parada = paradas[1]
        apos_exec_maq_parada = paradas[2]
    except:
        maq_parada_real = 'false'
        exec_maq_parada = 'false'
        apos_exec_maq_parada = 'false'

    # Maquina preventiva
    cur.execute(sql_maquina_preventiva,(maquina,))
    preventiva = cur.fetchall()
    
    if len(preventiva) > 0:
        preventiva = True
    else:
        preventiva = False

    lista_opcoes = ['Em execução', 'Finalizada', 'Aguardando material']

    opcoes = []
    opcoes.append(status)

    for opcao in lista_opcoes:
        opcoes.append(opcao)

    opcoes = list(set(opcoes))
    opcoes.remove(status)  # Remove o elemento 'c' da lista
    opcoes.insert(0, status)

    

    dados = {
        'setor_values':setor_values,
        'solicitante':solicitante,
        'data_abertura':data_abertura,
        'n_execucao':n_execucao,
        'maquina':maquina,
        'tombamento':tombamento,
        'equipamento_em_falha':equipamento_em_falha,
        'codigo_equipamento':codigo_equipamento,
        'setor_maquina_solda':setor_maquina_solda,
        'risco':risco,
        'desc_usuario':desc_usuario,
        'status':status,
        'tipo_manutencao':tipo_manutencao,
        'area_manutencao':area_manutencao,
        'preventiva':preventiva,
        'opcoes':opcoes,
        'maq_parada_real':maq_parada_real,
        'exec_maq_parada':exec_maq_parada,
        'apos_exec_maq_parada':apos_exec_maq_parada,
    }

    return dados

def buscar_funcionarios():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT * FROM tb_funcionario"""
    tb_funcionarios = pd.read_sql_query(query, conn)
    tb_funcionarios['matricula_nome'] = tb_funcionarios['matricula'] + \
        " - " + tb_funcionarios['nome']
    funcionarios = tb_funcionarios[['matricula_nome']].values.tolist()

    return funcionarios


def calcular_minutos_uteis(row, df):

    """
    Função para calcular os minutos úteis entre duas datas
    """

    hora_inicio_trabalho = 7
    hora_fim_trabalho = 17

    # Extraia as datas de início e fim da linha
    if row['parada3'] == 'true':
        data_inicio = pd.to_datetime(row['inicio']).replace(tzinfo=None)  # Remova as informações de fuso horário
        data_fim = row['fim'].replace(tzinfo=None)  # Remova as informações de fuso horário
    else:
        return 0
    
    def dentro_do_horario_trabalho(data):
        return hora_inicio_trabalho <= data.hour < hora_fim_trabalho

    # Inicialize variáveis
    minutos_uteis = 0
    data_atual = data_inicio

    # Calcule o número de minutos úteis
    while data_atual <= data_fim:
        if data_atual.weekday() < 5 and dentro_do_horario_trabalho(data_atual):
            minutos_uteis += 1
        data_atual += timedelta(minutes=1)

    return minutos_uteis


def obter_nome_mes(numeros_meses):

    """
    Função para obter nome do mês com base no número dele.
    """

    numeros_meses = list(map(int, numeros_meses))

    nomes_meses = {
        1: 'Janeiro',
        2: 'Fevereiro',
        3: 'Março',
        4: 'Abril',
        5: 'Maio',
        6: 'Junho',
        7: 'Julho',
        8: 'Agosto',
        9: 'Setembro',
        10: 'Outubro',
        11: 'Novembro',
        12: 'Dezembro'
    }

    nomes = [nomes_meses.get(numero_mes, '') for numero_mes in numeros_meses]

    return nomes


def ultimo_dia_mes(mes):

    """
    Função para buscar o último dia do mês e hora.
    """

    # Especifique o mês desejado
    mes_desejado = mes[-1]  # Outubro

    print(mes_desejado)
    # Obtenha o último dia do mês desejado
    if mes_desejado == 12:
        ultimo_dia_do_mes = datetime(datetime.now().year, mes_desejado, 1) - timedelta(days=1)
    elif mes_desejado != 1:
        ultimo_dia_do_mes = datetime(2023, mes_desejado + 1, 1) - timedelta(days=1)
    else:
        ultimo_dia_do_mes = datetime(datetime.now().year, mes_desejado + 1, 1) - timedelta(days=1)

    print(datetime.now().year)

    # Defina a hora desejada
    hora_desejada = time(23, 59, 59)

    # Combine a data e a hora
    ultimo_dia_do_mes_com_hora = datetime.combine(ultimo_dia_do_mes, hora_desejada)

    return ultimo_dia_do_mes_com_hora


def dias_uteis(meses):

    """
    Função que calcula os dias úteis em um determinado período de meses.
    """

    qtd_dias_uteis_total = 0

    for mes in meses:
        # Verificar se o mês é válido (entre 1 e 12)
        if not mes:
            data_atual = pd.Timestamp.now()
            mes = data_atual.month  # Mês atual
            qtd_dias_uteis = 0

            for m in range(7, mes + 1):
                primeiro_dia_mes = pd.Timestamp(data_atual.year, m, 1)

                if m == mes:
                    # Se for o mês atual, use o dia atual como o último dia útil
                    ultimo_dia_util_mes = data_atual
                else:
                    # Se não for o mês atual, use o último dia útil do mês
                    ultimo_dia_util_mes = primeiro_dia_mes + pd.offsets.BMonthEnd()

                datas_uteis = pd.bdate_range(
                    primeiro_dia_mes, ultimo_dia_util_mes)
                qtd_dias_uteis += len(datas_uteis)

        elif mes == int(pd.Timestamp.now().month):
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

        else:
            data_atual = pd.Timestamp.now()
            primeiro_dia_mes = pd.Timestamp(data_atual.year, mes, 1)

            # Obter o último dia útil do mês especificado
            ultimo_dia_util_mes = primeiro_dia_mes + BMonthEnd()

            # Obter a sequência de datas úteis no mês especificado
            datas_uteis = pd.bdate_range(primeiro_dia_mes, ultimo_dia_util_mes)

            # Contar o número de dias úteis
            qtd_dias_uteis = len(datas_uteis)

        qtd_dias_uteis_total += qtd_dias_uteis

    return qtd_dias_uteis_total

def dias_uteis_inicial_final(dia_inicial,dia_final):

    # Convertendo as strings para objetos de data
    data_inicial = pd.to_datetime(dia_inicial)
    data_final = pd.to_datetime(dia_final)

    # Criando um intervalo de datas entre as datas inicial e final
    intervalo = pd.date_range(start=data_inicial, end=data_final)

    # Contando apenas os dias úteis no intervalo
    dias_uteis = len([dia for dia in intervalo if dia.weekday() < 5])

    return dias_uteis

def dias_uteis_entre_datas(data_inicial, data_final):
    # Converte as strings de data para objetos datetime
    data_inicial = np.datetime64(data_inicial)
    data_final = np.datetime64(data_final)

    # Calcula o número de dias úteis
    dias_uteis = np.busday_count(data_inicial, data_final)

    return dias_uteis

def tempo_os():

    """
    Função para calcular tempo de execução de os. 
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

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

        # df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(
            lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0

    df_timeline = df_timeline[['datafim', 'diferenca']]
    df_agrupado = df_timeline.groupby(
        'datafim')['diferenca'].sum().reset_index()

    # df_timeline = df_timeline.values.tolist()

    return df_agrupado

def cards_get(query):

    """
    Função para gerar dados de quantidade de os em aberto, em execução, aguardadno material e fechada.
    """
    
    # query = """
    #     SELECT *
    #     FROM tb_ordens
    #     WHERE (ordem_excluida IS NULL OR ordem_excluida = FALSE)
    #     """
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cards = pd.read_sql_query(query, conn)
    # cards = cards[cards['id_ordem'] == 837]

    # cards['status'] = cards['status'].fillna('Em espera')

    # cards = cards.sort_values(by='n_ordem', ascending=True)

    # cards = cards.drop_duplicates(subset='id_ordem', keep='last')

    # em_execucao = cards[cards['status'] == 'Em espera'][['id_ordem', 'status','n_ordem']]

    # print(em_execucao)

    cards = cards.groupby(['status_atualizado'])['status_atualizado'].count()

    # Crie um dicionário para armazenar os resultados
    status_dict = {}
    for status, qt_os in cards.items():
        status_dict[status] = qt_os

    # Certifique-se de que todas as chaves estão presentes no dicionário, mesmo que com valor 0
    lista_qt = [
        status_dict.get('Aguardando material', 0),
        status_dict.get('Finalizada', 0),
        status_dict.get('Em execução', 0),
        status_dict.get('Aguardando OK', 0),
        status_dict.get('Em espera', 0)
    ]

    return lista_qt

def cards_post(query):

    """
    Função para gerar dados de quantidade de os em aberto, em execução, aguardadno material e fechada.
    """
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cards = pd.read_sql_query(query, conn)
    # cards = cards[cards['id_ordem'] == 837]

    # cards['status'] = cards['status'].fillna('Em espera')

    # cards = cards.sort_values(by='n_ordem', ascending=True)

    # cards = cards.drop_duplicates(subset='id_ordem', keep='last')

    # em_execucao = cards[cards['status'] == 'Em espera'][['id_ordem', 'status_atualizado','n_ordem']]

    cards = cards.groupby(['status_atualizado'])['status_atualizado'].count()

    # Crie um dicionário para armazenar os resultados
    status_dict = {}
    for status, qt_os in cards.items():
        status_dict[status] = qt_os

    # Certifique-se de que todas as chaves estão presentes no dicionário, mesmo que com valor 0
    lista_qt = [
        # status_dict.get('Em espera', 0),
        status_dict.get('Aguardando material', 0),
        status_dict.get('Finalizada', 0),
        status_dict.get('Em execução', 0),
        status_dict.get('Aguardando OK', 0),
        status_dict.get('Em espera', 0)
    ]

    print(lista_qt)

    return lista_qt

def tabela_maquinas():

    sql_tb_maquinas = """
    select setor,codigo, COALESCE(NULLIF(apelido, ''), codigo) AS codigo_tratado from tb_maquinas
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(sql_tb_maquinas,)
    tabela_maquinas = cur.fetchall()

    lista_tabela_maquinas = []

    for maquina in tabela_maquinas:
        lista_tabela_maquinas.append({'setor':maquina[0],'codigo':maquina[1],'codigo_tratado':maquina[2]})
    
    return pd.DataFrame(lista_tabela_maquinas,)

def agrupando_dados(data):

    # Convertendo a lista de dicionários para um DataFrame
    df = pd.DataFrame(data)

    # Agrupando por 'maquina' e calculando a média
    resultados_agrupados = df.groupby('maquina').agg({'qt_execucao': 'mean', 'resultado_mttr': 'mean'}).reset_index()

    # Renomeando as colunas
    resultados_agrupados = resultados_agrupados.rename(columns={'qt_execucao': 'qt_execucao', 'resultado_mttr': 'resultado_mttr'})

    # Convertendo de volta para uma lista de dicionários
    resultado_final = resultados_agrupados.to_dict('records')

    return resultado_final

def maquinas_importantes():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_maquinas_importantes = """
    select codigo from public.tb_planejamento_anual
    """

    cur.execute(query_maquinas_importantes)
    maquinas = cur.fetchall()

    maquinas_list = []

    for maquina in maquinas:
        maquinas_list.append(maquina[0])

    return tuple(maquinas_list)

@routes_bp.route('/api/calculo_mtbf_maquina', methods=['POST', 'GET'])
def calculo_mtbf_maquina():
    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data.get('dia_inicial')
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])

        query_mtbf = """
            SELECT
                COALESCE(NULLIF(apelido, ''), codigo) AS apelido,
                COUNT(*) AS quantidade_maquinas,
                SUM(duracao_total_em_horas) AS duracao_total_em_horas
            FROM (
                SELECT
                    t1.maquina,
                    CASE 
                        WHEN t1.maquina_parada = 'true' OR t2.parada3 = 'true' THEN 
                            EXTRACT(EPOCH FROM 
                                TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                                TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600
                        ELSE 0
                    END AS duracao_total_em_horas
                FROM
                    tb_ordens AS t1
                LEFT JOIN
                    tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                WHERE
                    (t1.maquina_parada = 'true' OR t2.parada3 = 'true') AND
                    (t1.datainicio BETWEEN %s AND %s) AND 
                    (ordem_excluida isnull)
            ) AS subquery
            LEFT JOIN tb_maquinas as m on m.codigo = split_part(subquery.maquina,' - ',1) 
            where 1=1 
        """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")

            print(dia_inicial)
            print(dia_final)
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:

            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_mtbf += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_mtbf += f' and codigo in {maquinas_importantes()}'

        query_mtbf += ' GROUP BY codigo,apelido,maquina;'

        conn = None
        resultado_mtbf_maquina = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_mtbf, (dia_inicial, dia_final))
            mtbf_maquina = cur.fetchall()

            # Processar resultados conforme necessário
            for maquina, qt_execucao, valor_decimal in mtbf_maquina:
                if valor_decimal is not None:
                    resultado = abs(round((horas_trabalhadas_otimo - float(valor_decimal)) / qt_execucao, 2))
                    resultado_mtbf_maquina.append({'maquina': maquina, 'resultado_mtbf': resultado, 'qt_execucao': qt_execucao})

                else:
                    resultado_mtbf_maquina['resultado_mtbf'] = abs(resultado_mtbf_maquina['resultado_mtbf'])            
                    resultado_mtbf_maquina.append({'maquina': maquina, 'resultado_mtbf': None, 'qt_execucao': None})
                    
            if not data_filtro:

                df_historico_mtbf = pd.read_csv("mtbf_historico.csv", sep=";")
                df_historico_mtbf['maquina'] = df_historico_mtbf['maquina'].apply(lambda x: x.split(" - ")[0])
                df_historico_mtbf['resultado_mtbf'] = pd.to_timedelta(df_historico_mtbf['historico_mtbf']).dt.total_seconds() / 3600
                
                if setores_selecionados:
                    setores_selecionados = [setor.strip() for setor in setores_selecionados]
                    df_historico_mtbf = df_historico_mtbf[df_historico_mtbf['setor'].isin(setores_selecionados)]
                
                if maquinas_importante:
                    df_historico_mtbf = df_historico_mtbf[df_historico_mtbf['setor'].isin(maquinas_importantes())]

                df_historico_mtbf = df_historico_mtbf[['maquina','resultado_mtbf']]
                
                resultado_mtbf_maquina = pd.DataFrame(resultado_mtbf_maquina)
                join_df = pd.concat([resultado_mtbf_maquina,df_historico_mtbf], ignore_index=True)
                join_df = join_df.groupby('maquina').agg({
                    'resultado_mtbf': 'mean',
                }).reset_index()

                join_df['resultado_mtbf'] = abs(join_df['resultado_mtbf'])

                resultado_mtbf_maquina = join_df.to_dict(orient='records')

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado_mtbf_maquina})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_mtbf_setor', methods=['POST','GET'])
def calculo_mtbf_setor():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data.get('dia_inicial')
        # dia_final = data.get('dia_final')
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])

        print(data)

        query_mtbf = """
                SELECT
                    setor,
                    COUNT(*) AS quantidade_setor,
                    SUM(duracao_total_em_horas) AS duracao_total_em_horas
                FROM (
                    SELECT
                        t1.setor,
                        CASE 
                            WHEN t1.maquina_parada = 'true' OR t2.parada3 = 'true' THEN 
                                EXTRACT(EPOCH FROM 
                                    TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                                    TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600 
                            ELSE 0
                        END AS duracao_total_em_horas
                    FROM
                        tb_ordens AS t1
                    LEFT JOIN
                        tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                    WHERE
                    (t1.maquina_parada = 'true' OR t2.parada3 = 'true') AND
                    (t1.datainicio BETWEEN %s AND %s) AND
                    (ordem_excluida isnull)
                ) AS subquery
                where 1=1
            """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:

            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_mtbf += f' and setor in {setores_selecionados_lista}'

        query_mtbf += ' GROUP BY setor;'

        conn = None
        resultado_mtbf_setor = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_mtbf, (dia_inicial, dia_final))
            mtbf_setor = cur.fetchall()

            # Processar resultados conforme necessário
            for setor, qt_execucao, valor_decimal in mtbf_setor:
                if valor_decimal is not None:
                    resultado = round((horas_trabalhadas_otimo - float(valor_decimal)) / qt_execucao, 2)
                    resultado_mtbf_setor.append({'setor': setor, 'resultado_mtbf': resultado, 'qt_execucao': qt_execucao})
                else:
                    resultado_mtbf_setor.append({'setor': setor, 'resultado_mtbf': None, 'qt_execucao': None})

            if not data_filtro:

                df_historico_mtbf = pd.read_csv("mtbf_historico.csv", sep=";")
                df_historico_mtbf['resultado_mtbf'] = pd.to_timedelta(df_historico_mtbf['historico_mtbf']).dt.total_seconds() / 3600
                
                if setores_selecionados:
                    setores_selecionados = [setor.strip() for setor in setores_selecionados]
                    df_historico_mtbf = df_historico_mtbf[df_historico_mtbf['setor'].isin(setores_selecionados)]
                
                df_historico_mtbf = df_historico_mtbf[['setor','resultado_mtbf']]
                
                resultado_mtbf_setor = pd.DataFrame(resultado_mtbf_setor)
                join_df = pd.concat([resultado_mtbf_setor,df_historico_mtbf], ignore_index=True)
                join_df = join_df.groupby('setor').agg({
                    'resultado_mtbf': 'mean',
                }).reset_index()

                resultado_mtbf_setor = join_df.to_dict(orient='records')
            
        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado_mtbf_setor})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_mttr_maquina', methods=['POST', 'GET'])
def calculo_mttr_maquina():
    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()
        
        print(data)
        
        # dia_inicial = data.get('dia_inicial')
        # dia_final = data.get('dia_final')
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])

        query_mttr = """
            SELECT
                COALESCE(NULLIF(apelido, ''), codigo) AS apelido,
                COUNT(*) AS quantidade_maquinas,
                SUM(duracao_total_em_horas) AS duracao_total_em_horas
            FROM (
                SELECT
                    t1.maquina,
                    EXTRACT(EPOCH FROM 
                        TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                        TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600  
                    as duracao_total_em_horas
                FROM
                    tb_ordens AS t1
                LEFT JOIN
                    tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                WHERE
                    (t1.datainicio BETWEEN %s AND %s) AND 
                    (ordem_excluida isnull)
            ) AS subquery
            LEFT JOIN tb_maquinas as m on m.codigo = split_part(subquery.maquina,' - ',1) 
            where 1=1
        """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:
            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_mttr += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_mttr += f' and codigo in {maquinas_importantes()}'

        query_mttr += ' GROUP BY codigo,apelido,maquina;'

        conn = None
        resultado_mttr_maquina = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_mttr, (dia_inicial, dia_final))
            mttr_maquina = cur.fetchall()

            # Processar resultados conforme necessário
            for maquina, qt_execucao, valor_decimal in mttr_maquina:
                if valor_decimal is not None:
                    resultado = round((float(valor_decimal)) / qt_execucao, 2)
                    resultado_mttr_maquina.append({'maquina': maquina, 'resultado_mttr': resultado, 'qt_execucao': qt_execucao})
                else:
                    resultado_mttr_maquina.append({'maquina': maquina, 'resultado_mttr': None, 'qt_execucao': None})

            resultado_mttr_maquina = agrupando_dados(resultado_mttr_maquina)
            # resultado_mttr_maquina = sorted(resultado_mttr_maquina, key=lambda x: x['resultado_mttr'])


        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado_mttr_maquina})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_mttr_setor', methods=['POST', 'GET'])
def calculo_mttr_setor():
    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data.get('dia_inicial')
        # dia_final = data.get('dia_final')
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])

        print(data)

        query_mttr = f"""
            SELECT
                setor,
                COUNT(*) AS quantidade_setores,
                SUM(duracao_total_em_horas) AS duracao_total_em_horas
            FROM (
                SELECT
                    t1.setor,
                    EXTRACT(EPOCH FROM 
                        TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                        TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600  
                    as duracao_total_em_horas
                FROM
                    tb_ordens AS t1
                LEFT JOIN
                    tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                WHERE
                    (t1.datainicio BETWEEN %s AND %s) AND
                    (ordem_excluida isnull)
            ) AS subquery
            where 1=1
        """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:
            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_mttr += f' and setor in {setores_selecionados_lista}'

        query_mttr += ' GROUP BY setor;'

        conn = None
        resultado_mttr_setor = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_mttr, (dia_inicial, dia_final))
            mttr_setor = cur.fetchall()

            # Processar resultados conforme necessário
            for setor, qt_execucao, valor_decimal in mttr_setor:
                if valor_decimal is not None:
                    resultado = round((float(valor_decimal)) / qt_execucao, 2)
                    resultado_mttr_setor.append({'setor': setor, 'resultado_mttr': resultado, 'qt_execucao': qt_execucao})
                else:
                    resultado_mttr_setor.append({'setor': setor, 'resultado_mttr': None, 'qt_execucao': None})

            # resultado_mttr_setor = sorted(resultado_mttr_setor, key=lambda x: x['resultado_mttr'])


        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado_mttr_setor})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_horas_trabalhadas_tipo_manutencao', methods=['POST','GET'])
def calculo_horas_trabalhadas_tipo():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])

        query_horas_trabalhadas = """
                SELECT
                    tipo_manutencao,
                    COUNT(*) AS ocorrencias,
                    SUM(horas) AS horas
                FROM (
                    SELECT
                        t1.setor,
                        t1.maquina,
                        t1.tipo_manutencao,
                        EXTRACT(EPOCH FROM 
                            TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                            TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600  
                        as horas
                    FROM
                        tb_ordens AS t1
                    WHERE
                        (t1.datainicio BETWEEN %s AND %s) AND
                        (ordem_excluida isnull) 

                ) AS subquery
                WHERE 1=1
            """

        if setores_selecionados:
            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_horas_trabalhadas += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_horas_trabalhadas += f' and maquina in {maquinas_importantes()}'

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        query_horas_trabalhadas += ' GROUP BY tipo_manutencao;'

        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_horas_trabalhadas, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()

            # Processar resultados conforme necessário
            for tipo, ocorrencias, horas in resultados_query:
                resultado.append({'tipo': tipo, 'ocorrencias': ocorrencias, 'horas': round(float(horas),2)})

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_horas_trabalhadas_area_manutencao', methods=['POST','GET'])
def calculo_horas_trabalhadas_area():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data['dia_inicial']
        # dia_final = data['dia_final']
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])
                
        query_horas_trabalhadas = """
                SELECT
                    area_manutencao,
                    COUNT(*) AS ocorrencias,
                    SUM(horas) AS horas
                FROM (
                    SELECT
                        t1.setor,
                        t1.maquina,
                        t1.area_manutencao,
                        EXTRACT(EPOCH FROM 
                            TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                            TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600  
                        as horas
                    FROM
                        tb_ordens AS t1
                    WHERE
                        (t1.datainicio BETWEEN %s AND %s) AND
                        (ordem_excluida isnull) 

                ) AS subquery
                WHERE 1=1 
                
            """
        
        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:
            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_horas_trabalhadas += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_horas_trabalhadas += f' and maquina in {maquinas_importantes()}'

        query_horas_trabalhadas += ' GROUP BY area_manutencao;'

        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_horas_trabalhadas, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()

            # Processar resultados conforme necessário
            for area, ocorrencias, horas in resultados_query:
                resultado.append({'area': area, 'ocorrencias': ocorrencias, 'horas': round(float(horas),2)})
  

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_horas_setor', methods=['POST','GET'])
def calculo_horas_setor():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data['dia_inicial']
        # dia_final = data['dia_final']
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])
        
        query_horas_trabalhadas = """
                SELECT
                    setor,
                    COUNT(*) AS ocorrencias,
                    SUM(horas) AS horas
                FROM (
                    SELECT
                        t1.setor,
                        t1.maquina,
                        EXTRACT(EPOCH FROM 
                            TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                            TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600  
                        as horas
                    FROM
                        tb_ordens AS t1
                    WHERE
                        (t1.datainicio BETWEEN %s AND %s) AND
                        (ordem_excluida isnull) 

                ) AS subquery
                WHERE 1=1
            """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:
            # setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_horas_trabalhadas += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_horas_trabalhadas += f' and maquina in {maquinas_importantes()}'

        query_horas_trabalhadas += ' GROUP BY setor;'

        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_horas_trabalhadas, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()

            # Processar resultados conforme necessário
            for setor, ocorrencias, horas in resultados_query:
                resultado.append({'setor': setor, 'ocorrencias': ocorrencias, 'horas': round(float(horas),2)})
  

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_cards', methods=['POST','GET'])
def calculo_cards():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        # dia_inicial = data['dia_inicial']
        # dia_final = data['dia_final']
        data_filtro = data.get('data_filtro')
        setores_selecionados = data.get('setores_selecionados', [])
        maquinas_importante = data.get('maquinasFavoritas', [])  

        query_cards = """
            WITH RankedOrders AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_ordem ORDER BY n_ordem DESC) AS rn,
                COALESCE(status, 'Em espera') as status_atualizado
            FROM
                tb_ordens)
                SELECT
                status, count(status) as quantidade_status
                FROM
                RankedOrders
                WHERE
                rn = 1
            AND (ordem_excluida IS NULL OR ordem_excluida = FALSE)
            AND ultima_atualizacao BETWEEN %s AND %s::date + 1
    
            """

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        if setores_selecionados:

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

            query_cards += f' and setor in {setores_selecionados_lista}'

        if maquinas_importante:
            query_cards += f' and maquina in {maquinas_importantes()}'

        query_cards += ' group by status'
        
        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query_cards, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()

            # Processar resultados conforme necessário
            for status, quantidade in resultados_query:
                resultado.append({'status': status, 'quantidade': quantidade})
  
        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado})

    except Exception as e:
        return jsonify({'error': str(e)})

def calculo_disponibilidade_maquina_parada(data_filtro):

    try:
        # Obtenha dados da solicitação POST
        # data = request.get_json()

        # dia_inicial = data['dia_inicial']
        # dia_final = data['dia_final']
        # data_filtro = data.get('data_filtro')

        query = """
        WITH ordens_ultima_atualizacao AS (
            SELECT 
                t1.setor,
                split_part(t1.maquina,' - ',1) as maquina,
                t1.datainicio,
                t1.horainicio,
                t1.datafim,
                t1.horafim,
                t1.dataabertura,
                t1.ultima_atualizacao,
                t2.parada3, 
                t2.data3,
                t1.status,
                t1.maquina_parada,
                t1.ordem_excluida,
                CURRENT_DATE AS data_de_hoje,
                ROW_NUMBER() OVER (PARTITION BY t1.id_ordem ORDER BY t1.ultima_atualizacao DESC) AS rn
            FROM tb_ordens t1
            LEFT JOIN tb_paradas t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
            )
            SELECT
                setor,
                split_part(maquina,' - ',1) as maquina,
                coalesce(datainicio,dataabertura) as datainicio,
                CASE
                    WHEN datainicio IS NULL THEN
                        ABS(EXTRACT(EPOCH FROM (CURRENT_DATE - dataabertura)) / 3600)
                    ELSE
                        ABS(EXTRACT(EPOCH FROM AGE(CURRENT_DATE, (datainicio || ' ' || horainicio)::timestamp)) / 3600)
                END AS diferenca_data_hoje_em_horas
            FROM ordens_ultima_atualizacao
            WHERE rn = 1 and status != 'Finalizada' and (maquina_parada = 'true' or parada3 = 'true') and ordem_excluida isnull;
        """
        
        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()
            
            # Processar resultados conforme necessário
            for item in resultados_query:

                # dia_inicial_datetime = datetime.strptime(dia_inicial, "%Y-%m-%d")
                # dia_final_datetime = datetime.strptime(dia_final, "%Y-%m-%d")

                # datainicio_sem_tz = item[2].astimezone(timezone.utc).replace(tzinfo=None)
                
                # if dia_final_datetime < datainicio_sem_tz:
                #     continue 

                # elif dia_inicial_datetime <= datainicio_sem_tz <= dia_final_datetime:

                #     horas_otimo = (dia_final_datetime - dia_inicial_datetime).total_seconds() / 3600
                #     horas_paradas = abs((datainicio_sem_tz - dia_final_datetime).total_seconds() / 3600)
                #     disponibilidade = (horas_otimo - horas_paradas) / horas_otimo

                resultado.append({'maquina': item[1], 'data_inicio':item[2], 'tempo_total_parada':float(item[3])})

                # else:
                #     disponibilidade = 0
                    
                #     resultado.append({'maquina': item[1], 'disponibilidade':round(disponibilidade*100, 2)})
            
        except Exception as e:
            return ({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return ({'resultados': resultado})

    except Exception as e:
        return ({'error': str(e)})

def disponibilidade_final(datainicio,datafim,setor=None,maquina_importantes=None):

    # parada1 = maquina parada desde a abertura da os
    # parada2 = exec feita com maquina parada
    # parada3 = ao finalizar a exec a maquina funcionou

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if maquina_importantes:
        maquinas_filter = maquinas_importantes()
    
    # Criando o DataFrame
    # df = pd.read_csv('ordens.csv')
    query = """select 
                to2.id_ordem,
                to2.n_ordem,
                split_part(to2.maquina,' - ',1) as maquina,
                to2.setor,
                to2.dataabertura - INTERVAL '6 hours' AS dataabertura,
                to2.datainicio,
                to2.horainicio,
                to2.datafim,
                to2.horafim,
                to2.status,
                tp.parada1,
                tp.parada2,
                tp.parada3
            from public.tb_ordens to2
            left join public.tb_paradas tp on to2.id_ordem = tp.id_ordem and to2.n_ordem = tp.n_ordem
            where parada1 notnull and maquina != '' and maquina != 'ETE' and maquina != 'Telhado ' and maquina != 'Outros'
            """

    df = pd.read_sql_query(query, conn)

    df = df.sort_values(by=['id_ordem', 'n_ordem'])
    df['status_geral'] = ''

    query_maquinas = """
        select codigo,setor from public.tb_maquinas tm 
    """

    df_maquinas = pd.read_sql_query(query_maquinas, conn)

    # Função para obter o status geral
    def obter_status_geral(grupo):
        # O status geral é o status do último registro de cada grupo
        status_geral = grupo.iloc[-1]['status']
        grupo['status_geral'] = status_geral
        return grupo

    # Aplicando a função ao DataFrame agrupado por 'ordem'
    df = df.groupby('id_ordem').apply(obter_status_geral).reset_index(drop=True)
    
    # Convertendo colunas de data e hora para objetos datetime
    # df['dataabertura'] = df['dataabertura'].apply(lambda x: x[:len(x)-10])
    df['dataabertura'] = pd.to_datetime(df['dataabertura'], format='%Y-%m-%d %H:%M:%S').dt.tz_localize(None)

    df['datainicio'] = df['datainicio'].astype(str)
    df['horainicio'] = df['horainicio'].astype(str)
    df['datainicio'] = pd.to_datetime(df['datainicio'] + ' ' + df['horainicio'], format='%Y-%m-%d %H:%M:%S')

    df['datafim'] = df['datafim'].astype(str)
    df['horafim'] = df['horafim'].astype(str)
    df['datafim'] = pd.to_datetime(df['datafim'] + ' ' + df['horafim'], format='%Y-%m-%d %H:%M:%S')

    df['parada1'] = df['parada1'].map({'true': True, 'false': False})
    df['parada2'] = df['parada2'].map({'true': True, 'false': False})
    df['parada3'] = df['parada3'].map({'true': True, 'false': False})

    df['parada1'] = df['parada1'].astype(bool)
    df['parada2'] = df['parada2'].astype(bool)
    df['parada3'] = df['parada3'].astype(bool)

    # Inicializa a coluna datafim_real com pd.NaT
    # df['datafim_real'] = pd.NaT

    def ultimo_dia_do_mes(ano, mes):
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        return datetime(ano, mes, ultimo_dia)

    novos_registros=[]

    for i in range(len(df) - 1):
        if df['id_ordem'][i] == df['id_ordem'][i + 1] and df['parada1'][i] and df['status'][i] != 'Finalizada':
            if df['dataabertura'][i].month < df['datainicio'][i].month and df['n_ordem'][i] == 1:
                
                qnt_novos_registro = df['datainicio'][i].month - df['dataabertura'][i].month

                for m in range(df['dataabertura'][i].month+1, df['dataabertura'][i].month+1 + qnt_novos_registro):
                    
                    ultimo_dia_mes = ultimo_dia_do_mes(df['datainicio'][i].year, m)

                    if ultimo_dia_mes > datetime.now():
                        ultimo_dia_mes = datetime.now()
                   
                    novo_registro = {
                        'id_ordem': df['id_ordem'][i],  # Manter o mesmo id de ordem
                        'n_ordem': df['n_ordem'][i],  # Manter o mesmo número de ordem
                        'maquina': df['maquina'][i],  # Manter a mesma máquina
                        'setor': df['setor'][i],  # Manter o mesmo setor
                        'dataabertura': df['dataabertura'][i] ,
                        'datainicio': datetime(df['datainicio'][i].year, m, 1),  # Primeiro dia do mês
                        'datafim': ultimo_dia_mes,  # Último dia do mês
                        'status': df['status'][i],
                        'parada1':df['parada1'][i],
                        'parada2':df['parada2'][i],
                        'parada3':df['parada3'][i],
                        'status_geral':df['status_geral'][i],
                    }

                    novos_registros.append(novo_registro)

                ultimo_dia_mes = ultimo_dia_do_mes(df['dataabertura'][i].year, df['dataabertura'][i].month)
                
                df.at[i, 'datafim'] = ultimo_dia_mes
                df.at[i, 'datainicio'] = df['dataabertura'][i]

            elif df['datainicio'][i].month == df['datainicio'][i - 1].month:
                df['datainicio'][i] = df['datafim'][i - 1]

            elif df['dataabertura'][i].month < df['datainicio'][i].month:
                df.at[i, 'datainicio'] = datetime(df['datainicio'][i].year,df['datainicio'][i].month,1) # 

        elif i != 0:

            if df['id_ordem'][i] == df['id_ordem'][i - 1] and df['parada1'][i] and df['status'][i] == 'Finalizada':
                df.at[i, 'datainicio'] = df['datafim'][i - 1]

            elif df['id_ordem'][i] == df['id_ordem'][i - 1]:
                df.at[i, 'datainicio'] = df['datafim'][i - 1]
                df.at[i, 'datafim'] = datetime.now()   
            
            elif df['parada1'][i] and df['status'][i] != 'Finalizada':
                df.at[i, 'datafim'] = datetime.now()  

    # Crie um DataFrame a partir da lista de novos registros
    df_novos_registros = pd.DataFrame(novos_registros)

    # Concatene o DataFrame original com o DataFrame dos novos registros
    df = pd.concat([df, df_novos_registros], ignore_index=True)

    # Função para calcular o tempo de parada dentro do horário de 7h às 17h
    def calcular_tempo_parada(row, now):
        if row['parada1'] and row['parada2']:
            inicio = row['dataabertura']
        elif row['parada1'] and not row['parada2']:
            inicio = row['dataabertura']
        elif not row['parada1'] and row['parada2']:
            inicio = row['datainicio']
        # elif not row['parada1'] and row['parada2']:
        #     inicio = row['datainicio']
        else:
            return timedelta(0)

        # Determina o fim do intervalo de tempo com base no status geral
        # if row['status_geral'] == 'Finalizada':
        fim = row['datafim']
        # else:
        #     fim = now

        # Define os intervalos de trabalho
        start_working_hour = 7
        end_working_hour = 17

        tempo_parada = timedelta(0)
        current = inicio

        while current < fim:
            start_of_day = datetime.combine(current.date(), datetime.min.time()) + timedelta(hours=start_working_hour)
            end_of_day = datetime.combine(current.date(), datetime.min.time()) + timedelta(hours=end_working_hour)

            if current < start_of_day:
                current = start_of_day
            
            if current < end_of_day:
                end_current_period = min(fim, end_of_day)
                tempo_parada += end_current_period - current

            current = datetime.combine(current.date() + timedelta(days=1), datetime.min.time()) + timedelta(hours=start_working_hour)

        return tempo_parada

    # Aplicar a função ao DataFrame
    now = datetime.now()
    df['tempo_parada'] = df.apply(lambda row: calcular_tempo_parada(row, now), axis=1)
    # filtro entra aqui
    df_filtro = df[(df['datainicio']>datainicio) & (df['datainicio']<datafim)]

    dias_uteis = dias_uteis_entre_datas(datainicio,datafim)

    # Agrupando o tempo de parada total por ordem
    tempo_parada_total_por_ordem = df_filtro.groupby('maquina')['tempo_parada'].sum().reset_index()

    df_maquinas['horas_funcionamento_bom'] = 9*dias_uteis
    
    df_final = df_maquinas.merge(tempo_parada_total_por_ordem, how='left', right_on='maquina',left_on='codigo')
    df_final['tempo_parada'] = df_final['tempo_parada'].fillna(timedelta(0))

    df_final['tempo_parada_horas'] = df_final['tempo_parada'].dt.total_seconds() / 3600
    df_final['disponibilidade_horas'] = 1 - (df_final['tempo_parada_horas'] / df_final['horas_funcionamento_bom'])
    df_final['tempo_parada'] = df_final['tempo_parada'].dt.total_seconds() / 3600 

    if maquina_importantes:
        df_final = df_final[df_final['maquina'].isin(maquinas_filter)]
    
    if setor:
        df_final = df_final[df_final['setor'].isin(setor)]

    disp_maquinas = df_final.sort_values(by='disponibilidade_horas')[['codigo','setor','tempo_parada','disponibilidade_horas']]
    disp_setor = disp_maquinas.groupby('setor')['disponibilidade_horas'].mean().reset_index()
    
    disp_maquinas_dict = disp_maquinas.to_dict(orient='records')
    disp_setor_dict = disp_setor.to_dict(orient='records')

    return disp_maquinas_dict,disp_setor_dict

@routes_bp.route('/api/calculo_setor_parada', methods=['POST','GET'])
def calculo_disponibilidade_setor_parada():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        dia_inicial = data['dia_inicial']
        dia_final = data['dia_final']
        
        query = """
        WITH ordens_ultima_atualizacao AS (
            SELECT 
                t1.setor,
                split_part(t1.maquina,' - ',1) as maquina,
                t1.datainicio,
                t1.horainicio,
                t1.datafim,
                t1.horafim,
                t1.dataabertura,
                t1.ultima_atualizacao,
                t2.parada3, 
                t2.data3,
                t1.status,
                t1.maquina_parada,
                t1.ordem_excluida,
                CURRENT_DATE AS data_de_hoje,
                ROW_NUMBER() OVER (PARTITION BY t1.id_ordem ORDER BY t1.ultima_atualizacao DESC) AS rn
            FROM tb_ordens t1
            LEFT JOIN tb_paradas t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
            )
            SELECT
                setor,
                split_part(maquina,' - ',1) as maquina,
                coalesce(datainicio,dataabertura) as datainicio,
                CASE
                    WHEN datainicio IS NULL THEN
                        ABS(EXTRACT(EPOCH FROM (CURRENT_DATE - dataabertura)) / 3600)
                    ELSE
                        ABS(EXTRACT(EPOCH FROM AGE(CURRENT_DATE, (datainicio || ' ' || horainicio)::timestamp)) / 3600)
                END AS diferenca_data_hoje_em_horas
            FROM ordens_ultima_atualizacao
            WHERE rn = 1 and status != 'Finalizada' and (maquina_parada = 'true' or parada3 = 'true') and ordem_excluida isnull;
        """

        conn = None
        resultado = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9
            cur.execute(query, (dia_inicial, dia_final))
            resultados_query = cur.fetchall()
            
            # Processar resultados conforme necessário
            for item in resultados_query:

                resultado.append({'setor': item[0], 'data_inicio':item[2], 'tempo_total_parada':float(item[3])})

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/calculo_setor_parada', methods=['POST','GET'])
def tempo_maquina_parada_api(data_filtro):

    try:
        # Obtenha dados da solicitação POST
        # data = request.get_json()

        # dia_inicial = data.get('dia_inicial')
        # data_filtro = data.get('data_filtro')
        # dia_final = data.get('dia_final')
        # setores_selecionados = data.get('setores_selecionados', [])
        # maquinas_importante = data.get('maquinasFavoritas', [])

        if data_filtro:
            # Divida a string com base no caractere "-"
            datas = data_filtro.split(" - ")

            dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
            dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        
        else:
            dia_inicial = '2023-01-06'
            dia_final = datetime.now().date().strftime('%Y-%m-%d')

        query_mtbf = """
            SELECT
                COALESCE(NULLIF(apelido, ''), codigo) AS apelido,
                COUNT(*) AS quantidade_maquinas,
                SUM(duracao_total_em_horas) AS duracao_total_em_horas
            FROM (
                SELECT
                    t1.maquina,
                    CASE 
                        WHEN t1.maquina_parada = 'true' OR t2.parada3 = 'true' THEN 
                            EXTRACT(EPOCH FROM 
                                TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                                TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600
                        ELSE 0
                    END AS duracao_total_em_horas
                FROM
                    tb_ordens AS t1
                LEFT JOIN
                    tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                WHERE
                    (t1.maquina_parada = 'true' OR t2.parada3 = 'true') AND
                    (t1.datainicio BETWEEN %s AND %s) AND 
                    (ordem_excluida isnull)
            ) AS subquery
            LEFT JOIN tb_maquinas as m on m.codigo = split_part(subquery.maquina,' - ',1) 
            where 1=1 
        """

        # if setores_selecionados:

        #     setores_selecionados = [setor.strip() for setor in setores_selecionados]
        #     setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

        #     query_mtbf += f' and setor in {setores_selecionados_lista}'

        # if maquinas_importante:
        #     query_mtbf += f' and codigo in {maquinas_importantes()}'

        query_mtbf += ' GROUP BY codigo,apelido,maquina;'

        conn = None
        resultado_mtbf_maquina = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute(query_mtbf, (dia_inicial, dia_final))
            mtbf_maquina = cur.fetchall()
            horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9

            # Processar resultados conforme necessário
            for item in mtbf_maquina:
                if item[0] is not None:
                    resultado_mtbf_maquina.append({'maquina': item[0], 'tempo_parada': float(item[2]),'tempo_planejado':float(horas_trabalhadas_otimo)})

        except Exception as e:
            return ({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return ({'resultados': resultado_mtbf_maquina})

    except Exception as e:
        return ({'error': str(e)})

@routes_bp.route('/api/calculo_setor_tempo_parada', methods=['POST', 'GET'])
def tempo_setor_parada():

    try:
        # Obtenha dados da solicitação POST
        data = request.get_json()

        dia_inicial = data.get('dia_inicial')
        dia_final = data.get('dia_final')
        # setores_selecionados = data.get('setores_selecionados', [])
        # maquinas_importante = data.get('maquinasFavoritas', [])

        query_mtbf = """
                SELECT
                    setor,
                    COUNT(*) AS quantidade_setor,
                    SUM(duracao_total_em_horas) AS duracao_total_em_horas
                FROM (
                    SELECT
                        t1.setor,
                        CASE 
                            WHEN t1.maquina_parada = 'true' OR t2.parada3 = 'true' THEN 
                                EXTRACT(EPOCH FROM 
                                    TO_TIMESTAMP(COALESCE(t1.datafim, t1.dataabertura::date) || ' ' || COALESCE(t1.horafim::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI') -
                                    TO_TIMESTAMP(COALESCE(t1.datainicio, t1.dataabertura::date) || ' ' || COALESCE(t1.horainicio::text, TO_CHAR(t1.dataabertura::timestamp, 'HH24:MI')), 'YYYY-MM-DD HH24:MI')) / 3600 
                            ELSE 0
                        END AS duracao_total_em_horas
                    FROM
                        tb_ordens AS t1
                    LEFT JOIN
                        tb_paradas AS t2 ON t1.id_ordem = t2.id_ordem AND t1.n_ordem = t2.n_ordem
                    WHERE
                    (t1.maquina_parada = 'true' OR t2.parada3 = 'true') AND
                    (t1.datainicio BETWEEN %s AND %s) AND
                    (ordem_excluida isnull)
                ) AS subquery
                where 1=1
            """

        # if setores_selecionados:

        #     setores_selecionados = [setor.strip() for setor in setores_selecionados]
        #     setores_selecionados_lista = "(" + ", ".join(f"'{setor}'" for setor in setores_selecionados) + ")" if setores_selecionados else "()"

        #     query_mtbf += f' and setor in {setores_selecionados_lista}'

        # if maquinas_importante:
        #     query_mtbf += f' and codigo in {maquinas_importantes()}'

        query_mtbf += ' GROUP BY setor;'

        conn = None
        resultado_mtbf_setor = []

        try:
            conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute(query_mtbf, (dia_inicial, dia_final))
            mtbf_maquina = cur.fetchall()
            horas_trabalhadas_otimo = dias_uteis_entre_datas(dia_inicial, dia_final) * 9

            # Processar resultados conforme necessário
            for item in mtbf_maquina:
                if item[0] is not None:
                    resultado_mtbf_setor.append({'setor': item[0], 'tempo_parada': float(item[1]),'tempo_planejado':float(horas_trabalhadas_otimo)})

        except Exception as e:
            return jsonify({'error': str(e)})

        finally:
            if conn:
                conn.close()

        return jsonify({'resultados': resultado_mtbf_setor})

    except Exception as e:
        return jsonify({'error': str(e)})

@routes_bp.route('/api/disponibilidade_maquina', methods=['POST', 'GET'])
def disponibilidade_maquina():
    
    data_get = request.get_json()
    
    # dia_inicial = data.get('dia_inicial')
    # dia_final = data.get('dia_final')
    data_filtro = data_get.get('data_filtro')
    setores_selecionados = data_get.get('setores_selecionados', [])
    maquinas_importante = data_get.get('maquinasFavoritas', [])
    
    # Defina a URL da API
    # url = 'https://manutencaocemag.onrender.com/api/calculo_maquina_tempo_parada'
    # url2 = 'https://manutencaocemag.onrender.com/api/calculo_maquina_parada'
    
    # Suponha que você tenha dados para enviar no corpo da solicitação (payload)
    # payload = {
        # 'dia_inicial': dia_inicial,
        # 'dia_final': dia_final,
        # 'data_filtro':data_filtro,
        # 'setores_selecionados':setores_selecionados,
        # 'maquinas_importante':maquinas_importante
    # }

    if data_filtro:
        # Divida a string com base no caractere "-"
        datas = data_filtro.split(" - ")

        dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
        dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
    
    else:
        dia_inicial = '2023-01-06'
        dia_final = datetime.now().date().strftime('%Y-%m-%d')

    # Faça a solicitação POST
    # response = requests.post(url, json=payload)
    # response2 = requests.post(url2, json=payload)

    # data = response.json()
    # data2 = response2.json()
    data = tempo_maquina_parada_api(data_filtro)
    data2 = calculo_disponibilidade_maquina_parada(data_filtro)

    tabela_mtbf = pd.DataFrame(data['resultados'])

    tabela_disponibilidade = pd.DataFrame(data2['resultados'])
    # tabela_disponibilidade['data_inicio'] = pd.to_datetime(tabela_disponibilidade['data_inicio'])
    tabela_disponibilidade['data_inicio'] = tabela_disponibilidade['data_inicio'].astype(str)
    tabela_disponibilidade['data_inicio'] = pd.to_datetime(tabela_disponibilidade['data_inicio'].apply(lambda x: x.split(' ')[0]))

    tabela_disponibilidade.loc[tabela_disponibilidade['data_inicio'] < pd.to_datetime(dia_inicial), 'data_inicio'] = pd.to_datetime(dia_inicial)
    tabela_disponibilidade['data_hoje'] = pd.to_datetime(dia_final)

    tabela_disponibilidade['tempo_total_parada'] = abs(tabela_disponibilidade['data_hoje'] - tabela_disponibilidade['data_inicio']).dt.days * 9
    tabela_disponibilidade['tempo_planejado'] = (pd.to_datetime(dia_final) - pd.to_datetime(dia_inicial)).days * 9
    tabela_disponibilidade['parada_antes'] = tabela_disponibilidade['data_inicio'].between(pd.to_datetime(dia_inicial), pd.to_datetime(dia_final))
    tabela_disponibilidade['disponibilidade'] = (tabela_disponibilidade['tempo_planejado'] - tabela_disponibilidade['tempo_total_parada']) / tabela_disponibilidade['tempo_planejado']

    tabela_disponibilidade = tabela_disponibilidade[['maquina','tempo_total_parada','tempo_planejado']]

    tabela_maquinas_ = tabela_maquinas()

    tabela_disponibilidade = tabela_maquinas_.merge(tabela_disponibilidade, left_on='codigo', right_on='maquina')  
    tabela_disponibilidade = tabela_disponibilidade[['codigo_tratado','tempo_total_parada','tempo_planejado']]
    tabela_disponibilidade = tabela_disponibilidade.rename(columns={'codigo_tratado':'maquina'})
    
    tabela_concatenada = pd.concat([tabela_disponibilidade, tabela_mtbf], ignore_index=True)
    
    if maquinas_importante:
    
        tabela_maquinas_ = tabela_maquinas_[tabela_maquinas_['codigo'].isin(list(maquinas_importantes()))] # filtro maquinas importantes

    if setores_selecionados:
        # setores_selecionados = json.loads(data_get["setores_selecionados"])

        setores_selecionados = [setor.strip() for setor in setores_selecionados]
        tabela_maquinas_ = tabela_maquinas_[tabela_maquinas_['setor'].isin(setores_selecionados)] # filtro setor

    tabela_maquinas_ = tabela_maquinas_[['codigo','codigo_tratado']]
    
    tabela_completa = tabela_maquinas_.merge(tabela_concatenada, how='left', left_on='codigo_tratado', right_on='maquina')

    tabela_completa['tempo_planejado'] = tabela_completa['tempo_planejado'].fillna(100)
    tabela_completa['tempo_total_parada'] = tabela_completa['tempo_total_parada'].fillna(0)

    tabela_completa['disponibilidade'] = round((tabela_completa['tempo_planejado'] - tabela_completa['tempo_total_parada']) / tabela_completa['tempo_planejado'], 2)
    
    if not data_filtro:

        df_historico_disponibilidade = pd.read_csv("disponibilidade_historico.csv", sep=";")
        df_historico_disponibilidade['maquina'] = df_historico_disponibilidade['maquina'].apply(lambda x: x.split(" - ")[0])

        if setores_selecionados:
            setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['setor'].isin(setores_selecionados)]
                
        if maquinas_importante:
            df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['setor'].isin(maquinas_importantes())]

        df_historico_disponibilidade['disponibilidade_historico_media'] = df_historico_disponibilidade['disponibilidade_historico_media'].apply(lambda x: float(x.replace(",",".").replace("%",""))/100)
        df_historico_disponibilidade.rename(columns={"disponibilidade_historico_media": "disponibilidade", "maquina": "codigo_tratado"}, inplace=True)
        df_historico_disponibilidade = df_historico_disponibilidade[['codigo_tratado','disponibilidade']]
        
        tabela_completa = pd.concat([tabela_completa,df_historico_disponibilidade],ignore_index=True)

    resultado_agrupado = tabela_completa.groupby('codigo_tratado').agg({
    'disponibilidade': 'mean'
    }).reset_index()
    
    resultado_agrupado = resultado_agrupado[['codigo_tratado','disponibilidade']]
    
    lista_resultado = resultado_agrupado.to_dict(orient='records')
    
    # lista_resultado = sorted(lista_resultado, key=lambda x: x['disponibilidade'])

    return jsonify({'resultados': lista_resultado})

@routes_bp.route('/api/disponibilidade_setor', methods=['POST', 'GET'])
def disponibilidade_setor():
    
    data_get = request.get_json()
    
    # dia_inicial = data.get('dia_inicial')
    # dia_final = data.get('dia_final')
    data_filtro = data_get.get('data_filtro')
    setores_selecionados = data_get.get('setores_selecionados', [])
    maquinas_importante = data_get.get('maquinasFavoritas', [])

    # Defina a URL da API
    # url = 'https://localhost:5000/api/calculo_maquina_tempo_parada'
    # url2 = 'https://manutencaocemag.onrender.com/api/calculo_maquina_parada'
    
    # Suponha que você tenha dados para enviar no corpo da solicitação (payload)
    # payload = {
        # 'dia_inicial': dia_inicial,
        # 'dia_final': dia_final,
        # 'data_filtro':data_filtro,
        # 'setores_selecionados':setores_selecionados,
        # 'maquinas_importante':maquinas_importante
    # }

    if data_filtro:
        # Divida a string com base no caractere "-"
        datas = data_filtro.split(" - ")

        dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
        dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")
    
    else:
        dia_inicial = '2023-01-06'
        dia_final = datetime.now().date().strftime('%Y-%m-%d')

    # Faça a solicitação POST
    # response = requests.post(url, json=payload)
    # response2 = requests.post(url2, json=payload)

    # data = response.json()
    # data2 = response2.json()
    data = tempo_maquina_parada_api(data_filtro)
    data2 = calculo_disponibilidade_maquina_parada(data_filtro)

    tabela_mtbf = pd.DataFrame(data['resultados'])

    tabela_disponibilidade = pd.DataFrame(data2['resultados'])
    tabela_disponibilidade['data_inicio'] = tabela_disponibilidade['data_inicio'].astype(str)
    tabela_disponibilidade['data_inicio'] = pd.to_datetime(tabela_disponibilidade['data_inicio'].apply(lambda x: x.split(' ')[0]))

    tabela_disponibilidade.loc[tabela_disponibilidade['data_inicio'] < pd.to_datetime(dia_inicial), 'data_inicio'] = pd.to_datetime(dia_inicial)
    tabela_disponibilidade['data_hoje'] = pd.to_datetime(dia_final)

    tabela_disponibilidade['tempo_total_parada'] = abs(tabela_disponibilidade['data_hoje'] - tabela_disponibilidade['data_inicio']).dt.days * 9
    tabela_disponibilidade['tempo_planejado'] = (pd.to_datetime(dia_final) - pd.to_datetime(dia_inicial)).days * 9
    tabela_disponibilidade['parada_antes'] = tabela_disponibilidade['data_inicio'].between(pd.to_datetime(dia_inicial), pd.to_datetime(dia_final))
    tabela_disponibilidade['disponibilidade'] = (tabela_disponibilidade['tempo_planejado'] - tabela_disponibilidade['tempo_total_parada']) / tabela_disponibilidade['tempo_planejado']
    tabela_disponibilidade = tabela_disponibilidade[['maquina','tempo_total_parada','tempo_planejado']]
    
    tabela_concatenada = pd.concat([tabela_disponibilidade, tabela_mtbf], ignore_index=True)
    tabela_maquinas_ = tabela_maquinas()
    
    if maquinas_importante:
    
        tabela_maquinas_ = tabela_maquinas_[tabela_maquinas_['codigo'].isin(list(maquinas_importantes()))] # filtro maquinas importantes

    if setores_selecionados:
        # setores_selecionados = json.loads(data_get["setores_selecionados"])
        
        setores_selecionados = [setor.strip() for setor in setores_selecionados]
        tabela_maquinas_ = tabela_maquinas_[tabela_maquinas_['setor'].isin(setores_selecionados)] # filtro setor
    
    tabela_final_disponibilidade = tabela_maquinas_.merge(tabela_concatenada, how='left', left_on='codigo', right_on='maquina')

    tabela_final_disponibilidade['tempo_total_parada'] = tabela_final_disponibilidade['tempo_total_parada'].fillna(0) # apenas para deixar a disponibilidade 100%
    tabela_final_disponibilidade['tempo_planejado'] = tabela_final_disponibilidade['tempo_planejado'].fillna(100) # apenas para deixar a disponibilidade 100%
     
    tabela_final_disponibilidade['disponibilidade'] = round((tabela_final_disponibilidade['tempo_planejado'] - tabela_final_disponibilidade['tempo_parada']) / tabela_final_disponibilidade['tempo_planejado'],2)
    
    if not data_filtro:

        df_historico_disponibilidade = pd.read_csv("disponibilidade_historico.csv", sep=";")

        if setores_selecionados:
            setores_selecionados = json.loads(data["setores_selecionados"])

            setores_selecionados = [setor.strip() for setor in setores_selecionados]
            df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['setor'].isin(setores_selecionados)]
                
        if maquinas_importante:
            df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['setor'].isin(maquinas_importantes())]

        df_historico_disponibilidade['disponibilidade_historico_media'] = df_historico_disponibilidade['disponibilidade_historico_media'].apply(lambda x: float(x.replace(",",".").replace("%",""))/100)
        df_historico_disponibilidade.rename(columns={"disponibilidade_historico_media": "disponibilidade"}, inplace=True)
        df_historico_disponibilidade = df_historico_disponibilidade[['setor','disponibilidade']]
        
        tabela_final_disponibilidade = pd.concat([tabela_final_disponibilidade,df_historico_disponibilidade],ignore_index=True)

    resultado_agrupado = tabela_final_disponibilidade.groupby('setor').agg({
    'disponibilidade': 'mean'
    }).reset_index()
    
    resultado_agrupado = resultado_agrupado[['setor','disponibilidade']]

    lista_resultado = resultado_agrupado.to_dict(orient='records')

    # lista_resultado = sorted(lista_resultado, key=lambda x: x['disponibilidade'])

    return jsonify({'resultados': lista_resultado})

@routes_bp.route('/api/disponibilidades', methods=['POST', 'GET'])
def disponibilidades():
    
    data_get = request.get_json()
    
    data_filtro = data_get.get('data_filtro')
    setores_selecionados = data_get.get('setores_selecionados', [])
    maquinas_importante = data_get.get('maquinasFavoritas', [])

    if data_filtro == '':
        data_filtro = None
    if len(setores_selecionados) == 0:
        setores_selecionados = None

    if data_filtro:
    
        # Divida a string com base no caractere "-"
        datas = data_filtro.split(" - ")

        dia_inicial = datetime.strptime(datas[0], "%d/%m/%Y").strftime("%Y-%m-%d")
        dia_final = datetime.strptime(datas[1], "%d/%m/%Y").strftime("%Y-%m-%d")

        disp_maquinas, disp_setor = disponibilidade_final(dia_inicial,dia_final,setores_selecionados,maquinas_importante)
        
        return jsonify({'disp_setor': disp_setor, 'disp_maquinas':disp_maquinas})
    
    else:

        disp_maquinas, disp_setor = disponibilidade_historica(setores_selecionados,maquinas_importante)

        return jsonify({'disp_setor': disp_setor, 'disp_maquinas':disp_maquinas})

def disponibilidade_historica(setor=None,maquinas_importante=None):

    df_historico_disponibilidade = pd.read_csv("disponibilidade_historico.csv", sep=";")
    df_historico_disponibilidade['maquina'] = df_historico_disponibilidade['maquina'].apply(lambda x: x.split(" - ")[0])

    if setor:
        df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['setor'].isin(setor)]
            
    if maquinas_importante:
        df_historico_disponibilidade = df_historico_disponibilidade[df_historico_disponibilidade['maquina'].isin(maquinas_importantes())]

    df_historico_disponibilidade['disponibilidade_historico_media'] = df_historico_disponibilidade['disponibilidade_historico_media'].apply(lambda x: float(x.replace(",",".").replace("%",""))/100)
    df_historico_disponibilidade.rename(columns={"disponibilidade_historico_media": "disponibilidade_horas", "maquina": "codigo"}, inplace=True)
    df_historico_disponibilidade_maquina = df_historico_disponibilidade[['codigo','disponibilidade_horas']].groupby('codigo').mean().reset_index()
    df_historico_disponibilidade_setor = df_historico_disponibilidade[['setor','disponibilidade_horas']].groupby('setor').mean().reset_index()

    disp_maquinas_dict = df_historico_disponibilidade_maquina.to_dict(orient='records')
    disp_setor_dict = df_historico_disponibilidade_setor.to_dict(orient='records')

    return disp_maquinas_dict,disp_setor_dict

def formulario_os(id_ordem):

    """
    Função para gerar arquivo excel com informações sobre a OS.
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT n_ordem,descmanutencao,operador,datainicio,horainicio,datafim,horafim,
    id_ordem,setor,solicitante,maquina,problemaaparente,maquina_parada,status,ultima_atualizacao
                    FROM tb_ordens 
                WHERE id_ordem = {}
                ORDER BY n_ordem asc
                    """.format(id_ordem)

    cur.execute(query)
    lista_solicitacoes = cur.fetchall()
    df = pd.read_sql_query(query, conn)

    cur.execute("INSERT INTO tb_confirmacao (id_ordem, n_ordem, confirmacao) VALUES (%s, %s, %s)", (int(df['id_ordem'][len(df) - 1]),int(df['n_ordem'][len(df) - 1]),True))

    ultima_atualizacao = df['ultima_atualizacao'][len(df) - 1] - timedelta(hours=3)

    wb = load_workbook('modelo_os_new_v2.xlsx')
    ws = wb.active

    nova_hora_formatada = ultima_atualizacao.strftime('%H:%M')
    data_atual = ultima_atualizacao.strftime('%d/%m/%Y')

    ws['G8'] = data_atual
    ws['G9'] = nova_hora_formatada

    ws['B8'] = df['id_ordem'][len(df) - 1]
    ws['B9'] = df['setor'][len(df) - 1]
    ws['B10'] = df['solicitante'][0]
    ws['B11'] = df['maquina'][len(df) - 1]
    ws['B12'] = df['problemaaparente'][len(df) - 1]

    # Aumentar a quantidade de Linhas
    for i in range(len(df)):
        # Define a linha de destino
        linha_destino = 24 + i
        ws.insert_rows(25 + i)

        # Copia o valor e o estilo da linha 27 para a linha de destino
        for coluna in range(1, 9):  # A coluna 1 é a A, a coluna 2 é a B, etc.
            ws.cell(row=linha_destino, column=coluna).font = copy.copy(ws.cell(row=24, column=coluna).font)
            ws.cell(row=linha_destino, column=coluna).fill = copy.copy(ws.cell(row=24, column=coluna).fill)
            ws.cell(row=linha_destino, column=coluna).border = copy.copy(ws.cell(row=24, column=coluna).border)
            ws.cell(row=linha_destino, column=coluna).alignment = copy.copy(ws.cell(row=24, column=coluna).alignment)
                    # Access and copy the line height from row 27
            line_height = ws.row_dimensions[24].height  # Access height from source row
            ws.row_dimensions[linha_destino].height = line_height  # Set the same height for the target row

    for i in range(1,len(df)):
        linha_destino = 23 + i

        ws.cell(row=linha_destino, column=1).value = lista_solicitacoes[i][0]
        ws.cell(row=linha_destino, column=2).value = lista_solicitacoes[i][1] 
        ws.cell(row=linha_destino, column=4).value = lista_solicitacoes[i][2] 
        data_inicio = lista_solicitacoes[i][3].strftime("%d/%m/%Y")
        ws.cell(row=linha_destino, column=5).value = data_inicio  # Motivo
        ws.cell(row=linha_destino, column=6).value = lista_solicitacoes[i][4]
        data_fim = lista_solicitacoes[i][5].strftime("%d/%m/%Y")  # Motivo
        ws.cell(row=linha_destino, column=7).value = data_fim  # Motivo
        ws.cell(row=linha_destino, column=8).value = lista_solicitacoes[i][6]  # Motivo

    if df['maquina_parada'][len(df) - 1] == True:
        ws['G11'] = 'Sim'
    else:
        ws['G11'] = 'Não'

    df = df.drop_duplicates(subset=['id_ordem'], keep='last').reset_index()

    ws['G10'] = df['status'][0]

    wb.save('Relatorio_OS.xlsx')

    wb.close()

    conn.commit()

    # Retorna o arquivo para download
    return send_file("Relatorio_OS.xlsx", as_attachment=True)


def mes_atual():

    """
    Função para mostrar mês atual
    """

    mesAtual = datetime.now().month

    return mesAtual


def calcular_dias_uteis(ano, mes):

    """
    Função para calcular dias úteis
    """

    dias_uteis = []

    start_date = pd.Timestamp(year=ano, month=mes, day=1)
    end_date = pd.Timestamp(year=ano, month=mes, day=1) + \
        pd.DateOffset(months=1) - pd.DateOffset(days=1)

    for day in pd.date_range(start_date, end_date):
        if day.weekday() < 5:  # 0 a 4 representam os dias da semana de segunda a sexta-feira
            dias_uteis.append(day)

    dias_uteis = len(dias_uteis)

    return dias_uteis


def custo_MO():

    """
    Cálculo de custo da mão obra por ordem de serviço
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = ("""
        SELECT
            id_ordem,
            dataabertura,
            n_ordem,
            status,
            datainicio,
            datafim,
            STRING_AGG(REGEXP_REPLACE(operador, '[^\d,]', '', 'g'), ', ') AS operador,
            MIN(TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS')) AS inicio,
            MAX(TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS')) AS fim
        FROM (
            SELECT
                id_ordem,
                dataabertura,
                n_ordem,
                status,
                datainicio,
                datafim,
                operador,
                descmanutencao,
                horainicio,
                horafim
            FROM tb_ordens
            WHERE (ordem_excluida IS NULL OR ordem_excluida = FALSE)
        ) subquery
        GROUP BY id_ordem, dataabertura, n_ordem, status, datainicio, datafim, descmanutencao;
        """)

    df_timeline = pd.read_sql_query(s, conn)
    df_funcionario = pd.read_sql_query("SELECT * FROM tb_funcionario", conn) 

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)

    for i in range(len(df_timeline)):
        if df_timeline['fim'][i] == 'NaT':
            df_timeline['fim'][i] = 0
            df_timeline['inicio'][i] = 0
        else:
            pass

    df_timeline = df_timeline.replace(np.nan, '-')

    df_timeline['operador'] = df_timeline['operador'].apply(lambda x: ', '.join(re.findall(r'\d+', x)))
    
    df_operadores = df_timeline['operador'].str.split(', ', expand=True).stack().reset_index(level=1, drop=True).reset_index()
    df_operadores.columns = ['index', 'operador']

    # Mesclar os DataFrames usando o índice original
    df_resultado = df_timeline.drop(columns=['operador']).merge(df_operadores, left_index=True, right_on='index', how='inner')
    df_timeline = df_resultado.drop(columns=['index'])
    
    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        # df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(
            lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0

    df_timeline = df_timeline.sort_values(by='n_ordem', ascending=True)

    if df_timeline['datainicio'][0] == '-':
        df_timeline['datainicio'][0] = df_timeline['dataabertura'][0]

    df_final = df_timeline

    df_final['operador'] = df_final['operador'].replace('',0).astype(int)
    df_funcionario['matricula'] = df_funcionario['matricula'].astype(int)

    df_timeline = df_final.merge(df_funcionario, left_on='operador', right_on='matricula')

    df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

    df_timeline['mesExecucao'] = df_timeline['fim'].dt.month
    df_timeline['anoExecucao'] = df_timeline['fim'].dt.year
    df_timeline['dias_uteis'] = df_timeline.apply(
        lambda row: calcular_dias_uteis(row['anoExecucao'], row['mesExecucao']), axis=1)
    df_timeline['horasTotalMes'] = df_timeline['dias_uteis'] * (9*60)
    df_timeline['salario'] = df_timeline['salario'].replace("-", 0)
    df_timeline['salario'] = df_timeline['salario'].astype(float)
    df_timeline['proporcional'] = (
        df_timeline['salario'] * df_timeline['diferenca']) / df_timeline['horasTotalMes']

    df_groupby = df_timeline[['id_ordem', 'proporcional']].groupby(
        ['id_ordem']).sum().reset_index().round(2)

    df_timeline = df_timeline.drop(columns=[
                                   'mesExecucao', 'anoExecucao', 'dias_uteis', 'horasTotalMes', 'proporcional', 'nome', 'matricula', 'salario'])
    df_timeline = df_timeline.drop_duplicates(subset=['id_ordem'])

    df_final = pd.merge(df_timeline, df_groupby, how='left', on='id_ordem')

    df_final['diferenca'] = df_final['diferenca'].astype(int)

    df_final = df_final[['id_ordem', 'proporcional']]

    return df_final


def tempo_maquina_parada():
    
    """
    Função para calcular tempo de máquina parada
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = ("""
        SELECT
            id_ordem,
            dataabertura,
            n_ordem,
            status,
            datainicio,
            datafim,
            STRING_AGG(REGEXP_REPLACE(operador, '[^\d,]', '', 'g'), ', ') AS operador,
            MIN(TO_TIMESTAMP(datainicio || ' ' || horainicio, 'YYYY-MM-DD HH24:MI:SS')) AS inicio,
            MAX(TO_TIMESTAMP(datafim || ' ' || horafim, 'YYYY-MM-DD HH24:MI:SS')) AS fim
        FROM (
            SELECT
                id_ordem,
                dataabertura,
                n_ordem,
                status,
                datainicio,
                datafim,
                operador,
                descmanutencao,
                horainicio,
                horafim
            FROM tb_ordens
            WHERE (ordem_excluida IS NULL OR ordem_excluida = FALSE)
        ) subquery
        GROUP BY id_ordem, dataabertura, n_ordem, status, datainicio, datafim, descmanutencao;
        """)

    df_timeline = pd.read_sql_query(s, conn)

    df_timeline['inicio'] = df_timeline['inicio'].astype(str)
    df_timeline['fim'] = df_timeline['fim'].astype(str)

    for i in range(len(df_timeline)):
        if df_timeline['fim'][i] == 'NaT':
            df_timeline['fim'][i] = 0
            df_timeline['inicio'][i] = 0
        else:
            pass

    df_timeline = df_timeline.replace(np.nan, '-')
        
    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        # df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(
            lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

    except:
        df_timeline['diferenca'] = 0

    df_timeline = df_timeline.sort_values(by='n_ordem', ascending=True)

    if df_timeline['datainicio'][0] == '-':
        df_timeline['datainicio'][0] = df_timeline['dataabertura'][0]

    df_final = df_timeline

    df_final['operador'] = df_final['operador'].replace('',0).astype(int)


    df_final = df_final[['id_ordem', 'proporcional']]

    return df_final


def allowed_file(filename):

    """
    Função para verificar a extensão do arquivo permitida
    """

    # Lista de extensões permitidas para vídeos
    ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@routes_bp.route('/')
@login_required
def inicio():  # Redirecionar para a página de login

    """
    Rota para página de login
    """

    return render_template("login/login.html")


@routes_bp.route('/index')
@login_required
def Index():  # Página inicial (Página com a lista de ordens de serviço)

    """
    Rota para página principal da aplicação, mostrando a tabela principal.
    """

    setor_selecionado = session.get('setor')
    identificador_selecionado = session.get('identificador')

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = (""" SELECT DISTINCT t10.*, tc.confirmacao,tc.data_atual FROM (
                SELECT * FROM (
                select DISTINCT t7.*, t8.id_ordem as contem_imagem
                    FROM (
                        select t5.*, t6.id_ordem as contem_video
                        FROM(
                            select t3.*, t4.parada1,t4.parada2,t4.parada3
                            FROM(
                                SELECT DISTINCT t1.total, t2.* 
                                FROM (
                                    SELECT tb_carrinho.id_ordem, SUM(tb_material.valor * tb_carrinho.quantidade) AS total
                                    FROM tb_carrinho
                                    JOIN tb_material ON tb_carrinho.codigo = tb_material.codigo
                                    GROUP BY tb_carrinho.id_ordem
                                    ) t1
                                RIGHT JOIN tb_ordens t2 ON t1.id_ordem = t2.id_ordem
                            ) as t3
                            LEFT JOIN tb_paradas t4 ON t3.id_ordem = t4.id_ordem
                            ORDER BY t3.id_ordem
                        ) as t5
                        LEFT JOIN tb_videos_ordem_servico t6 on t5.id_ordem = t6.id_ordem
                        ) as t7
                    LEFT JOIN tb_imagens t8 on t7.id_ordem = t8.id_ordem)  as t9
                    LEFT JOIN tb_planejamento_anual AS tpa ON t9.maquina LIKE '%' || tpa.codigo || '%') AS t10
                    LEFT JOIN tb_confirmacao as tc ON t10.id_ordem = tc.id_ordem AND t10.n_ordem = tc.n_ordem
         """)

    df = pd.read_sql_query(s, conn)
    df = df.sort_values(by='id_ordem').reset_index(drop=True)

    df = df[df['ordem_excluida'] != True].reset_index(drop=True)

    df.fillna('', inplace=True)

    for i in range(len(df)-1, 0, -1):
        if df['id_ordem'][i] == df['id_ordem'][i-1]:
            if df['maquina_parada'][i-1] == '':
                df['maquina_parada'][i-1] = df['maquina_parada'][i]

    for i in range(1, len(df)):
        if df['id_ordem'][i-1] == df['id_ordem'][i]:
            df['maquina_parada'][i] = df['maquina_parada'][i-1]

    df = df.sort_values(by='n_ordem')

    df.reset_index(drop=True, inplace=True)
    df.replace(np.nan, '', inplace=True)

    df['dataabertura'] = df['dataabertura'].fillna(method='ffill')
    df['dataabertura'] = df['dataabertura'].replace('', method='ffill')

    df = df.drop_duplicates(subset=['id_ordem'], keep='last')
    df = df.sort_values(by='id_ordem')
    df.reset_index(drop=True, inplace=True)

    for i in range(len(df)):
        if df['total'][i] == '':
            df['total'][i] = 0

    # df['total'] = df['total'].apply(lambda x: round(x, 2))

    df = df.sort_values('ultima_atualizacao', ascending=False)

    df['ultima_atualizacao'] = pd.to_datetime(df['ultima_atualizacao'])
    df['ultima_atualizacao'] = df['ultima_atualizacao'] - timedelta(hours=3)
    df['ultima_atualizacao'] = df['ultima_atualizacao'].dt.strftime(
        "%Y-%m-%d %H:%M:%S")
    
    # .dt.strftime("%d/%m/%Y")

    df.reset_index(drop=True, inplace=True)

    for i in range(len(df)):
        try:
            if df['dataabertura'][i].strftime('%H:%M') == '03:00':
                df['dataabertura'][i] = df['ultima_atualizacao'][i]
        except:
            pass
        
    for i in range(len(df)):
        if df['maquina_parada'][i] == '':
            df['maquina_parada'][i] = False

    for i in range(len(df)):
        if df['status'][i] == 'Finalizada' or df['parada1'][i] == 'false':
            df['maquina_parada'][i] = False

    df_custos = custo_MO()

    df = pd.merge(df, df_custos, how='left', on='id_ordem')

    df['proporcional'] = df['proporcional'].fillna(0)
    list_users = df.values.tolist()

    funcionarios = buscar_funcionarios()

    nomes_solicitantes = solicitantes()

    return render_template('user/index.html', funcionarios=funcionarios, nomes_solicitantes=nomes_solicitantes,list_users=list_users,setor_selecionado=setor_selecionado,identificador_selecionado=identificador_selecionado)

def proxima_os():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT MAX(id_ordem) FROM tb_ordens")
    ultima_os = cur.fetchone()[0]
    proxima_os = ultima_os+1

    return proxima_os

def salvar_imagem(imagens, os):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if not any(imagens):
        return 'sem video'
    else:
        for imagem in imagens:

            # Ler os dados da imagem
            imagem_data = imagem.read()

            # Abrir a imagem usando a biblioteca Pillow
            image = Image.open(io.BytesIO(imagem_data))

            # Converter a imagem para o modo RGB, se necessário
            if image.mode != 'RGB':
                image = image.convert('RGB')

            # Redimensionar a imagem para um tamanho desejado
            max_size = (800, 600)
            image.thumbnail(max_size)

            # Salvar a imagem com uma qualidade reduzida
            buffer = io.BytesIO()
            image.save(buffer, format='JPEG', quality=80)
            imagem_data_comprimida = buffer.getvalue()

            cur = conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("INSERT INTO tb_imagens (id_ordem, imagem) VALUES (%s,%s)",
                        (os, imagem_data_comprimida))
            conn.commit()
    

    return 'sucesso'

def salvar_video(videos, os):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    for video in videos:
        if video.filename != '':
            # Verificar a extensão do arquivo (opcional)
            if allowed_file(video.filename):
                filename = secure_filename(video.filename)
                # video.save(os.path.join(routes_bp.config['UPLOAD_FOLDER'], filename))

                # Ler os dados do vídeo como um objeto bytes
                video_data = video.read()

                # Inserir os dados do vídeo no banco de dados
                cur.execute(
                    "INSERT INTO tb_videos_ordem_servico (id_ordem, video) VALUES (%s, %s)", (os, video_data))
                conn.commit()

    return 'sucess'

@routes_bp.route('/abrir-os', methods=['POST'])
def abrir_os():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                    password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    setor = request.form.get('inputSetor')
    maquina = request.form.get('inputMaquinaLocal')
    problema = request.form.get('problemaAparente')
    solicitante = request.form.get('inputSolicitante')
    dataAbertura = datetime.now()
    equipamento_em_falha = request.form.get('inputEquipamentoFalha')
    setor_maquina_solda = request.form.get('inputcampoESetorMaquinaSolda')
    qual_ferramenta = request.form.get('inputFerramenta')
    cod_equipamento = request.form.get('inputCodigoFerramenta')
    risco = request.form['inputImpacto']
    n_ordem = 0
    status = 'Em espera'

    if 'checkboxMaquinaParada' in request.form:
        # O checkbox foi marcado
        maquina_parada = True
    else:
        # O checkbox não foi marcado
        maquina_parada = False

    # Receber e salvar imagem
    imagens = request.files.getlist('imagens')
    salvar_imagem(imagens, proxima_os())

    # Receber e salvar vídeo
    videos = request.files.getlist('video')
    salvar_video(videos, proxima_os())
    
    cur.execute("INSERT INTO tb_ordens (setor, maquina, risco,status, problemaaparente, id_ordem, n_ordem ,dataabertura, maquina_parada,solicitante,equipamento_em_falha,setor_maquina_solda,qual_ferramenta, cod_equipamento) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (setor, maquina, risco, status, problema, proxima_os(), n_ordem, dataAbertura, maquina_parada, solicitante, equipamento_em_falha, setor_maquina_solda, qual_ferramenta, cod_equipamento))
    
    conn.commit()

    return redirect(url_for('routes.open_os'))


@routes_bp.route('/edit/<id_ordem>/<identificador_selecionado>/<setor_selecionado>', methods=['POST', 'GET'])
@login_required
# Página para edição da ordem de serviço (Informar o andamento da ordem)
def get_employee(id_ordem, identificador_selecionado, setor_selecionado):

    """
    Função para criar uma execução para ordem de serviço
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ('SELECT tb_ordens.*, tb_maquinas.tombamento FROM tb_ordens LEFT JOIN tb_maquinas ON tb_ordens.maquina = tb_maquinas.codigo WHERE tb_ordens.id_ordem = {};'.format(int(id_ordem)))
    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)

    data1 = data1.sort_values(by='n_ordem')
    data1.reset_index(drop=True, inplace=True)
    data1.replace(np.nan, '', inplace=True)

    data1.iloc[:,8:]

    try:
        dataabertura = data1['dataabertura'][0] - timedelta(hours=3)
        dataabertura = dataabertura.tz_convert(None).strftime('%Y-%m-%d %H:%M')
    except:
        dataabertura = data1['ultima_atualizacao'][0] - timedelta(hours=3)
        dataabertura = dataabertura.tz_convert(None).strftime('%Y-%m-%d %H:%M')

    # Loop para percorrer todas as linhas da coluna
    for i in range(1, len(data1['dataabertura'])):
        if data1['dataabertura'][i] == '':
            data1['dataabertura'][i] = data1['dataabertura'][i-1]
        if data1['solicitante'][i] == '':
            data1['solicitante'][i] = data1['solicitante'][i-1]
        if data1['equipamento_em_falha'][i] == '':
            data1['equipamento_em_falha'][i] = data1['equipamento_em_falha'][i-1]
        if data1['setor_maquina_solda'][i] == '':
            data1['setor_maquina_solda'][i] = data1['setor_maquina_solda'][i-1]
        if data1['qual_ferramenta'][i] == '':
            data1['qual_ferramenta'][i] = data1['qual_ferramenta'][i-1]
        if data1['cod_equipamento'][i] == '':
            data1['cod_equipamento'][i] = data1['cod_equipamento'][i-1]
        if data1['pvlye'][i] == '':
            data1['pvlye'][i] = data1['pvlye'][i-1]
        if data1['pa_plus'][i] == '':
            data1['pa_plus'][i] = data1['pa_plus'][i-1]
        if data1['tratamento'][i] == '':
            data1['tratamento'][i] = data1['tratamento'][i-1]
        if data1['ph_agua'][i] == '':
            data1['ph_agua'][i] = data1['ph_agua'][i-1]

    data1 = data1.drop_duplicates(subset=['id_ordem'], keep='last')
    data1 = data1.sort_values(by='id_ordem')

    tipo_manutencao = data1['tipo_manutencao'].values.tolist()[0]
    area_manutencao = data1['area_manutencao'].values.tolist()[0]

    pvlye = data1['pvlye'].values.tolist()[0]
    pa_plus = data1['pa_plus'].values.tolist()[0]
    tratamento = data1['tratamento'].values.tolist()[0]
    ph_agua = data1['ph_agua'].values.tolist()[0]

    data1 = data1.values.tolist()
 
    opcaoAtual = data1[0][4]

    lista_opcoes = ['Em execução', 'Finalizada', 'Aguardando material']

    opcoes = []
    opcoes.append(opcaoAtual)

    for opcao in lista_opcoes:
        opcoes.append(opcao)

    opcoes = list(set(opcoes))
    opcoes.remove(opcaoAtual)  # Remove o elemento 'c' da lista
    opcoes.insert(0, opcaoAtual)

    query = """SELECT * FROM tb_funcionario"""
    tb_funcionarios = pd.read_sql_query(query, conn)
    tb_funcionarios['matricula_nome'] = tb_funcionarios['matricula'] + \
        " - " + tb_funcionarios['nome']
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

    query_maquinas_preventivas = f""" SELECT DISTINCT codigo
                                        FROM tb_ordens AS t1
                                        JOIN tb_maquinas_preventivas AS t2 ON t1.maquina = t2.codigo
                                        WHERE t1.id_ordem = {int(id_ordem)} """
    
    cur.execute(query_maquinas_preventivas)

    maquinas_preventivas = cur.fetchall()

    preventiva = False

    if len(maquinas_preventivas) > 0:

        preventiva = True

    return render_template('user/edit.html', dataabertura=dataabertura, ordem=data1[0], tb_funcionarios=tb_funcionarios,
                           opcoes=opcoes, tipo_manutencao=tipo_manutencao,
                           area_manutencao=area_manutencao, maquinas=maquinas, pvlye=pvlye, pa_plus=pa_plus,
                           tratamento=tratamento, ph_agua=ph_agua,identificador_selecionado=identificador_selecionado,
                           setor_selecionado=setor_selecionado,preventiva=preventiva)

@routes_bp.route('/dados-ordem-servico', methods=['POST'])
@login_required
def dados_ordem_servico():
    data = request.get_json()
    
    data = buscar_dados_os(data['id_ordem'])
    print(data)
    
    return jsonify(data)

@routes_bp.route('/dados-editar-ordem', methods=['POST'])
@login_required
def dados_editar_ordem():
    data = request.get_json()
    data = dados_para_editar(data['id_ordem'], data['n_ordem'])

    print(data)

    return jsonify(data)

def verificar_maquina_preventiva(maquina):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = """select codigo from tb_maquinas_preventivas where codigo = %s"""
    cur.execute(s,(maquina,))

    data = cur.fetchall()

    if len(data) > 0:
        return True
    else:
        return False
    
@routes_bp.route('/tombamento', methods=['POST'])
@login_required
def obter_tombamento():
    data = request.get_json()
    codigo_maquina = data['codigo']

    if ' - ' in codigo_maquina:
        codigo_maquina = codigo_maquina.split(' - ')
        codigo_maquina = codigo_maquina[0]

    print(codigo_maquina)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = f"""SELECT tombamento
            FROM tb_maquinas
            WHERE codigo = '{codigo_maquina}'"""

    cur.execute(query)
    tombamento = cur.fetchall()

    print(tombamento)

    return jsonify({'tombamento': tombamento})

@routes_bp.route('/guardar-ordem-editada', methods=['POST'])
@login_required
def editar_ordem_banco():
    
    dados = request.get_json()
    print(dados)
    
    setor = dados['setor_edicao']
    solicitante = dados['inputSolicitante_edicao']
    maquina = dados['maquina_edicao']
    tombamento = dados['inputTombamento_edicao']
    risco = dados['inputRisco_edicao']
    status = dados['statusLista_edicao']
    # problema = dados['problema']
    id_ordem = int(dados['numeroOs'])
    n_execucao = int(dados['n_ordem_edicao'])
    descmanutencao = dados['descmanutencao_edicao']
    operador = dados['operador_edicao']
    inputEquipamentoEmFalha_edicao = dados['inputEquipamentoEmFalha_edicao']
    codigoEquipamento_edicao = dados['codigoEquipamento_edicao']
    setorMaqSolda_edicao = dados['setorMaqSolda_edicao']
    qual_ferramenta_edicao = dados['qual_ferramenta_edicao']

    matriculas_operadores = []

    # Itere sobre a lista
    for item in operador:
        # Divida a string usando '-' como separador e pegue a parte antes do hífen
        partes = item.split('-')
        if len(partes) > 0:
            matriculas = partes[0].strip()
            matriculas_operadores.append(matriculas)

    operador = matriculas_operadores

    operador = ",".join(operador)

    tipo_manutencao = dados['selectTipoManutencao_edicao']
    datetimes = dados['data_edit_edicao']
    area_manutencao = dados['areaManutencao_edicao']
    pvlye = dados['pvlye_edicao']
    pa_plus = dados['pa-plus_edicao']
    tratamento = dados['tratamento_edicao']
    ph_agua = dados['ph-agua_edicao']

    dataAbertura,natureza = buscar_data_abertura_natureza(id_ordem)

    datainicio,horainicio,datafim,horafim = formatar_data_hora(datetimes)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if n_execucao == 0:
        cur.execute("""update tb_ordens
        set maquina=%s,solicitante=%s,setor=%s, risco=%s, tipo_manutencao=%s, area_manutencao=%s, 
        equipamento_em_falha=%s,setor_maquina_solda=%s,qual_ferramenta=%s,
        cod_equipamento=%s,pvlye=%s, pa_plus=%s, tratamento=%s, ph_agua=%s
        where id_ordem = %s""", (maquina,solicitante,setor,risco,tipo_manutencao,area_manutencao,inputEquipamentoEmFalha_edicao,
                                setorMaqSolda_edicao,qual_ferramenta_edicao,codigoEquipamento_edicao,pvlye,
                                pa_plus,tratamento,ph_agua,id_ordem))
    else:
        cur.execute("""update tb_ordens
        set status=%s, datainicio=%s, horainicio=%s,
            datafim=%s, horafim=%s, descmanutencao = %s,
            operador=%s,equipamento_em_falha=%s,setor_maquina_solda=%s,qual_ferramenta=%s,
            cod_equipamento=%s,pvlye=%s, pa_plus=%s, tratamento=%s, ph_agua=%s
            where id_ordem = %s and n_ordem = %s""", (status,datainicio,horainicio,
                                                    datafim,horafim,descmanutencao,operador,inputEquipamentoEmFalha_edicao,
                                                    setorMaqSolda_edicao,qual_ferramenta_edicao,codigoEquipamento_edicao,
                                                    pvlye,pa_plus,tratamento,ph_agua,id_ordem,n_execucao))

    conn.commit()
    cur.close()

    return 'sucess'

@routes_bp.route('/envio_ok', methods=['POST'])
@login_required
def envio_ok():  # Inserir as edições no banco de dados

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    json_confirmacao = request.get_json()
    
    numero_execucao = int(json_confirmacao['numero_execucao'])

    numeroOrdemValue = int(json_confirmacao['numeroOrdemValue'])

    status = 'Finalizada'

    confirmacao = True

    print(status,confirmacao,numero_execucao,numeroOrdemValue)

    query = "SELECT n_execucao FROM tb_confirmacao;"

    cur.execute(query)

    maquinas_preventivas = cur.fetchall()

    for maquina in maquinas_preventivas:
        print(maquina[0], numeroOrdemValue)
        if maquina[0] == numeroOrdemValue:
            print("Entrou")
            return "Número da Ordem já enviado"

    cur.execute("INSERT INTO tb_confirmacao (n_ordem,n_execucao,confirmacao) VALUES (%s,%s,%s)",(numero_execucao,numeroOrdemValue,confirmacao))

    cur.execute("""
        UPDATE tb_ordens
        SET status=%s, confirmacao=%s
        WHERE n_ordem = %s and id_ordem = %s
        """, (status, confirmacao , numero_execucao - 1, numeroOrdemValue))

    conn.commit()

    return "Sucesso"

@routes_bp.route('/update/<id_ordem>/<identificador_selecionado>/<setor_selecionado>', methods=['POST'])
@login_required
def update_student(id_ordem,identificador_selecionado,setor_selecionado):  # Inserir as edições no banco de dados

    """
    Rota para editar ordem de serviço
    """

    # # Execute a instrução SQL para alterar o tipo da coluna
    # alter_query = "ALTER TABLE tb_ordens ALTER COLUMN tipo_manutencao TYPE TEXT;"
    # cur.execute(alter_query)
    # conn.commit()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    if request.method == 'POST':

        # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # cur.execute(""" 
        #     SELECT MAX(id) FROM tb_ordens
        # """)

        # ultimo_id = cur.fetchone()[0]

        # try:
        #     ultimo_id = ultimo_id+1
        # except:
        #     ultimo_id = 0

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
        confirmacao = False
        # operador = json.dumps(operador)

        matriculas_operadores = []

        # Itere sobre a lista
        for item in operador:
            # Divida a string usando '-' como separador e pegue a parte antes do hífen
            partes = item.split('-')
            if len(partes) > 0:
                matriculas = partes[0].strip()
                matriculas_operadores.append(matriculas)

        operador = matriculas_operadores

        operador = ",".join(operador)

        print(operador)

        tipo_manutencao = request.form['tipo_manutencao']
        datetimes = request.form['datetimes']
        area_manutencao = request.form['area_manutencao']
        pvlye = request.form.get('pvlye')
        pa_plus = request.form.get('pa-plus')
        tratamento = request.form.get('tratamento')
        ph_agua = request.form.get('ph-agua')

        natureza = natureza

        query_maquinas_preventivas = f""" SELECT DISTINCT codigo
                                        FROM tb_ordens AS t1
                                        JOIN tb_maquinas_preventivas AS t2 ON t1.maquina = t2.codigo
                                        WHERE t1.id_ordem = {int(id_ordem)} """
    
        cur.execute(query_maquinas_preventivas)

        maquinas_preventivas = cur.fetchall()

        if len(maquinas_preventivas) > 0 and status == 'Finalizada':
            status = 'Aguardando OK'
            confirmacao = True
            print("Entrou")
        else:
            print("Entrou no Elsee ")

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

        if status == 'Finalizada' or status == 'Aguardando OK':
            botao3 = 'true'
            

        print(botao1)
        print(botao2)
        print(botao3)

        # Divida a string em duas partes: data/hora inicial e data/hora final
        data_hora_inicial_str, data_hora_final_str = datetimes.split(" - ")

        # Faça o parsing das strings de data e hora
        data_inicial = datetime.strptime(
            data_hora_inicial_str, "%d/%m/%y %I:%M %p")
        data_final = datetime.strptime(
            data_hora_final_str, "%d/%m/%y %I:%M %p")

        # Formate as datas e horas no formato desejado
        datainicio = data_inicial.strftime("%Y-%m-%d")
        horainicio = data_inicial.strftime("%H:%M:%S")
        datafim = data_final.strftime("%Y-%m-%d")
        horafim = data_final.strftime("%H:%M:%S")

        # print(datainicio, horainicio, datafim, horafim)

        # print(ultimo_id, setor, maquina, risco, status, problema, datainicio, horainicio, datafim, horafim, id_ordem, n_ordem, descmanutencao, [operador])

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            INSERT INTO tb_ordens ( setor,maquina,risco,status,problemaaparente,
                                    datainicio,horainicio,datafim,horafim,id_ordem,n_ordem,
                                    descmanutencao, operador, natureza, tipo_manutencao,
                                    area_manutencao,pvlye,pa_plus,tratamento,ph_agua,confirmacao) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (setor, maquina, risco, status, problema, datainicio, horainicio,
              datafim, horafim, id_ordem, n_ordem, descmanutencao,
              operador, natureza, tipo_manutencao, area_manutencao,
              pvlye, pa_plus, tratamento, ph_agua, confirmacao))

        cur.execute("""
            INSERT INTO tb_paradas (id_ordem,n_ordem, parada1, parada2, parada3) 
                    VALUES (%s,%s,%s,%s,%s)
        """, (id_ordem, n_ordem, botao1, botao2, botao3))

        flash('OS de número {} atualizada com sucesso!'.format(int(id_ordem)))
        conn.commit()

        return redirect(url_for('routes.get_employee', id_ordem=id_ordem,identificador_selecionado=identificador_selecionado,setor_selecionado=setor_selecionado))

def buscar_data_abertura_natureza(ordem_id):
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    query_buscar_data_abertura = "select dataabertura,natureza from tb_ordens where id_ordem = %s and n_ordem = 0"
    
    cur.execute(query_buscar_data_abertura,(ordem_id,))
    data = cur.fetchall()

    dataAbertura = [row['dataabertura'] for row in data][0]
    natureza = [row['natureza'] for row in data][0]

    return dataAbertura,natureza

def formatar_data_hora(datetimes):

    # Divida a string em duas partes: data/hora inicial e data/hora final
    data_hora_inicial_str, data_hora_final_str = datetimes.split(" - ")

    # Faça o parsing das strings de data e hora
    data_inicial = datetime.strptime(
        data_hora_inicial_str, "%d/%m/%Y %H:%M")
    data_final = datetime.strptime(
        data_hora_final_str, "%d/%m/%Y %H:%M")

    # Formate as datas e horas no formato desejado
    datainicio = data_inicial.strftime("%Y-%m-%d")
    horainicio = data_inicial.strftime("%H:%M:%S")
    datafim = data_final.strftime("%Y-%m-%d")
    horafim = data_final.strftime("%H:%M:%S")

    return datainicio,horainicio,datafim,horafim

@routes_bp.route('/executar-ordem', methods=['POST'])
@login_required
def executar_ordem():

    dados = request.get_json()
    
    setor = dados['setor']
    maquina = dados['maquina']
    risco = dados['inputRisco']
    status = dados['statusLista']
    problema = dados['problema']
    id_ordem = dados['numeroOs']
    n_execucao = dados['n_ordem']
    descmanutencao = dados['descmanutencao']
    operador = dados['operador']
    confirmacao = verificar_maquina_preventiva(maquina)
    inputEquipamentoEmFalha = dados['inputEquipamentoEmFalha']
    codigoEquipamento = dados['codigoEquipamento']
    setorMaqSolda = dados['setorMaqSolda']
    qual_ferramenta = dados['qual_ferramenta']
    problemaaparente = dados['problema'] # será usado para descrever o nome do grupo de atividades
    solicitante = dados['inputSolicitante']

    # if status == 'Finalizado' and confirmacao:
    #     confirmacao = True
    #     status = 'Aguardando OK'
    # else:
    #     confirmacao = False
    
    matriculas_operadores = []

    # Itere sobre a lista
    for item in operador:
        # Divida a string usando '-' como separador e pegue a parte antes do hífen
        partes = item.split('-')
        if len(partes) > 0:
            matriculas = partes[0].strip()
            matriculas_operadores.append(matriculas)

    operador = matriculas_operadores

    operador = ",".join(operador)

    tipo_manutencao = dados['selectTipoManutencao']
    datetimes = dados['data_edit']
    area_manutencao = dados['areaManutencao']
    pvlye = dados['pvlye']
    pa_plus = dados['pa-plus']
    tratamento = dados['tratamento']
    ph_agua = dados['ph-agua']

    dataAbertura,natureza = buscar_data_abertura_natureza(id_ordem)

    botao1 = dados['maq-real-parada']
    botao2 = dados['exec-maq-parada']
    botao3 = dados['apos-exec-maq-parada']
    
    if status == 'Finalizada' or status == 'Aguardando OK':
        botao3 = 'true'
    
    datainicio,horainicio,datafim,horafim = formatar_data_hora(datetimes)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if solicitante == 'Automático' and natureza == 'Planejada' and status == 'Finalizada':
        cur.execute('update public.tb_grupos_preventivas set ult_manutencao = %s where codigo = %s and grupo = %s', (datafim,maquina,problemaaparente))

    cur.execute("""
        INSERT INTO tb_ordens (setor,maquina,risco,status,problemaaparente,
                                dataabertura,datainicio,horainicio,datafim,horafim,id_ordem,n_ordem,
                                descmanutencao, operador, natureza, tipo_manutencao,
                                area_manutencao,equipamento_em_falha,setor_maquina_solda, 
                                qual_ferramenta,cod_equipamento,
                                pvlye,pa_plus,tratamento,ph_agua,confirmacao) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (setor, maquina, risco, status, problema,dataAbertura, datainicio, horainicio,
            datafim, horafim, id_ordem, n_execucao, descmanutencao,
            operador, natureza, tipo_manutencao, area_manutencao,inputEquipamentoEmFalha,
            setorMaqSolda,qual_ferramenta,codigoEquipamento,
            pvlye, pa_plus, tratamento, ph_agua, confirmacao))

    cur.execute("""
        INSERT INTO tb_paradas (id_ordem,n_ordem, parada1, parada2, parada3) 
                VALUES (%s,%s,%s,%s,%s)
    """, (id_ordem, n_execucao, botao1, botao2, botao3))

    conn.commit()

    return 'sucess'

@routes_bp.route('/editar_ordem/<id_ordem>/<n_ordem>', methods=['POST', 'GET'])
@login_required
def editar_ordem(id_ordem, n_ordem):

    """
    Rota para editar execução dentro da ordem de serviço
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = ('SELECT * FROM tb_ordens WHERE id_ordem = {} AND n_ordem = {}'.format(int(id_ordem), int(n_ordem)))
    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)

    data1.reset_index(drop=True, inplace=True)
    data1.replace(np.nan, '', inplace=True)

    desc_manutencao = data1['descmanutencao'].values.tolist()[0]

    executante = data1['operador'].values.tolist()[0].replace(
        "{", "").replace("[", "").replace("\\", "").replace('"', '').replace("]}", "")
    executante = [exec.strip() for exec in executante.split(',')]

    try:
        s = ('SELECT * FROM tb_funcionario')
        cur.execute(s)
        df_funcionarios = pd.read_sql_query(s, conn)

        lista_executante = []

        for matricula in executante:
            funcionario = df_funcionarios[df_funcionarios['matricula'] == matricula][[
                'nome']].values.tolist()[0][0]
            lista_executante.append(matricula + " - " + funcionario)

        executante = lista_executante

    except:
        pass

    tipo_manutencao = data1['tipo_manutencao'].values.tolist()[0]
    area_manutencao = data1['area_manutencao'].values.tolist()[0]

    data_inicio = datetime.strptime(
        str(data1['datainicio'].values[0]), '%Y-%m-%d').strftime('%d/%m/%Y')
    hora_inicio = datetime.strptime(
        str(data1['horainicio'].values[0]), '%H:%M:%S').strftime('%H:%M')
    data_fim = datetime.strptime(
        str(data1['datafim'].values[0]), '%Y-%m-%d').strftime('%d/%m/%Y')
    hora_fim = datetime.strptime(
        str(data1['horafim'].values[0]), '%H:%M:%S').strftime('%H:%M')

    data_atual = f'{data_inicio} {hora_inicio} - {data_fim} {hora_fim}'

    print(data_atual)

    print(data_atual)

    data1 = data1.values.tolist()
    opcaoAtual = data1[0][4]

    cur.close()

    lista_opcoes = ['Em execução', 'Finalizada', 'Aguardando material']

    opcoes = []
    opcoes.append(opcaoAtual)

    for opcao in lista_opcoes:
        opcoes.append(opcao)

    opcoes = list(set(opcoes))
    opcoes.remove(opcaoAtual)  # Remove o elemento 'c' da lista
    opcoes.insert(0, opcaoAtual)

    query = """SELECT * FROM tb_funcionario"""
    tb_funcionarios = pd.read_sql_query(query, conn)
    tb_funcionarios['matricula_nome'] = tb_funcionarios['matricula'] + \
        " - " + tb_funcionarios['nome']
    tb_funcionarios = tb_funcionarios[['matricula_nome']].values.tolist()

    return render_template('user/editar_ordem.html', ordem=data1[0], tb_funcionarios=tb_funcionarios, opcoes=opcoes, tipo_manutencao=tipo_manutencao,
                           area_manutencao=area_manutencao, executante=executante, desc_manutencao=desc_manutencao, data_atual=data_atual)


@routes_bp.route('/editar_ordem_inicial/<id_ordem>/<n_ordem>', methods=['POST', 'GET'])
@login_required
def editar_ordem_inicial(id_ordem, n_ordem):

    """
    Rota para editar a ordem de serviço inicial, por exemplo: data de abertura, máquina, setor...
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    s = ("""SELECT tb_ordens.*, tb_maquinas.tombamento 
        FROM tb_ordens LEFT JOIN tb_maquinas ON tb_ordens.maquina = tb_maquinas.codigo
        WHERE tb_ordens.id_ordem = {} AND tb_ordens.n_ordem = {};""".format(int(id_ordem), int(n_ordem)))

    cur.execute(s)
    data1 = pd.read_sql_query(s, conn)
    
    try:
        data_completa = str(data1['dataabertura'][0] - timedelta(hours=3))
    except:
        data_completa = str(data1['ultima_atualizacao'][0] - timedelta(hours=3))

    try:
        data_datetime = datetime.strptime(data_completa, '%Y-%m-%d %H:%M:%S.%f%z')
    except:
        data_datetime = datetime.strptime(data_completa, '%Y-%m-%d %H:%M:%S+00:00')
    
    if data_datetime.strftime('%H:%M') == '00:00':
        data_completa = str(data1['ultima_atualizacao'][0])
        data_datetime = datetime.strptime(data_completa, '%Y-%m-%d %H:%M:%S.%f%z')

    dataabertura = data_datetime.strftime('%Y-%m-%d %H:%M')

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

    return render_template('user/editar_ordem_inicial.html', ordem=data1[0], maquina=maquina, n_ordem=n_ordem, dataabertura=dataabertura)


@routes_bp.route('/update_ordem/<id_ordem>/<n_ordem>', methods=['POST'])
@login_required
def update_ordem(id_ordem, n_ordem):  # Inserir as edições no banco de dados

    # # Execute a instrução SQL para alterar o tipo da coluna
    # alter_query = "ALTER TABLE tb_ordens ALTER COLUMN tipo_manutencao TYPE TEXT;"
    # cur.execute(alter_query)
    # conn.commit()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    if request.method == 'POST':

        # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # cur.execute(""" 
        #     SELECT MAX(id) FROM tb_ordens
        # """)

        # ultimo_id = cur.fetchone()[0]

        # try:
        #     ultimo_id = ultimo_id+1
        # except:
        #     ultimo_id = 0

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        s = (""" 
            SELECT natureza FROM tb_ordens where id_ordem = {} 
        """).format(id_ordem)

        df = pd.read_sql_query(s, conn)

        natureza = df['natureza'][0]
        # setor = request.form['setor']
        # maquina = request.form['maquina']
        # risco = request.form['risco']
        status = request.form['statusLista']
        # problema = request.form['problema']
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

        data_inicial = datetime.strptime(
            data_hora_inicial_str, "%d/%m/%Y %H:%M")
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
# Inserir as edições no banco de dados
def guardar_ordem_editada(id_ordem, n_ordem):

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
        dataabertura = request.form.get('datetimes')

        try:
            maquina = maquina.split(" - ")[0]
        except:
            pass

        if maquina_parada:
            maquina_parada = 'True'
        else:
            maquina_parada = 'False'

        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            UPDATE tb_ordens
            SET setor=%s,maquina=%s,risco=%s,maquina_parada=%s,equipamento_em_falha=%s,setor_maquina_solda=%s,
            qual_ferramenta=%s,cod_equipamento=%s,problemaaparente=%s,dataabertura=%s
            WHERE id_ordem = %s
            """, (setor, maquina, risco, maquina_parada, equipamento_em_falha, setor_maquina_solda,
                   qual_ferramenta, codigo_equipamento, problema, dataabertura, id_ordem))

        conn.commit()
        cur.close()

        return redirect(url_for('routes.Index'))


def solicitantes():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT concat(nome_colaborador,' - ',matricula_colaborador) FROM tb_matriculas"""

    cur.execute(query)
    nomes_solicitantes = cur.fetchall()

    return nomes_solicitantes

@routes_bp.route('/openOs')
def open_os():  # Página de abrir OS

    nomes_solicitantes = solicitantes()

    return render_template("user/openOs.html", solicitantes=nomes_solicitantes)


@login_required
@routes_bp.route('/maquinas/<setor>')
def filtro_maquinas(setor):

    # setor = setor.upper()
    if setor == 'Serralheria':
        setor = 'Solda'

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if setor == 'Administrativo':
        query = """
        SELECT codigo FROM tb_maquinas
        WHERE setor = %s
        """
    else:
        query = """
            SELECT concat (codigo, ' - ', descricao) FROM tb_maquinas
            WHERE setor = %s
            """

    cur.execute(query,(setor,))
    lista_maquinas = cur.fetchall()
    lista_maquinas.append(["Outros"])

    return jsonify(lista_maquinas)


@routes_bp.route('/edit_material/<id_ordem>', methods=['POST', 'GET'])
@login_required
def get_material(id_ordem):  # Informar material que foi utilizado na ordem de serviço
    # Verifica se a requisição é um POST

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == 'POST':

        # Obtendo o ultimo id

        # cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # s = ("""
        #     SELECT MAX(id) FROM tb_carrinho
        # """)
        # cur.execute(s)
        # try:
        #     max_id = cur.fetchall()[0][0] + 1
        # except:
        #     max_id = 0

        # Obtém os dados do formulário
        id_ordem = id_ordem
        codigo = request.form['codigo']
        quantidade = request.form['quantidade']

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("INSERT INTO tb_carrinho (id_ordem, codigo, quantidade) VALUES (%s,%s,%s)",
                    (id_ordem, codigo, quantidade))
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

    # cur.close()

    return render_template('user/material.html', datas=data, id_ordem=id_ordem, valorTotal=valorTotal[0][0])


@routes_bp.route('/grafico', methods=['POST', 'GET'])
@login_required
def grafico():

    return render_template('user/grafico.html')

@routes_bp.route('/timeline', methods=['POST'])
@login_required
def timeline_os():

    dados = request.get_json()

    id_ordem = dados['id_ordem']
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = ("""
        SELECT
            coalesce(o.dataabertura,o.ultima_atualizacao) as dataabertura,
            o.n_ordem,
            o.status,
            o.operador,
            o.descmanutencao,
            COALESCE(TO_TIMESTAMP(o.datainicio || ' ' || o.horainicio, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(o.dataabertura  - INTERVAL '3 hours' || ' ' || '00:00:00', 'YYYY-MM-DD HH24:MI:SS')) AS inicio,
            COALESCE(TO_TIMESTAMP(o.datafim || ' ' || o.horafim, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(o.dataabertura  - INTERVAL '3 hours' || ' ' || '00:00:00', 'YYYY-MM-DD HH24:MI:SS')) AS fim,
            func.nome,
            func.matricula,
            func.salario,
            tp.parada1,
            tp.parada2,
            tp.parada3
        FROM tb_ordens as o
        LEFT JOIN tb_funcionario as func ON ',' || o.operador || ',' LIKE '%,' || func.matricula || ',%'
        LEFT JOIN tb_paradas as tp on o.id_ordem = tp.id_ordem and o.n_ordem = tp.n_ordem
        WHERE o.id_ordem = {} AND (o.ordem_excluida IS NULL OR o.ordem_excluida = FALSE);
        """).format(int(id_ordem))
    
    df_timeline = pd.read_sql_query(s, conn)
    df_timeline = df_timeline.replace(np.nan, '-')
    
    try:
        df_timeline['inicio'] = pd.to_datetime(df_timeline['inicio'])
        df_timeline['fim'] = pd.to_datetime(df_timeline['fim'])

        # df_timeline['diferenca'] = pd.to_datetime(df_timeline['fim']) - pd.to_datetime(df_timeline['inicio'])
        df_timeline['diferenca'] = (df_timeline['fim'] - df_timeline['inicio']).apply(
            lambda x: x.total_seconds() // 60 if pd.notnull(x) else None)

        for i in range(len(df_timeline)):
            df_timeline['operador'][i] = df_timeline['operador'][i].replace(
                "{", "").replace("[", "").replace("\\", "").replace('"', '').replace("]}", "")
    except:
        df_timeline['diferenca'] = 0

    df_timeline = df_timeline.sort_values(by='n_ordem', ascending=True)

    if df_timeline['inicio'][0] == '-':
        df_timeline['inicio'][0] = df_timeline['dataabertura'][0]

    df_final = df_timeline

    if len(df_timeline) > 1:

        df_timeline['mesExecucao'] = df_timeline['fim'].dt.month
        df_timeline['anoExecucao'] = df_timeline['fim'].dt.year
        df_timeline['dias_uteis'] = df_timeline.apply(
            lambda row: calcular_dias_uteis(int(row['anoExecucao']), int(row['mesExecucao'])), axis=1)
        df_timeline['horasTotalMes'] = df_timeline['dias_uteis'] * (9*60)
        df_timeline['salario'] = df_timeline['salario'].replace("-", 0)
        df_timeline['salario'] = df_timeline['salario'].astype(float)
        df_timeline['proporcional'] = (
            df_timeline['salario'] * df_timeline['diferenca']) / df_timeline['horasTotalMes']

        df_groupby = df_timeline[['n_ordem', 'proporcional']].groupby(
            ['n_ordem']).sum().reset_index().round(2)

        df_timeline = df_timeline.drop(columns=[
                                       'mesExecucao', 'anoExecucao', 'dias_uteis', 'horasTotalMes', 'proporcional', 'nome', 'matricula', 'salario'])
        df_timeline = df_timeline.drop_duplicates(subset=['n_ordem'])

        df_final = pd.merge(df_timeline, df_groupby, how='left', on='n_ordem')

        df_final['diferenca'] = df_final['diferenca'].astype(int)

        totalMinutos = df_final['diferenca'].sum().tolist()
        totalCusto = df_final['proporcional'].sum().round(2).tolist()

        df_final = df_final.iloc[:, 1:]

        df_final = df_final.values.tolist()

        return jsonify (id_ordem, df_final, totalMinutos, totalCusto)
    else:
        df_final = df_final.iloc[:, 1:]
        
        df_final = df_final.values.tolist()

        return jsonify (id_ordem, df_final)

def tabela_maquinas_preventivas():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = (""" SELECT * FROM tb_planejamento_anual """)

    cur.execute(s)
    tabela_maquinas_preventivas = cur.fetchall()

    return tabela_maquinas_preventivas

@routes_bp.route('/tabela-grupos-preventivas')
def grupos_preventivas():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = (""" select * from tb_grupos_preventivas where excluidos = 'False' """)

    data = pd.read_sql_query(s, conn)

    # Retirando os grupos que não tem data estabelecida
    df_grupos_nan = data[data.isna().any(axis=1)]
    df_grupos_nan['proxima_manutencao'] = 'À decidir'
    df_grupos_nan.fillna('À decidir',inplace=True)
    
    # Grupos que ja tem data estabelecida
    df_grupos_notna = data.dropna()
    df_grupos_notna['proxima_manutencao'] = df_grupos_notna.apply(lambda row: calcular_proxima_data(row['ult_manutencao'], float(row['periodicidade'])*30), axis=1)

    df_grupos_notna['proxima_manutencao'] = pd.to_datetime(df_grupos_notna['proxima_manutencao'],format="%Y-%m-%d").dt.strftime("%d/%m/%Y")
    df_grupos_notna['ult_manutencao'] = pd.to_datetime(df_grupos_notna['ult_manutencao'],format="%Y-%m-%d").dt.strftime("%d/%m/%Y")

    # Juntando os dois grupos
    grupos_juntos = pd.concat([df_grupos_notna,df_grupos_nan]).values.tolist()

    return jsonify(grupos_juntos)

@routes_bp.route('/mostrar-preventivas', methods=['POST'])
def mostrar_preventivas():

    data = request.get_json()
    print(data)
    periodicidade_dias = data['periodicidade']*30
    data_ultima_manutencao = pd.to_datetime(data['ultima_manutencao'], format="%d/%m/%Y").date()
    planejamento_anual,semanas = calcular_planejamento_anual(data_ultima_manutencao,periodicidade_dias)

    data_return = {'planejamento_anual': planejamento_anual, 'semanas': semanas}

    return jsonify(data_return) 

@routes_bp.route('/tabela-historico-preventivas')
@login_required
def historico_planejadas():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query_historico_preventivas = """select max(n_ordem) as n_ordem_max,datafim,id_ordem,maquina, coalesce(status,'Em espera') as status 
                                    from tb_ordens 
                                    where natureza = 'Planejada' and ordem_excluida isnull
                                    group by status,datafim,id_ordem,maquina
                                    order by id_ordem,n_ordem_max"""

    data_historico_planejadas = pd.read_sql_query(query_historico_preventivas,conn)

    data_historico_planejadas.drop_duplicates(subset='id_ordem',keep='last',inplace=True)
    # data_historico_planejadas['datafim'] = data_historico_planejadas['datafim'].fillna('-')

    data_historico_planejadas['datafim'] = pd.to_datetime(data_historico_planejadas['datafim'],format="%Y-%m-%d",errors='coerce').dt.strftime("%d/%m/%Y").fillna('-')

    data_historico_planejadas = data_historico_planejadas.values.tolist()

    return jsonify(data_historico_planejadas)

@routes_bp.route('/52semanas', methods=['GET'])
@login_required
def plan_52semanas():

    # tabela com máquinas que podem ser criadas grupos de preventiva
    tabela_maquinas_preventivas_ = tabela_maquinas_preventivas()

    return render_template('user/52semanas.html', tabela_maquinas_preventivas_=tabela_maquinas_preventivas_)

@routes_bp.route('/preventivas', methods=['GET'])
@login_required
def preventivas():
    
    """
    Rota para visualizar as opções de atividades preventivas
    """

    # Obter o código da máquina a partir dos parâmetros da consulta
    codigo_maquina = request.args.get('codigo_maquina')

    # Use o código da máquina para gerar as opções dinamicamente
    # Substitua esta lógica pela lógica real que você precisa
    opcoes = obter_opcoes_preventivas(codigo_maquina)

    return jsonify(opcoes)

# Função de exemplo para gerar opções com base no código da máquina
def obter_opcoes_preventivas(codigo_maquina):

    """
    Função para buscar grupos de atividades por máquina caso tenha
    """
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = f"""SELECT DISTINCT (grupo) FROM tb_grupos_preventivas WHERE codigo = '{codigo_maquina}' AND excluidos = 'false' """

    cur.execute(sql)
    grupos = cur.fetchall()

    if len(grupos) == 0:
        return [[]]
    else:
        return grupos

@routes_bp.route('/atividadesGrupo', methods=['GET'])
@login_required
def atividadesGrupo():
    # Obtenha os parâmetros da consulta
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    codigo_maquina = request.args.get('codigo_maquina')
    grupo_selecionado = request.args.get('grupo')

    sql = 'SELECT ult_manutencao,periodicidade FROM tb_grupos_preventivas WHERE codigo = %s and grupo = %s'

    cur.execute(sql,(codigo_maquina,grupo_selecionado))
    data = cur.fetchall()

    # Use os parâmetros para carregar os dados associados
    dados_associados, parametros = tarefasGrupo(codigo_maquina, grupo_selecionado)  
    
    print(data)

    try:
        nova_data = data[0][0].strftime("%Y-%m-%d")
        periodicidade = data[0][1]
    except:
        nova_data = None
        periodicidade = None
    
    df = pd.DataFrame({'data': [nova_data],
                    'periodicidade': [periodicidade]})
    df.index = [0]  # Adiciona um índice à primeira linha

    try:
        df['data'] = pd.to_datetime(df['data'])
    except:
        pass

    try:
        df['proxima_manutencao'] = df.apply(lambda row: calcular_proxima_data(row['data'], float(row['periodicidade'])*30), axis=1)
        proxima_data = df['proxima_manutencao'][0]
    except Exception as e:
        print(f"Erro ao calcular próxima manutenção: {e}")
        proxima_data = None

    print(parametros)

    if len(parametros)>0:
        parametros[0].append(formatar_data(proxima_data))
    else:
        parametros = None


    # Retorne os dados como JSON
    return jsonify(dados_associados,parametros)

# Função de exemplo para obter dados associados a uma máquina e grupo

def tarefasGrupo(codigo_maquina, grupo_selecionado):
    
    """
    Função para buscar atividades preventivas de acordo com o grupo escolhido
    """
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    sql = f"""SELECT * FROM tb_atividades_preventiva WHERE codigo = '{codigo_maquina}' and grupo = '{grupo_selecionado}' and excluidos = 'false' """

    cur.execute(sql)
    atividades = cur.fetchall()
        
    sql = f"""SELECT ult_manutencao,periodicidade FROM tb_grupos_preventivas WHERE codigo = '{codigo_maquina}' and grupo = '{grupo_selecionado}'"""

    cur.execute(sql)
    parametros = cur.fetchall()

    if len(parametros) > 0:
        parametros[0][0] = formatar_data(parametros[0][0])

    if len(atividades) == 0:
        return [[]],parametros
    else:
        return atividades,parametros

@routes_bp.route("/excluir-tarefa", methods=['POST'])
def excluirTarefa():
    
    """
    Rota para excluir uma atividade preventiva
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    data = request.get_json()
    codigo_maquina = data['codigo_maquina']
    grupo = data['grupoSelecionado']
    excluidos = True
    try:
        idDaLinha = data['idDaLinha']
    except:
        idDaLinha = None

    if idDaLinha == '' or idDaLinha == None:
        sql_delete = f"""UPDATE public.tb_atividades_preventiva SET excluidos = '{excluidos}' WHERE codigo = '{codigo_maquina}' AND grupo = '{grupo}'; """

    else:
        sql_delete = f"""DELETE FROM public.tb_atividades_preventiva WHERE grupo = '{grupo}' and id = '{idDaLinha}' """

    cur.execute(sql_delete)

    conn.commit()
    conn.close()

    return 'sucess'


def proxima_data_util(data_inicial, periodicidade):
  
  """
  Retorna a próxima data útil, contando apenas dias úteis, sem sábados e domingos.

  Args:
    data_inicial: A data inicial.
    periodicidade: A periodicidade da data.

  Returns:
    A próxima data útil.
  """

  # Converte as datas para objetos do tipo date.

  data_inicial = date.fromisoformat(data_inicial)

  # Calcula o número de dias úteis entre a data inicial e a data atual.

  dias_uteis = 0
  while data_inicial <= date.today():
    if data_inicial.weekday() < 5:
      dias_uteis += 1
    data_inicial += timedelta(days=1)

  # Adiciona a periodicidade à data inicial.

  data_final = data_inicial + timedelta(days=int(periodicidade))

  # Verifica se a data final é um sábado ou domingo.

  if data_final.weekday() >= 5:
    # Empurra a data para segunda-feira.
    data_final += timedelta(days=3)

  return data_final.isoformat()


def formatar_data(data):
    
    """
    Função para formatar data dentro da lista
    """

    return data.strftime("%Y-%m-%d") if isinstance(data, date) else data


@routes_bp.route('/criar-grupo', methods=['POST'])
def rota_criar_grupo():

    """
    Rota para receber o nome da máquina e criar o grupo
    """

    data = request.get_json()

    nome_grupo = data['nome_grupo']
    codigo_maquina = data['codigo_maquina']

    resultado_criar_grupo = criar_grupo(codigo_maquina,nome_grupo)

    if resultado_criar_grupo == "Grupo já existente":
        return jsonify("Grupo já existente")

    return 'sucess'

def criar_grupo(codigo_maquina,nome_grupo):

    """
    Função para criar grupo de atividades preventivas ao clicar no botão "Criar grupo".
    Recebe apenas a máquina e busca o último grupo criado a ela e adicionar + 1.
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = f"""SELECT * FROM tb_grupos_preventivas WHERE codigo = '{codigo_maquina}' AND grupo = '{nome_grupo}' """

    cur.execute(sql)
    grupos = cur.fetchall()

    if len(grupos) > 0:
        return "Grupo já existente"
        
    sql = f"""INSERT INTO tb_grupos_preventivas (codigo,grupo) VALUES ('{codigo_maquina}','{nome_grupo}')"""

    cur.execute(sql)

    conn.commit()
    conn.close()

    return "Sucesso"


@routes_bp.route('/receber-tarefas', methods=['POST'])
def receber_tarefas():

    """
    Rota para receber as atividades associadas a maquina e ao grupo
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    json_tarefas = request.get_json()
    print(json_tarefas)

    periodicidade = float(json_tarefas['parametros'][0]['periodicidade_grupo'])
    ultima_manutencao = json_tarefas['parametros'][0]['ultima_manutencao']
    grupo = json_tarefas['parametros'][0]['grupo']
    codigo_maquina = json_tarefas['parametros'][0]['codigo_maquina']

    sql_update = f"""UPDATE tb_grupos_preventivas SET ult_manutencao = '{ultima_manutencao}', periodicidade = {periodicidade}
        WHERE codigo = '{codigo_maquina}' and grupo = '{grupo}'"""

    sql_insert = f"""INSERT INTO tb_grupos_preventivas (codigo,grupo,ult_manutencao,periodicidade) VALUES ('{codigo_maquina}', '{grupo}',
        '{ultima_manutencao}', '{periodicidade}')"""
    
    try:
        cur.execute(sql_update)
    except:
        cur.execute(sql_insert) 

    conn.commit()

    if len(json_tarefas['dadosTabela']) > 0:
        adicionar_editar_tarefa(json_tarefas)
  
    return 'sucess'


def adicionar_editar_tarefa(json_tarefas):
    """
    Função para editar e adicionar tarefa no banco de dados.
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    for tarefa in range(len(json_tarefas['dadosTabela'])):
        
        if json_tarefas['dadosTabela'][tarefa]['atividadeAntiga'] != '':

            atividade_atual = json_tarefas['dadosTabela'][tarefa]['atividade']
            responsabilidade_atual = json_tarefas['dadosTabela'][tarefa]['responsabilidade']

            atividade_antiga = json_tarefas['dadosTabela'][tarefa]['atividadeAntiga']
            responsabilidade_antiga = json_tarefas['dadosTabela'][tarefa]['responsabilidadeAntiga']

            id_unico = int(json_tarefas['dadosTabela'][tarefa]['id_unico'])

            sql_edit = f"""UPDATE tb_atividades_preventiva SET atividade = '{atividade_atual}', responsabilidade = '{responsabilidade_atual}', id = {id_unico}
                        WHERE atividade = '{atividade_antiga}' and responsabilidade = '{responsabilidade_antiga}' and id = {id_unico} """
            
            cur.execute(sql_edit)
        
        else:

            atividade = json_tarefas['dadosTabela'][tarefa]['atividade'] 
            responsabilidade = json_tarefas['dadosTabela'][tarefa]['responsabilidade']
            grupo = json_tarefas['dadosTabela'][tarefa]['grupo']
            codigo_maquina = json_tarefas['dadosTabela'][tarefa]['codigo_maquina']
            
            sql = f"""
                    INSERT INTO tb_atividades_preventiva (codigo,grupo,responsabilidade,atividade)
                        VALUES ('{codigo_maquina}','{grupo}','{responsabilidade}','{atividade}')
                    """

            cur.execute(sql)

    conn.commit()
    conn.close()


@routes_bp.route('/receber-upload', methods=['POST'])
def receber_upload():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Obter o arquivo do formulário
    file = request.files['file']

    # Obter outras informações do formulário
    grupo_selecionado = request.form['grupoSelecionado']
    codigo_maquina = request.form['codigo_maquina']   

    print(file)

    print(codigo_maquina)

    # Salvar o arquivo no servidor (opcional)
    # file.save('uploads_atividade/' + file.filename)

    # file = r"uploads_atividade/" + file.filename

    try:
        df = pd.read_csv(file, sep=";", encoding='ISO-8859-1')
    except pd.errors.ParserError:
        df = pd.read_excel(file)

    # Remover caracteres especiais do cabeçalho das colunas
    df.columns = df.columns.str.replace('ï»¿', '')

    colunas_esperadas = ['codigo_maquina', 'responsabilidade', 'atividade']  # Substitua com as colunas reais
    

    # Verificar se as colunas do DataFrame coincidem com as colunas esperadas
    if set(df.columns) != set(colunas_esperadas):
        return 'Colunas do arquivo não correspondem ao modelo'

    df['grupo'] = grupo_selecionado

    df = df[['codigo_maquina','grupo','responsabilidade','atividade']]

    df_list = df.values.tolist()

    print(df_list)

    for row in df_list:       

        codigo_maquina_grupo = row[0]
        if codigo_maquina != codigo_maquina_grupo:
            return 'Código de máquina inválido'
        grupo = row[1]
        responsabilidade = row[2]
        atividade = row[3]

        sql_insert = f"""INSERT INTO tb_atividades_preventiva (codigo,grupo,responsabilidade,atividade)
                        VALUES ('{codigo_maquina_grupo}','{grupo}','{responsabilidade}','{atividade}')"""

        cur.execute(sql_insert)

    conn.commit()
    conn.close()

    # os.remove(file)
    
    return 'sucess'


@routes_bp.route('/download-modelo-atividades', methods=['GET'])
def download_modelo_excel():
    # Caminho para o arquivo modelo CSV
    excel_filename = 'modelo_atividades.csv'

    # Envie o arquivo para download
    return send_file(excel_filename, as_attachment=True)


@routes_bp.route('/cadastrar52', methods=['POST', 'GET'])
@login_required
def cadastro_preventiva():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
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
            apelido = request.form['apelido']

            print(codigo,setor,descricao,tombamento,criticidade,manut_inicial,periodicidade,togglePreventiva)

            # df = gerador_de_semanas_informar_manutencao(
            #     setor, codigo, descricao, tombamento, criticidade, manut_inicial, periodicidade)

            print("antes")

            # codigo = 'teste1'
            # setor = 'Administrativo'
            # descricao ='123'
            # tombamento = '123'
            # criticidade = 'B'
            # manut_inicial = '2023-12-12'
            # periodicidade = 30
            # apelido = 'teste'

            df = gerar_planejamento_maquinas_preventivas(codigo,setor,descricao,tombamento,criticidade,manut_inicial,periodicidade)
            
            print("depois")

            df['ultima_manutencao'] = df['ultima_manutencao'].dt.strftime("%Y-%m-%d")
            df['proxima_manutencao'] = df['proxima_manutencao'].dt.strftime("%Y-%m-%d")
            df['periodicidade'] = df['periodicidade'].astype(str)

            lista = df.values.tolist()
            # lista = lista[0]

            print(lista)

            s = ("""
                SELECT * FROM tb_planejamento_anual
                """)

            maquina_cadastrada = pd.read_sql_query(s, conn)

            if len(maquina_cadastrada[maquina_cadastrada['codigo'] == codigo]) > 0:
                flash("Máquina ja cadastrada", category='danger')

            else:

                try:
                    conn = psycopg2.connect(
                        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
                    cur = conn.cursor(
                        cursor_factory=psycopg2.extras.DictCursor)

                    # Consulta SQL para inserir os dados na tabela
                    sql_insert = "INSERT INTO tb_planejamento_anual VALUES ({})".format(','.join(['%s'] * len(lista[0])))

                    # Executar a consulta SQL para cada sublista
                    for linha in lista:
                        cur.execute(sql_insert, linha)

                    query_max = ("""SELECT max(id) FROM tb_maquinas""")
                    cur.execute(query_max)
                    id = cur.fetchall()
                    id = id[0][0] + 1

                    cur.execute("INSERT INTO tb_maquinas (id, setor, codigo, descricao, tombamento,apelido) VALUES (%s,%s, %s, %s, %s,%s)",
                                (id, setor, codigo, descricao, tombamento, apelido))

                    # Confirmar a transação
                    conn.commit()

                    flash("Máquina cadastrada com sucesso", category='sucess')

                except Error as e:
                    print(
                        f"Ocorreu um erro ao conectar ou executar a consulta no PostgreSQL: {e}")

                finally:
                    # Fechar o cursor e a conexão com o banco de dados
                    cur.close()
                    conn.close()

                flash("Máquina cadastrada com sucesso", category='sucess')

            return render_template('user/cadastrar52.html')

        except:
            codigo = request.form['codigo']
            tombamento = request.form['tombamento']
            descricao = request.form['descricao']
            setor = request.form['setor']
            apelido = request.form['apelido']

            s = ("""
                SELECT * FROM tb_maquinas
                """)

            query_max = ("""SELECT max(id) FROM tb_maquinas""")
            cur.execute(query_max)
            id = cur.fetchall()
            id = id[0][0] + 1

            maquina_cadastrada = pd.read_sql_query(s, conn)

            if len(maquina_cadastrada[maquina_cadastrada['codigo'] == codigo]) > 0:
                flash("Máquina ja cadastrada", category='danger')

            else:

                cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

                cur.execute("INSERT INTO tb_maquinas (id, setor, codigo, descricao, tombamento,apelido) VALUES (%s,%s, %s, %s, %s,%s)",
                            (id, setor, codigo, descricao, tombamento, apelido))

                conn.commit()

                flash("Máquina cadastrada com sucesso", category='sucess')

            return render_template('user/cadastrar52.html')

    return render_template('user/cadastrar52.html')

@routes_bp.route('/verificar-codigo-existente', methods=['POST'])
def verificar_codigo_existente():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    codigo = request.get_json()

    print(codigo)

    cur.execute('select codigo from tb_maquinas where codigo = %s', (codigo,))

    data = cur.fetchall()

    if len(data)>0:
        return jsonify({'codigo_existente':True})
    else:
        return jsonify({'codigo_existente':False})

def adicionar_maquina(dados):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute('insert into public.tb_maquinas (setor,codigo,descricao,tombamento,apelido) values(%s,%s,%s,%s,%s)',(dados['setor'],dados['codigo'],dados['descricao'],
                                                                                                                     dados['tombamento'],dados['apelido']))

    conn.commit()

    return 'sucess'

def adicionar_maquina_preventiva(dados):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute('insert into public.tb_planejamento_anual (codigo,classificacao) values(%s,%s)',(dados['codigo'],dados['criticidade']))

    conn.commit()

    return 'sucess'

@routes_bp.route('/cadastrar-maquina', methods=['POST'])
@login_required
def cadastrar_maquina():

    dados = request.get_json()
    
    adicionar_maquina(dados)

    if dados['preventiva']:
        adicionar_maquina_preventiva(dados)

    return 'sucess'


@routes_bp.route('/testes_envio_pdf/<codigo_maquina>', methods=['POST'])
@login_required
def testes_envio_pdf(codigo_maquina):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    # Certifique-se de usar 'pdfFile' para corresponder ao nome do campo no FormData
    pdfs = request.files.getlist('pdfFile')

    print(pdfs)

    if len(pdfs) > 0:
        for pdf in pdfs:
            if pdf.filename != '':
                pdf_data = pdf.read()
                pdf_filename = pdf.filename  # Obtém o nome do arquivo

                cur.execute("INSERT INTO tb_anexos (codigo_maquina, checklist, nome_arquivo) VALUES (%s,%s,%s)",
                            (codigo_maquina, pdf_data, pdf_filename))
                conn.commit()

    cur.close()
    conn.close()

    # Você pode personalizar a mensagem de retorno conforme necessário
    return jsonify({"message": "Upload bem-sucedido"})


@routes_bp.route('/visualizar_midias/<id_ordem>', methods=['GET'])
@login_required
def visualizar_midias(id_ordem):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    # Buscar as imagens associadas à ordem de serviço
    cur.execute("SELECT imagem FROM tb_imagens WHERE id_ordem = %s", (id_ordem,))
    imagens_data = [base64.b64encode(row[0]).decode(
        'utf-8') for row in cur.fetchall()]

    return jsonify(imagens_data=imagens_data, id_ordem=id_ordem)


@routes_bp.route('/visualizar_video/<id_ordem>', methods=['GET'])
@login_required
def visualizar_video(id_ordem):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    # Buscar os vídeos associados à ordem de serviço
    cur.execute(
        "SELECT video FROM tb_videos_ordem_servico WHERE id_ordem = %s", (id_ordem,))
    videos_data = [base64.b64encode(row[0]).decode('utf-8')
                   for row in cur.fetchall()]

    # Convertendo os dados de vídeo em URLs
    video_urls = []
    for video_data in videos_data:
        video_url = f"data:video/mp4;base64,{video_data}"
        video_urls.append(video_url)

    return jsonify(videos_data=video_urls)


@routes_bp.route('/timeline-preventiva/<maquina>', methods=['POST', 'GET'])
@login_required
# Mostrar o histórico de preventiva daquela máquina
def timeline_preventiva(maquina):

    print(maquina)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Obtém os dados da tabela
    s = ("""
        SELECT * 
        FROM tb_ordens
        WHERE ordem_excluida IS NULL OR ordem_excluida = FALSE
        """)

    df = pd.read_sql_query(s, conn)
    df['maquina'] = df['maquina'].str.strip()

    df = df[df['maquina'] == maquina].reset_index(drop=True)
    # df = df[df['natureza'] == 'Planejada'].reset_index(drop=True)

    df[['dataabertura', 'id_ordem']]

    # Limpar a coluna
    for i in range(len(df)):
        try:
            df['operador'][i] = df['operador'][i].replace("{", "").replace("}", "").replace(
                "[", "").replace("]", "").replace("\\", "").replace('"', '').replace("]}", "").replace("}}", "")
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

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    cur.execute(
        "SELECT checklist, nome_arquivo FROM tb_anexos WHERE codigo_maquina = %s", (codigo_maquina,))
    pdf_records = cur.fetchall()

    pdf_urls = []
    nome_arquivos = []

    for pdf_record in pdf_records:
        pdf_data, nome_arquivo = pdf_record
        pdf_stream = BytesIO(pdf_data)

        # Gere o URL do download incluindo o nome do arquivo
        pdf_url = f'/download_pdf/{codigo_maquina}/{len(pdf_urls) + 1}'
        pdf_urls.append(pdf_url)
        nome_arquivos.append(nome_arquivo)

    cur.close()
    conn.close()

    return jsonify({'pdfUrls': pdf_urls, 'nome_arquivo': nome_arquivos})


@routes_bp.route('/download_pdf/<codigo_maquina>/<int:pdf_index>', methods=['GET'])
@login_required
def download_pdf(codigo_maquina, pdf_index):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    print(pdf_index)

    cur.execute(
        "SELECT checklist FROM tb_anexos WHERE codigo_maquina = %s", (codigo_maquina,))
    pdf_records = cur.fetchall()

    if 0 <= pdf_index - 1 < len(pdf_records):
        pdf_data = pdf_records[pdf_index - 1][0]
        pdf_stream = BytesIO(pdf_data)

        cur.close()
        conn.close()

        response = make_response(pdf_stream.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = f'attachment; filename=pdf_{pdf_index}.pdf'
        return response
    else:
        cur.close()
        conn.close()
        return jsonify({'error': 'PDF não encontrado'}), 404


@routes_bp.route('/remover_pdf/<codigo_maquina>/<nome_arquivo>', methods=['DELETE'])
@login_required
def remover_pdf(codigo_maquina, nome_arquivo):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    print(nome_arquivo)

    cur.execute("SELECT id FROM tb_anexos WHERE codigo_maquina = %s AND nome_arquivo = %s",
                (codigo_maquina, nome_arquivo))
    pdf_id = cur.fetchone()

    if pdf_id:
        cur.execute("DELETE FROM tb_anexos WHERE id = %s", (pdf_id[0],))
        conn.commit()

        cur.close()
        conn.close()

        return jsonify({"message": "PDF removido com sucesso"})
    else:
        cur.close()
        conn.close()
        return jsonify({'error': 'PDF não encontrado'}), 404


@routes_bp.route('/lista_maquinas', methods=['GET'])
@login_required
def lista_maquinas():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cur.execute(""" SELECT 
                        tb_planejamento_anual.codigo,
                        tb_maquinas.setor,
                        tb_maquinas.descricao,
                        tb_maquinas.tombamento,
                        tb_maquinas.apelido
                    FROM tb_planejamento_anual
                    JOIN tb_maquinas ON tb_planejamento_anual.codigo = tb_maquinas.codigo; """)

    df_c_preventivas = pd.DataFrame(cur.fetchall(), columns=[
                                    'codigo', 'setor', 'descricao', 'tombamento', 'apelido'])
    df_c_preventivas['setor'] = df_c_preventivas['setor'].str.title()
    df_c_preventivas['preventiva'] = 'Y'

    cur.execute(""" SELECT codigo, setor, descricao, tombamento, apelido
                    FROM tb_maquinas; """)

    df_s_preventivas = pd.DataFrame(cur.fetchall(), columns=[
                                    'codigo', 'setor', 'descricao', 'tombamento', 'apelido'])
    df_s_preventivas['setor'] = df_s_preventivas['setor'].str.title()
    df_s_preventivas['preventiva'] = 'N'

    df_final = pd.concat([df_c_preventivas, df_s_preventivas]).drop_duplicates(
        subset='codigo', keep='first').reset_index(drop=True)

    for i in range(len(df_final)):
        if df_final['tombamento'][i] == None:
            df_final['tombamento'][i] = ''
        if df_final['apelido'][i] == None:
            df_final['apelido'][i] = ''

    data = df_final.values.tolist()

    return render_template('user/lista_maquinas.html', data=data)


@routes_bp.route('/excluir-ordem', methods=['POST'])
@login_required
def excluir_ordem():
    
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    data = request.get_json()
    
    print(data)
    id_linha = data['id']
    texto = data['texto']

    print(id_linha, texto)

    query = """UPDATE tb_ordens
            SET ordem_excluida = 'true', motivo_exclusao = %s
            WHERE id_ordem = %s
            """

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query, [texto, id_linha])
    conn.commit()

    return 'Dados recebidos com sucesso!'


@routes_bp.route('/visualizar_pdf/<id_ordem>')
@login_required
def visualizar_pdf(id_ordem):

    return formulario_os(id_ordem)


@routes_bp.route('/transformar-maquina',methods=['POST'])
@login_required
def transformar_maquina():

    data = request.get_json()

    codigo_maquina = data['codigo_preventivas']
    classificacao = data['classificacao']

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    print(codigo_maquina,classificacao)
    
    cur.execute("INSERT INTO tb_planejamento_anual (codigo, classificacao) VALUES (%s, %s)",
                            (codigo_maquina, classificacao))
    print("Máquina transformada com sucesso")

    conn.commit()
    conn.close()

    return 'Sucesso'


@routes_bp.route('/modal-editar-maquina',methods=['POST'])
@login_required
def editar_maquina():

    codigo_maquina = request.get_json()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    query = """SELECT 
                    tb_maquinas.codigo,
                    tb_maquinas.setor,
                    tb_maquinas.apelido,
                    tb_maquinas.descricao,
                    tb_maquinas.tombamento
                FROM tb_maquinas
                WHERE tb_maquinas.codigo = '{}';""".format(codigo_maquina)

    cur.execute(query)
    data = cur.fetchall()

    setor = data[0][1]
    apelido = data[0][2]
    descricao = data[0][3]
    tombamento = data[0][4]

    if not tombamento:
        tombamento = ''

    if not apelido:
        apelido = ''

    data = {'codigo':codigo_maquina,'setor':setor,'descricao':descricao,'tombamento':tombamento,'apelido':apelido}

    return jsonify(data)


@routes_bp.route('/envio-edicao-maquina-preventiva', methods=['POST'])
@login_required
def envio_editar_maquina_preventiva():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    data = request.get_json()

    codigo_maquina = data['codigo_preventivas']
    tombamento = data['tombamento_preventivas']
    descricao = data['descricao_preventivas']
    apelido = data['apelido_preventivas']

    print(tombamento,descricao,apelido)

    cur.execute("""
                UPDATE tb_maquinas
                SET tombamento=%s,descricao=%s,apelido=%s
                WHERE codigo = %s
                """, (tombamento, descricao, apelido, codigo_maquina))

    conn.commit()
    conn.close()
    
    return jsonify(codigo_maquina)

@routes_bp.route('/modal-editar-maquina-preventiva', methods=['POST'])
@login_required
def editar_maquina_preventiva():

    codigo_maquina = request.get_json()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """SELECT 
                tb_planejamento_anual.codigo,
                tb_maquinas.tombamento,
                tb_maquinas.setor,
                tb_maquinas.descricao,
                tb_planejamento_anual.classificacao,
                tb_maquinas.apelido
            FROM tb_planejamento_anual
            JOIN tb_maquinas ON tb_planejamento_anual.codigo = tb_maquinas.codigo
            WHERE tb_planejamento_anual.codigo = '{}';""".format(codigo_maquina)

    cur.execute(query)
    data = cur.fetchall()

    codigo_maquina = codigo_maquina
    tombamento = data[0][1]
    setor = data[0][2]
    descricao = data[0][3]
    criticidade = data[0][4]
    apelido = data[0][5]

    if not tombamento:
        tombamento = ''
    if not apelido:
        apelido = ''

    data = {'codigo':codigo_maquina,'setor':setor,'descricao':descricao,'tombamento':tombamento,'apelido':apelido,
            'criticidade':criticidade}

    return jsonify(data)


@routes_bp.route('/envio-edicao-maquina', methods=['POST'])
@login_required
def envio_editar_maquina():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    data = request.get_json()

    codigo_maquina = data['codigo']
    tombamento = data['tombamento']
    descricao = data['descricao']
    apelido = data['apelido']

    print(tombamento,descricao,apelido,codigo_maquina)

    cur.execute("""
                UPDATE tb_maquinas
                SET tombamento=%s,descricao=%s,apelido=%s
                WHERE codigo = %s
                """, (tombamento, descricao, apelido, codigo_maquina))

    conn.commit()
    conn.close()
    
    return 'Sucesso'


@routes_bp.route('/excluir-maquina', methods=['POST'])
@login_required
def excluir_maquina():
    if request.method == 'POST':
        # Obter o código da máquina enviado pelo frontend
        codigo_maquina = request.form.get('codigo_maquina')

        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """DELETE FROM tb_maquinas
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])

        query = """DELETE FROM tb_planejamento_anual
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])

        conn.commit()
        conn.close()

        # flash("Máquina excluída com sucesso", category='sucess')

        return 'Dados recebidos com sucesso!'


@routes_bp.route('/excluir-preventiva', methods=['POST'])
@login_required
def excluir_preventiva():

    if request.method == 'POST':
        # Obter o código da máquina enviado pelo frontend
        codigo_maquina = request.form.get('codigo_maquina')

        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """DELETE FROM tb_planejamento_anual
                WHERE codigo = %s;
                """

        cur.execute(query, [codigo_maquina])

        conn.commit()
        conn.close()

        # flash("Máquina excluída com sucesso", category='sucess')

        return 'Dados recebidos com sucesso!'


@routes_bp.route('/excluir-execucao', methods=['POST'])
@login_required
def excluir_execucao():

    id_ordem = int(request.form.get('id_ordem'))
    n_ordem = int(request.form.get('n_execucao'))

    print(id_ordem, n_ordem)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """
            DELETE FROM tb_ordens
            WHERE id_ordem=%s and n_ordem=%s;
            """

    cur.execute(query, [id_ordem, n_ordem])

    conn.commit()
    conn.close()

    flash("Execução excluída com sucesso", category='sucess')

    return 'Execução excluída com sucesso'

@routes_bp.route('/excluir-grupo', methods=['POST'])
@login_required
def excluir_grupo():


    json_grupos_excluidos = request.get_json()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    excluido = True
    
    codigo_maquina = json_grupos_excluidos['codigo_maquina']

    grupoSelecionado = json_grupos_excluidos['grupoSelecionado']

    print(codigo_maquina,grupoSelecionado,excluido)

    cur.execute(""" UPDATE tb_grupos_preventivas
                    SET excluidos=%s
                    WHERE codigo = %s AND grupo = %s
                    """, (excluido, codigo_maquina, grupoSelecionado))

    conn.commit()
    conn.close()

    flash("Execução excluída com sucesso", category='sucess')

    return 'Execução excluída com sucesso'

@routes_bp.route("/funcionarios", methods=['POST', 'GET'])
@login_required
def funcionarios():
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == 'POST':

        data = request.get_json()

        print(data)

        nome = data['nome']
        matricula = data['matricula']
        ativo = data['ativo']
        salario = data['salario']
        funcao = data['funcao']

        print(nome, matricula, ativo, salario, funcao)

        s = (""" SELECT * FROM tb_funcionario""")

        funcionario_cadastrado = pd.read_sql_query(s, conn)

        if len(funcionario_cadastrado[funcionario_cadastrado['nome'] == nome]) > 0 or len(funcionario_cadastrado[funcionario_cadastrado['matricula'] == matricula]) > 0:
            print("Funcionário ja cadastrado")
            return jsonify("Funcionário ja cadastrado")
        else:

            cur.execute("INSERT INTO tb_funcionario (nome, matricula, ativo, salario, funcao) VALUES (%s, %s, %s, %s,%s)",
                        (nome, matricula, ativo, salario, funcao))

            conn.commit()
            conn.close()

            print("Funcionário cadastrado com sucesso")

        return jsonify("Funcionário cadastrado com sucesso")

    query = """SELECT * FROM tb_funcionario"""

    cur.execute(query)
    data = cur.fetchall()
    df_data = pd.DataFrame(data)
    funci = df_data.values.tolist()

    return render_template("user/funcionarios.html", funci=funci)


@routes_bp.route("/editar_funcionarios", methods=['POST', 'GET'])
@login_required
def editar_funcionarios():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == 'POST':
        nome_antigo = request.form.get('nome_antigo')
        nome_novo = request.form.get('nome')
        matricula = request.form.get('matricula')
        ativo = request.form.get('ativo')
        salario = request.form.get('salario')
        funcao = request.form.get('funcao')

        # Use o valor selecionado no <select> para atualizar o registro no banco de dados
        query = """
        UPDATE tb_funcionario
        SET nome = %s, matricula = %s, ativo = %s, salario = %s, funcao = %s
        WHERE nome = %s
        """

        cur.execute(query, (nome_novo, matricula, ativo,
                    salario, funcao, nome_antigo))
        conn.commit()
        conn.close()

        return render_template("user/funcionarios.html")

    selected_value = request.args.get('selectedValue')
    if selected_value:
        query = """SELECT nome, matricula, ativo, salario, funcao FROM tb_funcionario WHERE nome = '{}';""".format(
            selected_value)
        cur.execute(query)
        data = cur.fetchone()  # Assume que há apenas um registro correspondente
    else:
        # Se nenhum valor foi selecionado, retorne valores vazios
        data = {'nome': '', 'matricula': '',
                'ativo': '', 'salario': '', 'funcao': ''}

    return jsonify(data)