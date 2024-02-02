import psycopg2  # pip install psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date,timedelta,datetime
from pandas.tseries.offsets import BMonthEnd,MonthEnd,BDay
import numpy as np
import datetime

DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

def calcular_proxima_data(data_atual, periodicidade_em_dias):

    dias_uteis = pd.offsets.BDay(round(periodicidade_em_dias))  # Considera dias úteis (BDay)
    proxima_data = data_atual + dias_uteis
    return proxima_data.strftime("%Y-%m-%d") 

def abertura_15_dias():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """select *, periodicidade*30 as periodicidade_em_dias from tb_grupos_preventivas where ult_manutencao is not null and excluidos = false and periodicidade > 1"""

    cur.execute(query)
    data = cur.fetchall()

    df_data = pd.DataFrame(data)

    df_data['proxima_manutencao'] = ''
    
    for row in range(len(df_data)):
        
        ultima_manutencao = df_data[3][row]
        periodicidade = df_data[6][row]

        proxima_manutencao = calcular_proxima_data(ultima_manutencao, round(periodicidade))

        df_data['proxima_manutencao'][row] = proxima_manutencao

    hoje_string = date.today()
    quinze_dias = calcular_proxima_data(hoje_string,15)

    proxima_manutencao_ = df_data[df_data['proxima_manutencao'] == quinze_dias]

    # proxima_manutencao_ = proxima_manutencao_.drop_duplicates(subset=1) 

    # maquinas = ['PredT.Mec','PredVa.Mec','UT-SE-00','PredT.Ele','UT-SE-01','UT-SE-02']
    # grupos = ['15 Dias','2 Meses','6 Meses','15 Dias','6 Meses','6 Meses']

    if len(proxima_manutencao_) > 0:

        for index, row in proxima_manutencao_.iterrows():
        # for maquina,grupo in zip(maquinas, grupos):
            maquina = row[1]
            grupo = row[2]
            # atividades = buscar_atividades(maquina,grupo)
            criar_ordem(maquina, grupo)

def abertura_7_dias():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """select *, periodicidade*30 as periodicidade_em_dias from tb_grupos_preventivas where ult_manutencao is not null and excluidos = false and periodicidade < 1"""

    cur.execute(query)
    data = cur.fetchall()

    df_data = pd.DataFrame(data)

    df_data['proxima_manutencao'] = ''

    for row in range(len(df_data)):
        
        ultima_manutencao = df_data[3][row]
        periodicidade = df_data[6][row]

        proxima_manutencao = calcular_proxima_data(ultima_manutencao, round(periodicidade))

        df_data['proxima_manutencao'][row] = proxima_manutencao

    hoje_string = date.today()
    sete_dias = calcular_proxima_data(hoje_string,5)

    proxima_manutencao_ = df_data[df_data['proxima_manutencao'] == sete_dias]
    # proxima_manutencao_ = proxima_manutencao_.drop_duplicates(subset=1) 

    # maquinas = ['PredT.Mec','PredVa.Mec','UT-SE-00','PredT.Ele','UT-SE-01','UT-SE-02']
    # grupos = ['15 Dias','2 Meses','6 Meses','15 Dias','6 Meses','6 Meses']

    if len(proxima_manutencao_) > 0:

        for index, row in proxima_manutencao_.iterrows():
        # for maquina,grupo in zip(maquinas, grupos):
            maquina = row[1]
            grupo = row[2]
            # atividades = buscar_atividades(maquina,grupo)
            criar_ordem(maquina, grupo)

def formatar_data(data):
    
    """
    Função para formatar data dentro da lista
    """

    return data.strftime("%Y-%m-%d") if isinstance(data, date) else data

def buscar_atividades(maquina,grupo):

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql = f"""select atividade from tb_atividades_preventiva where codigo = '{maquina}' and grupo = '{grupo}'"""

    cur.execute(sql)
    atividades = cur.fetchall()

    atividades_lista = [item for sublist in atividades for item in sublist]

    # Criando a string com quebras de linha
    atividades = '\n'.join(atividades_lista)

    return atividades

def criar_ordem(maquina, grupo):

  """
  Função para criar ordem com base na data da preventiva
  """

  conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                          password=DB_PASS, host=DB_HOST)
  cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

  sql_maquina = f"""SELECT setor FROM tb_maquinas WHERE codigo = '{maquina}'"""

  cur.execute(sql_maquina)
  data_maquinas = cur.fetchall()
  setor = data_maquinas[0][0]

  maquina = maquina
  setor = setor
  risco = 'Médio'
  problemaaparente = grupo
  tipo_manutencao = 'Preventiva'
  solicitante = 'Automático'
  natureza = 'Planejada'
  status = 'Em espera'
  n_ordem = 0
  dataabertura = datetime.datetime.now()

  cur.execute("select MAX(id_ordem) from tb_ordens")

  lista_max = cur.fetchall()
  max_id_ordem = (lista_max[0][0] + 1)

  # Defina seus valores de inserção
  valores_inserir = (maquina, setor, risco, problemaaparente, tipo_manutencao, solicitante, natureza, max_id_ordem, n_ordem, dataabertura)

  query_insert = """
      INSERT INTO tb_ordens
      (maquina, setor, risco, problemaaparente, tipo_manutencao, solicitante, natureza, id_ordem, n_ordem,dataabertura)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
  """

  # Executar a consulta de inserção com os valores
  cur.execute(query_insert, valores_inserir)

  # Certifique-se de cometer as alterações e fechar a conexão
  conn.commit()
  conn.close()

abertura_15_dias()
abertura_7_dias()