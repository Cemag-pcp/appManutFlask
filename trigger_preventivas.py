import psycopg2  # pip install psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date,timedelta
from pandas.tseries.offsets import BMonthEnd,MonthEnd,BDay
import numpy as np


DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"


def geral():
    
    DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
    DB_NAME = "postgres"
    DB_USER = "postgres"
    DB_PASS = "15512332"

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    query = """select distinct *, periodicidade*30 as periodicidade_em_dias from tb_grupos_preventivas where ult_manutencao is not null"""

    cur.execute(query)
    data = cur.fetchall()

    df_data = pd.DataFrame(data)

    df_data['proxima_manutencao'] = ''

    for row in range(len(df_data)):
        
        ultima_manutencao = df_data[3][row]
        periodicidade = df_data[5][row]

        proxima_manutencao = proxima_data_util(ultima_manutencao, periodicidade)

        df_data['proxima_manutencao'][row] = proxima_manutencao

    hoje_string = date.today().strftime("%Y-%m-%d")

    proxima_manutencao_hoje = df_data[df_data['proxima_manutencao'] == hoje_string]
    proxima_manutencao_hoje = proxima_manutencao_hoje.drop_duplicates(subset=1)

    if len(proxima_manutencao_hoje) > 0:

        for index, row in proxima_manutencao_hoje.iterrows():
            maquina = row[1]
            grupo = row[2]
            atividades = buscar_atividades(maquina,grupo)
            criar_ordem(maquina, atividades)


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


def criar_ordem(maquina, atividades):

    """
    Função para criar ordem com base na data da preventiva
    """

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                            password=DB_PASS, host=DB_HOST)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql_maquina = f"""SELECT setor FROM tb_maquinas_preventivas WHERE codigo = '{maquina}'"""

    cur.execute(sql_maquina)
    data_maquinas = cur.fetchall()
    setor = data_maquinas[0][0]

    maquina = maquina
    setor = setor
    risco = 'Médio'
    problemaaparente = atividades
    tipo_manutencao = 'Preventiva'
    solicitante = 'Automático'
    natureza = 'Planejada'
    status = 'Em espera'
    n_ordem = 0

    cur.execute("select MAX(id_ordem), MAX(id) from tb_ordens")

    lista_max = cur.fetchall()
    max_id_ordem, max_id = (lista_max[0][0] + 1), (lista_max[0][1] + 1)

    # Defina seus valores de inserção
    valores_inserir = (maquina, setor, risco, problemaaparente, tipo_manutencao, solicitante, natureza, max_id_ordem, n_ordem, max_id)

    query_insert = """
        INSERT INTO tb_ordens
        (maquina, setor, risco, problemaaparente, tipo_manutencao, solicitante, natureza, id_ordem, n_ordem, id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Executar a consulta de inserção com os valores
    cur.execute(query_insert, valores_inserir)

    # Certifique-se de cometer as alterações e fechar a conexão
    conn.commit()
    conn.close()


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

  # data_inicial = date.fromisoformat(data_inicial)

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