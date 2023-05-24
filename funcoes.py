from datetime import datetime, date
from datetime import timedelta
import datetime
import numpy as np
from pandas.tseries.offsets import BDay
import pandas as pd
from flask import redirect, url_for, session
from functools import wraps
import psycopg2
import psycopg2.extras

DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

def gerador_de_semanas_informar_manutencao(grupo,codigo_maquina,maquina,classificacao,ultima_manutencao,periodicidade): # Função para gerar as 52 semanas

    lista_campos = []

    lista_campos.append([codigo_maquina,grupo,maquina,classificacao,ultima_manutencao,periodicidade])

    df_maquinas = pd.DataFrame(data = lista_campos, columns=['Código da máquina', 'Grupo','Descrição da máquina','Classificação','Última Manutenção','Periodicidade'])
    
    # Converte a coluna de data para o tipo datetime
    df_maquinas['Última Manutenção'] = pd.to_datetime(df_maquinas['Última Manutenção'])
    
    # Cria um DataFrame vazio para armazenar o planejamento de manutenção
    df_manutencao = pd.DataFrame(columns=['Última Manutenção', 'Código da máquina'])
    manut_outras_maquinas = False

    # Loop pelas máquinas
    for i, row in df_maquinas.iterrows():
        # Define as variáveis da máquina atual
        periodicidade = row['Periodicidade']
        
        if periodicidade == 'Quinzenal':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            
            semana_inicial = primeira_manutencao.isocalendar().week
            data_manutencao = primeira_manutencao + 14 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                                                    'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                    
                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 14 * BDay()
        
        if periodicidade == 'Bimestral':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            
            semana_inicial = primeira_manutencao.isocalendar().week
            data_manutencao = primeira_manutencao + 39 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                                                    'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                                        
                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 39 * BDay()
                
        if periodicidade == 'Semanal':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            
            semana_inicial = primeira_manutencao.isocalendar().week
            data_manutencao = primeira_manutencao + 6 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                                                    'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                                        
                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 6 * BDay()

        if periodicidade == 'Semestral':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            
            semana_inicial = primeira_manutencao.isocalendar().week
            data_manutencao = primeira_manutencao + 180 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                                                    'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                                        
                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 180 * BDay()

    #df_manutencao['Última Manutenção'] = pd.to_datetime(df_maquinas['Última Manutenção'])

    df_manutencao['Week_Number'] = df_manutencao['Última Manutenção'].dt.isocalendar().week

    df_manutencao['year'] = df_manutencao['Última Manutenção'].dt.isocalendar().year
    
    df_manutencao['Week_Number'] = df_manutencao['Week_Number'].astype(int) 
    
    df_manutencao = df_manutencao.loc[(df_manutencao['year'] == 2023)] 
    
    df_manutencao['Última Manutenção'] = df_manutencao['Última Manutenção'].dt.strftime("%d-%m-%Y") 
    
    ############### 52 semanas ##################
    
    lista_maq = df_manutencao['Código da máquina'].unique()
    
    df_filter = df_manutencao.loc[(df_manutencao['Código da máquina'] == lista_maq[i])] 
    
    df_vazio = pd.DataFrame()
    
    list_52 = ['Código da máquina','Grupo', 'Descrição da máquina','Classificação', 'Periodicidade','Última manutenção']
    
    for li in range(1,53):
        list_52.append(li)
        
    index = 0
    
    df_vazio = pd.DataFrame()
    
    for i in range(len(lista_maq)):
            
        df_52semanas = pd.DataFrame(columns=list_52, index=[index]) 
        df_filter = df_manutencao.loc[(df_manutencao['Código da máquina'] == lista_maq[i])] 
        df_filter = df_filter.reset_index(drop=True)
        df_52semanas['Código da máquina'] = df_filter['Código da máquina'][i]
        df_52semanas['Descrição da máquina'] = df_filter['Descrição da máquina'][i]
        df_52semanas['Periodicidade'] = df_filter['Periodicidade'][i]
        df_52semanas['Classificação'] = df_filter['Classificação'][i]
        df_52semanas['Grupo'] = df_filter['Grupo'][i]
        df_52semanas['Última manutenção'] = df_filter['primeira_manutencao'][i]
        
        index = index + 1
        
        for k in range(len(df_filter)):
            number_week = df_filter['Week_Number'][k]
            df_52semanas[number_week] = df_filter['Última Manutenção'][k]
            
        df_vazio = df_vazio.append(df_52semanas) 
    
    df_vazio = df_vazio.replace(np.nan, '')
    
    return df_vazio

def login_required(func): # Lógica do parâmetro de login_required, onde escolhe quais páginas onde apenas o usuário logado pode acessar
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'loggedin' not in session:
            return redirect(url_for('login.login'))
        return func(*args, **kwargs)
    return wrapper

def trigger_ordem_planejada():

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

    s = (""" SELECT * FROM tb_maquinas """)
    df_maquinas = pd.read_sql_query(s, conn)

    df_final = pd.DataFrame()

    for i in range(len(df_maquinas)):    
        try:
            df_planejamento = gerador_de_semanas_informar_manutencao(df_maquinas['setor'][i], df_maquinas['codigo'][i],
                                                                      df_maquinas['descricao'][i], df_maquinas['criticidade'][i],
                                                                        df_maquinas['manut_inicial'][i], df_maquinas['periodicidade'][i])
            df_final = pd.concat([df_final, df_planejamento], axis=0)
        except:
            pass

    # Obtendo a data atual
    data_atual = datetime.date.today()

    # Obtendo o número da semana atual
    numero_semana = data_atual.isocalendar()[1] + 1    
    
    df_final = df_final[['Código da máquina','Grupo', 'Descrição da máquina','Classificação', 'Periodicidade','Última manutenção',numero_semana]]
    df_final = df_final[df_final[numero_semana] != ''].reset_index(drop=True)

    for i in range(len(df_final)):
        
        if df_final['Classificação'][i] == 'A':
           df_final['Classificação'][i] = 'Alto'
        elif df_final['Classificação'][i] == 'B':
            df_final['Classificação'][i] = 'Médio'
        elif df_final['Classificação'][i] == 'C':
            df_final['Classificação'][i] = 'Baixo'
    
    df_final['Grupo'] = df_final['Grupo'].str.title()

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    n_ordem = 0
    problemaaparente = 'Manutenção Planejada'
    status = 'Em espera'
    natureza = 'Planejada'

    s = """SELECT max(id), max(id_ordem) FROM tb_ordens"""
    cur.execute(s)
    data = cur.fetchall()
    max_id = data[0][0] + 1
    max_ordem = data[0][1] + 1

    for i in range(len(df_final)):

        sql = """ INSERT INTO tb_ordens (id,setor, maquina, risco, status, problemaaparente, id_ordem, n_ordem, dataabertura, natureza) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        
        df_final[numero_semana][i] = pd.to_datetime(df_final[numero_semana][i], format='%d-%m-%Y').strftime("%Y-%m-%d")

        values = (max_id, df_final['Grupo'][i], df_final['Código da máquina'][i], df_final['Classificação'][i], status, problemaaparente,max_ordem,n_ordem, df_final[numero_semana][i] , natureza)
        
        max_ordem = max_ordem + 1
        max_id = max_id + 1

        cur.execute(sql, values)    

    
    conn.commit()

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
    df_agrupado = df_timeline.groupby('datafim')['diferenca'].sum()

    df_timeline = df_timeline.values.tolist()

    return df_timeline
