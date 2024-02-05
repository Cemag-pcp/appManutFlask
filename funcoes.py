from datetime import datetime, date
from datetime import timedelta
import numpy as np
from pandas.tseries.offsets import BDay
import pandas as pd
from flask import redirect, url_for, session
from functools import wraps
import psycopg2
import psycopg2.extras
from psycopg2 import Error

DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

def gerador_de_semanas_informar_manutencao(grupo,codigo_maquina,maquina,tombamento,classificacao,ultima_manutencao,periodicidade):
    
    lista_campos = []

    lista_campos.append([codigo_maquina,grupo,maquina,tombamento,classificacao,ultima_manutencao,periodicidade])

    df_maquinas = pd.DataFrame(data = lista_campos, columns=['Código da máquina', 'Grupo','Descrição da máquina','Tombamento','Classificação','Última Manutenção','Periodicidade'])
    
    # codigo_maquina = 'ABC'
    # grupo = 'ABC'
    # maquina = 'ABC'
    # classificacao = 'A'
    # ultima_manutencao = '2023-06-12'
    # periodicidade = 'Quinzenal'

    # Converte a coluna de data para o tipo datetime
    df_maquinas['Última Manutenção'] = pd.to_datetime(df_maquinas['Última Manutenção'])
    
    # Cria um DataFrame vazio para armazenar o planejamento de manutenção
    df_manutencao = pd.DataFrame(columns=['Última Manutenção', 'Código da máquina'])
    manut_outras_maquinas = False

    # Loop pelas máquinas
    for i, row in df_maquinas.iterrows():
        # Define as variáveis da máquina atual
        periodicidade = row['Periodicidade']

        if periodicidade == 'Mensal':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 29 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                
                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)

                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 29 * BDay()
 
        if periodicidade == 'Quinzenal':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 14 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                
                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)

                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 14 * BDay()
        
        if periodicidade == 'Bimestral':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 39 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                
                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)

                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 39 * BDay()
                
        if periodicidade == 'Semanal':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 6 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)

                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)                      

                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 6 * BDay()

        if periodicidade == 'Semestral':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 180 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)

                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)                     

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
    
    list_52 = ['Código da máquina','Tombamento','Grupo', 'Descrição da máquina','Classificação', 'Periodicidade','Última manutenção']
    
    for li in range(1,53):
        list_52.append(li)
        
    index = 0
    
    df_vazio = pd.DataFrame()
    
    for i in range(len(lista_maq)):
            
        df_52semanas = pd.DataFrame(columns=list_52, index=[index]) 
        df_filter = df_manutencao.loc[(df_manutencao['Código da máquina'] == lista_maq[i])] 
        df_filter = df_filter.reset_index(drop=True)
        df_52semanas['Código da máquina'] = df_filter['Código da máquina'][i]
        df_52semanas['Tombamento'] = df_filter['Tombamento'][i]
        df_52semanas['Descrição da máquina'] = df_filter['Descrição da máquina'][i]
        df_52semanas['Periodicidade'] = df_filter['Periodicidade'][i]
        df_52semanas['Classificação'] = df_filter['Classificação'][i]
        df_52semanas['Grupo'] = df_filter['Grupo'][i]
        df_52semanas['Última manutenção'] = df_filter['primeira_manutencao'][i]

        index = index + 1
        
        for k in range(len(df_filter)):
            number_week = df_filter['Week_Number'][k]
            df_52semanas[number_week] = df_filter['Última Manutenção'][k]
            
        df_vazio = pd.concat([df_vazio, df_52semanas], ignore_index=True)
    
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
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    s = (""" SELECT * FROM tb_maquinas_preventivas """)
    df_final = pd.read_sql_query(s, conn)
    
    # Obtendo a data atual
    data_atual = datetime.date.today()

    # Obtendo o número da semana atual
    numero_semana = str(data_atual.isocalendar()[1])    
    
    df_final = df_final[['codigo','tombamento', 'setor','descricao', 'classificacao','periodicidade','ultima_manutencao',numero_semana]]
    df_final = df_final[df_final[numero_semana] != '-'].reset_index(drop=True)
    df_final[numero_semana] = pd.to_datetime(df_final[numero_semana])
    df_final = df_final.dropna().reset_index(drop=True)

    for i in range(len(df_final)):
        
        if df_final['classificacao'][i] == 'A':
           df_final['classificacao'][i] = 'Alto'
        elif df_final['classificacao'][i] == 'B':
            df_final['classificacao'][i] = 'Médio'
        elif df_final['classificacao'][i] == 'C':
            df_final['classificacao'][i] = 'Baixo'
    
    df_final['setor'] = df_final['setor'].str.title()

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
        
        df_final = df_final.dropna().reset_index(drop=True)
        df_final[numero_semana][i] = pd.to_datetime(df_final[numero_semana][i], format='%d-%m-%Y').strftime("%Y-%m-%d")

        values = (max_id, df_final['setor'][i], df_final['codigo'][i], df_final['classificacao'][i], status, problemaaparente,max_ordem,n_ordem, df_final[numero_semana][i] , natureza)
        
        max_ordem = max_ordem + 1
        max_id = max_id + 1

        cur.execute(sql, values)    

    conn.commit()
    conn.close()

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

def gerador_de_semanas_informar_manutencao_diario(grupo,codigo_maquina,maquina,tombamento,classificacao,ultima_manutencao,periodicidade):
    
    lista_campos = []

    lista_campos.append([codigo_maquina,grupo,maquina,tombamento,classificacao,ultima_manutencao,periodicidade])

    df_maquinas = pd.DataFrame(data = lista_campos, columns=['Código da máquina', 'Grupo','Descrição da máquina','Tombamento','Classificação','Última Manutenção','Periodicidade'])
    
    # codigo_maquina = 'ABC'
    # grupo = 'ABC'
    # maquina = 'ABC'
    # classificacao = 'A'
    # ultima_manutencao = '2023-06-12'
    # periodicidade = 'Quinzenal'

    # Converte a coluna de data para o tipo datetime
    df_maquinas['Última Manutenção'] = pd.to_datetime(df_maquinas['Última Manutenção'])
    
    # Cria um DataFrame vazio para armazenar o planejamento de manutenção
    df_manutencao = pd.DataFrame(columns=['Última Manutenção', 'Código da máquina'])
    manut_outras_maquinas = False

    # Loop pelas máquinas
    for i, row in df_maquinas.iterrows():
        # Define as variáveis da máquina atual
        periodicidade = row['Periodicidade']

        if periodicidade == 'Diaria':
            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 1 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)
                

                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)

                # Avança para a próxima data de manutenção
                data_manutencao = data_manutencao + 1 * BDay()

            nome_maquina = row['Código da máquina']
            desc_maquina = row['Descrição da máquina']
            classificacao = row['Classificação']
            primeira_manutencao = row['Última Manutenção']
            periodicidade = row['Periodicidade']
            grupo = row['Grupo']
            tombamento = row['Tombamento']
            
            semana_inicial = int(primeira_manutencao.strftime("%V"))
            data_manutencao = primeira_manutencao + 180 * BDay()
            
            # Loop pelas semanas
            for j in range(52-semana_inicial):
                # Se a data de manutenção cair em um final de semana, avança para a segunda-feira seguinte
                while data_manutencao.weekday() in [5, 6]:
                    data_manutencao = data_manutencao + 1 * BDay()
        
                # df_manutencao = df_manutencao.append({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"), 'Última Manutenção': data_manutencao,'Código da máquina': nome_maquina,
                #                                     'Descrição da máquina': desc_maquina,'Periodicidade': periodicidade, 'Grupo': grupo, 'Classificação': classificacao},ignore_index=True)

                df_new_row = pd.DataFrame({'primeira_manutencao': primeira_manutencao.strftime("%d/%m/%Y"),
                           'Última Manutenção': data_manutencao,
                           'Código da máquina': nome_maquina,
                           'Tombamento': tombamento,                           
                           'Descrição da máquina': desc_maquina,
                           'Periodicidade': periodicidade,
                           'Grupo': grupo,
                           'Classificação': classificacao},
                            index=[0])

                df_manutencao = pd.concat([df_manutencao, df_new_row], ignore_index=True)                     

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
    
    list_52 = ['Código da máquina','Tombamento','Grupo', 'Descrição da máquina','Classificação', 'Periodicidade','Última manutenção']
    
    for li in range(1,53):
        list_52.append(li)
        
    index = 0
    
    df_vazio = pd.DataFrame()
    
    for i in range(len(lista_maq)):
            
        df_52semanas = pd.DataFrame(columns=list_52, index=[index]) 
        df_filter = df_manutencao.loc[(df_manutencao['Código da máquina'] == lista_maq[i])] 
        df_filter = df_filter.reset_index(drop=True)
        df_52semanas['Código da máquina'] = df_filter['Código da máquina'][i]
        df_52semanas['Tombamento'] = df_filter['Tombamento'][i]
        df_52semanas['Descrição da máquina'] = df_filter['Descrição da máquina'][i]
        df_52semanas['Periodicidade'] = df_filter['Periodicidade'][i]
        df_52semanas['Classificação'] = df_filter['Classificação'][i]
        df_52semanas['Grupo'] = df_filter['Grupo'][i]
        df_52semanas['Última manutenção'] = df_filter['primeira_manutencao'][i]

        index = index + 1
        
        for k in range(len(df_filter)):
            number_week = df_filter['Week_Number'][k]
            df_52semanas[number_week] = df_filter['Última Manutenção'][k]
            
        df_vazio = pd.concat([df_vazio, df_52semanas], ignore_index=True)
    
    df_vazio = df_vazio.replace(np.nan, '')
    
    return df_vazio

# import pandas as pd
# from datetime import timedelta

# # Supondo que você tenha um DataFrame com as informações
# # Substitua isso pelos seus dados reais
# data = {
#     'codigo': [1],
#     'periodicidade': [2],  # Exemplo de periodicidade em meses
#     'data_ultima_manutencao': ['2023-01-01']
# }

# df = pd.DataFrame(data)
# df['data_ultima_manutencao'] = pd.to_datetime(df['data_ultima_manutencao'])

# # Inicializa uma lista para armazenar as datas planejadas
# datas_planejadas = []

# Define a função para calcular a próxima data ajustada

def calcular_proxima_data(data_atual, periodicidade_em_dias):

    dias_uteis = pd.offsets.BDay(round(periodicidade_em_dias))  # Considera dias úteis (BDay)
    proxima_data = data_atual + dias_uteis
    return proxima_data.strftime("%Y-%m-%d") 

def calcular_planejamento_anual(data_atual, periodicidade_em_dias):
    planejamento_anual = []
    semanas = []
    for _ in range(52):  # Iterar sobre os meses do ano
        data_atual = pd.to_datetime(data_atual)  # Garantir que a data atual é do tipo datetime
        proxima_data = calcular_proxima_data(data_atual, periodicidade_em_dias)
        if pd.to_datetime(proxima_data).year == datetime.now().year:
            planejamento_anual.append(proxima_data)
            semanas.append(pd.to_datetime(proxima_data).weekofyear)
            data_atual = proxima_data  # Atualizar a data atual para a próxima iteração
        else:
            continue

    return planejamento_anual,semanas

def gerar_planejamento_maquinas_preventivas(codigo_maquina,grupo,maquina,tombamento,classificacao,ultima_manutencao,periodicidade):

    # codigo_maquina = 'ABC'
    # grupo = 'ABC'
    # maquina = 'ABC'
    # classificacao = 'A'
    # ultima_manutencao = '2023-06-12'
    # periodicidade = 2
    # tombamento = 'teste'

    data = {
        'codigo': pd.Series(codigo_maquina, index=[0]),
        'periodicidade': pd.Series(periodicidade, index=[0]),
        'ultima_manutencao': pd.Series(ultima_manutencao, index=[0]),
        'maquina': pd.Series(maquina, index=[0]),
        'tombamento': pd.Series(tombamento, index=[0]),
        'classificacao': pd.Series(classificacao, index=[0]),
        'setor': pd.Series(grupo, index=[0])
    }

    df = pd.DataFrame(data)
    df['ultima_manutencao'] = pd.to_datetime(df['ultima_manutencao'])

    # Inicializa uma lista para armazenar as datas planejadas
    datas_planejadas = []

    # Inicializar lista para armazenar as datas planejadas
    datas_planejadas = []

    # Iterar sobre os códigos
    for item in data['codigo']:
        
        # Iterar sobre as 52 semanas
        for semana in range(52):
            if semana == 0:
                data_atual = pd.to_datetime(df[df['codigo'] == item]['ultima_manutencao'].values[0])
            else:
                data_atual = datas_planejadas[-1]

            proxima_data_planejada = calcular_proxima_data(data_atual, int(df[df['codigo'] == item]['periodicidade'].values[0]))
            datas_planejadas.append(proxima_data_planejada)

    # Criar DataFrame com a coluna 'proxima_manutencao'
    df_resultado = pd.DataFrame({'proxima_manutencao': datas_planejadas})

    # Adicionar a coluna 'codigo' ao DataFrame resultado
    df_resultado['codigo'] = [item for item in data['codigo'] for _ in range(52)]
    df_resultado['tombamento'] = [item for item in data['tombamento'] for _ in range(52)]
    df_resultado['setor'] = [item for item in data['setor'] for _ in range(52)]
    df_resultado['maquina'] = [item for item in data['maquina'] for _ in range(52)]
    df_resultado['classificacao'] = [item for item in data['classificacao'] for _ in range(52)]
    df_resultado['periodicidade'] = [item for item in data['periodicidade'] for _ in range(52)]
    df_resultado['ultima_manutencao'] = [item for item in data['ultima_manutencao'] for _ in range(52)]

    # Reordenar as colunas se desejar
    df_resultado = df_resultado[['codigo','tombamento','setor','maquina','classificacao','periodicidade','ultima_manutencao','proxima_manutencao']]

    df_resultado = df_resultado[df_resultado['proxima_manutencao'] < '2025-01-01']

    df_resultado['ultima_manutencao'] = pd.to_datetime(df_resultado['ultima_manutencao'])

    return df_resultado

# # Exibir o DataFrame resultante
# df_resultado.to_csv('planejamento_anual.csv', index=False)




# # Cria um DataFrame com as datas planejadas
# df_planejamento = pd.DataFrame({'data_planejada': datas_planejadas})

# # Imprime o DataFrame resultante
# print(df_planejamento)



# conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
#                         password=DB_PASS, host=DB_HOST)

# cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# data = pd.read_sql_query(con=conn, sql='select * from tb_maquinas_preventivas')
# data.dtypes
# data = data[['codigo','tombamento','setor','descricao','classificacao','periodicidade','ultima_manutencao']]

# def periodicidade(row):
#     if row == 'Semanal':
#         return 7
#     elif row == 'Mensal':
#         return 30
#     elif row == 'Bimestral':
#         return 60
#     elif row == 'Quinzenal':
#         return 15
#     else:
#         return None
    
    

# data['periodicidade'] = data['periodicidade'].apply(periodicidade)



# data_antigo = data.to_csv()

