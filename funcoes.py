from datetime import datetime, date
from datetime import timedelta
import datetime
import numpy as np
from pandas.tseries.offsets import BDay
import pandas as pd
from flask import redirect, url_for, session
from functools import wraps
import psycopg2

DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"

def gerador_de_semanas_informar_manutencao(grupo,codigo_maquina,maquina,classificacao,ultima_manutencao,periodicidade): # Função para gerar as 52 semanas

    # grupo = 'CARPINTARIA'
    # codigo_maquina = 'auqlauqw'
    # maquina = 'qauqql'
    # classificacao = 'A'
    # ultima_manutencao = '2023/03/07'
    # periodicidade = 'Quinzenal'

    lista_campos = []

    lista_campos.append([grupo,codigo_maquina,maquina,classificacao,ultima_manutencao,periodicidade])

    df_maquinas = pd.DataFrame(data = lista_campos, columns=['Grupo','Código da máquina','Descrição da máquina','Classificação','Última Manutenção','Periodicidade'])
    
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
    
    list_52 = ['Grupo', 'Código da máquina', 'Descrição da máquina','Classificação', 'Periodicidade','Última manutenção']
    
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

# def trigger_ordem_planejada():

#     conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

#     s = (""" SELECT * FROM tb_maquinas """)
#     df_maquinas = pd.read_sql_query(s, conn)
    
#     df_final = pd.DataFrame()

#     for i in range(len(df_maquinas)):    
        
#         df_planejamento = gerador_de_semanas_informar_manutencao(df_maquinas['setor'][i], df_maquinas['codigo'][i], df_maquinas['descricao'][i], df_maquinas['criticidade'][i], df_maquinas['manut_inicial'][i], df_maquinas['periodicidade'][i])
#         df_final = pd.concat([df_final, df_planejamento], axis=0)

#     df_final = df_final.replace('','-')

#     data = date.today().strftime(format="%d/%m/%Y")
#     data = pd.to_datetime(data, format="%d/%m/%Y") #+ timedelta(3) # rodar na sexta porém com data segunda-feira
    
#     lista_indices = []

#     for i in range(1,7):
#         indices = []
#         for coluna in table.columns:
#             data_str = data.strftime(format='%d-%m-%Y')
#             idx = table.index[table[coluna] == data_str].tolist()
#             if len(idx) > 0:
#                  semana = coluna
#             indices.extend([(i) for i in idx])
#             #colunas.extend([(i,coluna) for i in idx])

#         lista_indices.append(indices)
#         #lista_colunas.append(colunas)
#         #semana = lista_colunas[1][1][1]

#         data = data + timedelta(1)
    
#     maquinas_merge = pd.DataFrame()
    
#     for j in range(6):

#         if len(table.iloc[lista_indices[j]]) > 0:

#             # DataFrame com informações das máquinas

#             table_maquinas = table.iloc[lista_indices[j]].reset_index(drop=True).sort_values(by='Classificação')

#             maquinas = pd.DataFrame(table_maquinas['Código da máquina'].copy())

#             media_tempos = table3[['Código da máquina','Criticidade','tempo de manutencao']]
#             media_tempos = pd.DataFrame(media_tempos.groupby(['Código da máquina','Criticidade']).mean()).reset_index()
#             media_tempos['Data real'] = semana
#             i = i-1
            
#             maquinas = maquinas.merge(media_tempos)

#             maquinas_merge = maquinas_merge.append(maquinas).reset_index(drop=True)

#         else:
#             pass

#     # Criando as listas de equipamentos e tempos

#     equipamentos = maquinas_merge['Código da máquina'].values.tolist()
#     tempos = maquinas_merge['tempo de manutencao'].values.tolist()
    
#     df_planejamento = pd.DataFrame(data=equipamentos, columns=['Código da máquina'])
#     df_planejamento['Dia'] = ''

#     data = date.today().strftime(format="%d/%m/%Y")
#     data = pd.to_datetime(data, format="%d/%m/%Y")# + timedelta(3) # data de hoje (segunda-feira)
    
#     # Criando a variável de tempo máximo
#     tempo_maximo = 180

#     # Criando o laço de repetição
#     for i in range(len(equipamentos)):
#         # Verificando se o tempo do equipamento cabe no dia atual
#         if tempos[i] <= tempo_maximo:

#             tempo_maximo = tempo_maximo - tempos[i]

#         else:
#             # Incrementando o dia atual
#             data = data + 1 * BDay()
#             tempo_maximo = 180 - tempos[i]

#         df_planejamento['Dia'][i] = data
    
#     df_planejamento = df_planejamento[['Dia','Código da máquina']]
#     df_planejamento = df_planejamento.merge(maquinas_merge, how='inner')
    
#     df_planejamento['Dia'] = pd.to_datetime(df_planejamento['Dia'])
#     df_planejamento['Dia'] = df_planejamento['Dia'].dt.strftime("%d/%m/%Y")

#     df_planejamento['Data real'] = semana
#     df_planejamento['Data real'] = semana

#     table = table[['Código da máquina', 'Descrição da máquina', 'Setor']]
#     df_planejamento = df_planejamento.merge(table)

#     df_planejamento = df_planejamento[['Data real','Dia','Código da máquina', 'Descrição da máquina', 'Setor', 'Criticidade', 'tempo de manutencao']]










