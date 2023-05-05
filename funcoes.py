
from datetime import datetime, date
from datetime import timedelta
import datetime
import numpy as np
from pandas.tseries.offsets import BDay
import pandas as pd

def gerador_de_semanas_informar_manutencao(grupo,codigo_maquina,maquina,classificacao,ultima_manutencao,periodicidade):

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
