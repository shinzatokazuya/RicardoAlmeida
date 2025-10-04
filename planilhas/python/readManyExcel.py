import pandas as pd
import os
import glob

try:
    df_servicos = pd.read_csv('planilhas/csv/Solicitacoes_Geral_28-08-2025.csv', delimiter=';')
    padrao_arquivo = 'planilhas/csv/RICARDOALMEIDA*.csv'
    arquivos_ricardo = glob.glob(padrao_arquivo)

    if not arquivos_ricardo:
        print(f"Erro: Nenhum arquivo encontrado com o padrão '{padrao_arquivo}'.")
    else:
        print(f"Arquivos encontrados: {arquivos_ricardo}")
        lista_dataframes = []

        for arquivo in arquivos_ricardo:
            print(f"Lendo o arquivo: {arquivo}")
            df_temp = pd.read_csv(arquivo, delimiter=';')
            df_temp['Solicitação'] = pd.to_numeric(df_temp['Solicitação'], errors='coerce')
            df_temp['Vl.Solicitação'] = (
                df_temp['Vl.Solicitação']
                .astype(str)
                .str.replace(',', '.', regex=False)
                .str.strip()
            )
            df_temp['Vl.Solicitação'] = pd.to_numeric(df_temp['Vl.Solicitação'], errors='coerce')

            if 'RICARDOALMEIDA_1858_MANT ES_Geral_28-08-2025.csv' in arquivo:
                # Somar apenas até a solicitação 13229
                df_somar = df_temp[df_temp['Solicitação'] <= 13229]
                #df_nao_somar = df_temp[df_temp['Solicitação'] > 13229]
                # Agrupa e soma até 13229
                df_somadas = df_somar.groupby('Solicitação').agg(
                    Empresa=('Empresa', 'first'),
                    Data=('Data', 'first'),
                    Situacao=('Situação', 'first'),
                    Usuario=('Usuário', 'first'),
                    Nr_nf=('Nr. Nf', 'first'),
                    Ds_Obs_Cmc=('Ds. Obs Cmc', 'first'),
                    Descricao=('Descrição', 'first'),
                    Pedido=('Pedido', 'first'),
                    Data_Prev=('Dt. Preventrega', 'first'),
                    Ds_Compra=('Ds. Compra', 'first'),
                    Prioridade=('Ds. Prioridade', 'first'),
                    Vl_Solicitacao_Total=('Vl.Solicitação', 'sum')
                ).reset_index()
                # Junta as linhas não somadas
                lista_dataframes.append(df_somadas)
                df_somadas['Vl_Solicitacao_Total'] = df_somadas['Vl_Solicitacao_Total'].round(2)
                #lista_dataframes.append(df_nao_somar)
            elif 'RICARDOALMEIDA_1858_MANT ES_Geral_05-09-2025.csv' in arquivo:
                # Soma todo o arquivo
                df_somar = df_temp[df_temp['Solicitação'] <= 13354]
                df_somadas = df_somar.groupby('Solicitação').agg(
                    Empresa=('Empresa', 'first'),
                    Data=('Data', 'first'),
                    Situacao=('Situação', 'first'),
                    Usuario=('Usuário', 'first'),
                    Nr_nf=('Nr. Nf', 'first'),
                    Ds_Obs_Cmc=('Ds. Obs Cmc', 'first'),
                    Descricao=('Descrição', 'first'),
                    Pedido=('Pedido', 'first'),
                    Data_Prev=('Dt. Preventrega', 'first'),
                    Ds_Compra=('Ds. Compra', 'first'),
                    Prioridade=('Ds. Prioridade', 'first'),
                    Vl_Solicitacao_Total=('Vl.Solicitação', 'sum')
                ).reset_index()
                lista_dataframes.append(df_somadas)
                df_somadas['Vl_Solicitacao_Total'] = df_somadas['Vl_Solicitacao_Total'].round(2)
            elif 'RICARDOALMEIDA_1858_MANT ES_Geral_12-09-2025.csv' in arquivo:
                # Soma todo o arquivo
                df_somar = df_temp[df_temp['Solicitação'] <= 13659]
                df_somadas = df_somar.groupby('Solicitação').agg(
                    Empresa=('Empresa', 'first'),
                    Data=('Data', 'first'),
                    Situacao=('Situação', 'first'),
                    Usuario=('Usuário', 'first'),
                    Nr_nf=('Nr. Nf', 'first'),
                    Ds_Obs_Cmc=('Ds. Obs Cmc', 'first'),
                    Descricao=('Descrição', 'first'),
                    Pedido=('Pedido', 'first'),
                    Data_Prev=('Dt. Preventrega', 'first'),
                    Ds_Compra=('Ds. Compra', 'first'),
                    Prioridade=('Ds. Prioridade', 'first'),
                    Vl_Solicitacao_Total=('Vl.Solicitação', 'sum')
                ).reset_index()
                lista_dataframes.append(df_somadas)
                df_somadas['Vl_Solicitacao_Total'] = df_somadas['Vl_Solicitacao_Total'].round(2)
            elif 'RICARDOALMEIDA_1858_MANT ES_Geral_19-09-2025.csv' in arquivo:
                # Soma todo o arquivo
                # df_somar = df_temp[df_temp['Solicitação'] <= 13659]
                df_somadas = df_temp.groupby('Solicitação').agg(
                    Empresa=('Empresa', 'first'),
                    Data=('Data', 'first'),
                    Situacao=('Situação', 'first'),
                    Usuario=('Usuário', 'first'),
                    Nr_nf=('Nr. Nf', 'first'),
                    Ds_Obs_Cmc=('Ds. Obs Cmc', 'first'),
                    Descricao=('Descrição', 'first'),
                    Pedido=('Pedido', 'first'),
                    Data_Prev=('Dt. Preventrega', 'first'),
                    Ds_Compra=('Ds. Compra', 'first'),
                    Prioridade=('Ds. Prioridade', 'first'),
                    Vl_Solicitacao_Total=('Vl.Solicitação', 'sum')
                ).reset_index()
                lista_dataframes.append(df_somadas)
                df_somadas['Vl_Solicitacao_Total'] = df_somadas['Vl_Solicitacao_Total'].round(2)
            else:
                # Para outros arquivos, apenas adiciona sem agrupar
                lista_dataframes.append(df_temp)

        df_ricardo_todos = pd.concat(lista_dataframes, ignore_index=True)

        # Merge e seleção de colunas igual ao seu código original
        df_final = pd.merge(df_ricardo_todos, df_servicos,
                            left_on='Solicitação',
                            right_on='Solicitação',
                            how='outer',
                            suffixes=('_ricardo', '_servicos'))

        colunas_finais = [
            'Empresa',
            'Tipo',
            'Servico',
            'ID_Prestador',
            'Nr_nf',
            'Ds_Obs_Cmc',
            'Obs',
            'Descricao',
            'Vl_Solicitacao_Total',
            'Solicitação',
            'Pedido',
            'Ds_Compra',
            'Prioridade',
            'Data',
            'Data_Prev',
            'Natureza da Solicitacao',
            'Usuario',
            'Situacao'
        ]
        df_final = df_final[colunas_finais]
        df_final.to_csv('planilhas/Solicitacoes_Geral_19-09-2025.csv', index=False, sep=';')
        print("Processo concluído. O resultado foi salvo no arquivo 'Solicitacoes_Geral_19-09-2025.csv'.")

except FileNotFoundError:
    print("Erro: Verifique se os nomes dos arquivos e o padrão de busca estão corretos.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
