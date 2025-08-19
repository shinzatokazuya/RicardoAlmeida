import pandas as pd

try:
    # 1. Carregar os arquivos CSV com o delimitador ';'
    df_servicos = pd.read_csv('Serviços-18-08-2025(SOLIC_SERVIÇO).csv', delimiter=';')
    df_ricardo = pd.read_csv('RICARDOALMEIDA_1858_MANT ES_1410.csv', delimiter=';')

    # 2. Tratar solicitações duplicadas no arquivo do Ricardo.
    # Usamos o keep=False para marcar todas as ocorrências de um duplicado como True.
    solicitacoes_duplicadas = df_ricardo[df_ricardo.duplicated(subset=['Solicitação'], keep=False)]['Solicitação'].unique()

    # Criar uma nova tabela de Ricardo sem as solicitações duplicadas.
    df_ricardo_sem_duplicadas = df_ricardo[~df_ricardo['Solicitação'].isin(solicitacoes_duplicadas)]

    if len(solicitacoes_duplicadas) > 0:
        print(f"Atenção: As seguintes solicitações foram ignoradas por estarem duplicadas no arquivo RICARDOALMEIDA:")
        print(solicitacoes_duplicadas)
        print("-" * 50)

    # 3. Selecionar as colunas desejadas de cada arquivo antes de fazer a junção.
    # Isso otimiza o uso de memória e evita que colunas desnecessárias sejam processadas.

    # Colunas a serem extraídas do arquivo de Serviços
    colunas_servicos = ['Tipo ', 'Serviço ', 'Razão Social', 'NF ', 'Natureza da Solicitação ', 'Solicitação']
    df_servicos_selecionado = df_servicos[colunas_servicos]

    # Colunas a serem extraídas do arquivo do Ricardo
    colunas_ricardo = ['Empresa', 'Data', 'Vl. Customedio', 'Solicitação']
    df_ricardo_selecionado = df_ricardo_sem_duplicadas[colunas_ricardo]

    # 4. Fazer a junção (merge) dos dataframes.
    df_final = pd.merge(df_servicos_selecionado, df_ricardo_selecionado, on='Solicitação', how='left')

    # 5. Renomear as colunas para remover espaços em branco e garantir consistência, se necessário.
    df_final = df_final.rename(columns={'Tipo ': 'Tipo',
                                        'Serviço ': 'Serviço',
                                        'NF ': 'NF',
                                        'Natureza da Solicitação ': 'Natureza da Solicitação'})

    # 6. Reordenar as colunas na ordem final desejada.
    colunas_finais = ['Empresa', 'Tipo', 'Serviço', 'Razão Social', 'NF', 'Vl. Customedio', 'Solicitação', 'Data', 'Natureza da Solicitação']
    df_final = df_final[colunas_finais]

    # 7. Salvar o resultado em um novo arquivo CSV.
    df_final.to_csv('Analise_Final_Servicos.csv', index=False, sep=';')

    print("Processo concluído com sucesso!")
    print("O resultado foi salvo no arquivo 'Analise_Final_Servicos.csv'.")

except FileNotFoundError:
    print("Erro: Verifique se os nomes dos arquivos estão corretos e se estão na mesma pasta do script.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
