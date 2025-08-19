import pandas as pd

try:
    # 1. Carregar os arquivos CSV com o delimitador ';'
    df_servicos = pd.read_csv('planilhas/Serviços-18-08-2025(SOLIC_SERVIÇO).csv', delimiter=';')
    df_ricardo = pd.read_csv('planilhas/RICARDOALMEIDA_1858_MANT ES_1853.csv', delimiter=';')

    # 2. Consolidar o arquivo RICARDOALMEIDA_1858_MANT ES_1853.csv
    # Agrupar pela coluna 'Solicitação' e somar 'Vl. Customedio'
    # Para as outras colunas, vamos pegar o primeiro valor de cada grupo.
    # É importante ter certeza que 'Empresa' e 'Data' são consistentes para a mesma solicitação.

    # Limpar a coluna 'Vl. Customedio' antes de somar (remover 'R$', espaços, e substituir vírgulas por pontos)
    df_ricardo['Vl. Customedio'] = df_ricardo['Vl. Customedio'].astype(str).str.replace(r'R\$', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
    df_ricardo['Vl. Customedio'] = pd.to_numeric(df_ricardo['Vl. Customedio'], errors='coerce')

    # Agrupar por 'Solicitação' e consolidar os dados
    df_ricardo_consolidado = df_ricardo.groupby('Solicitação').agg(
        Empresa=('Empresa', 'first'),
        Data=('Data', 'first'),
        Vl_Customedio_Total=('Vl. Customedio', 'sum')
    ).reset_index()

    print("Arquivo RICARDOALMEIDA consolidado. Duplicadas de solicitação foram somadas.")
    print("-"*50)

    # 3. Fazer a junção (merge) dos datasgframes usando um 'outer join'.
    # Isso garante que todas as solcitações de ambos os arquivos estejam no resultado.
    # O 'left_on' e 'right_on' são usados caso os nomes das colunas de junção sejam diferentes.
    # No caso, ambos se chamam 'Solicitação', mas é uma boa prática.
    df_final = pd.merge(df_ricardo_consolidado, df_servicos,
                        left_on='Solicitação',
                        right_on='Solicitação',
                        how='outer',
                        suffixes=('_ricardo', '_servicos'))

    # 4. Selecionar e renomear as colunas para o resultado final.
    # Aqui, garantimos que as colunas vêm dos arquivos corretos e estão na ordem certa.
    # Como as colunas 'Solicitação' e 'Data' podem ser de ambos os arquivos após o 'merge',
    # vamos preferir as do arquivo RICARDOALMEIDA (que contém todas as solicitações).

    # Cria uma lista com os nomes de colunas na ordem desejada
    colunas_finais = [
        'Empresa',
        'Tipo',
        'Serviço',
        'Razão Social',
        'NF',
        'Vl_Customedio_Total',
        'Solicitação',
        'Data',
        'Natureza da Solicitação'
    ]

    # Seleciona as colunas finais na ordem correta
    df_final = df_final[colunas_finais]

    # 5. Salvar o resultado em um novo arquivo CSV.
    df_final.to_csv('Analise_Final_Servicos_1853.csv', index=False, sep=';')

    print("Processo concluído com sucesso!")
    print("O resultado foi salvo no arquivo 'Analise_Final_Servicos_1853.csv'.")

except FileNotFoundError:
    print("Erro: Verifique se os nomes dos arquivos estão corretos e se estão na mesma pasta do script.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
