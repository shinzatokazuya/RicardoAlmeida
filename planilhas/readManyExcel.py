import pandas as pd
import os
import glob

try:
    # 1. Carregar o arquivo de serviços
    df_servicos = pd.read_csv('planilhas/Serviços-18-08-2025(SOLIC_SERVIÇO).csv', delimiter=';')

    # 2. Ler e juntar todos os arquivos de usuários do RICARDOALMEIDA

    # Define o padrão de nome de arquivo para encontrar todos os arquivos dos usuários
    # (por exemplo, "RICARDOALMEIDA_*.csv")
    padrao_arquivo = 'planilhas/RICARDOALMEIDA_1858_MANT ES_18*.csv'

    # Use glob para encontrar todos os arquivos que correspondem ao padrão na pasta
    arquivos_ricardo = glob.glob(padrao_arquivo)

    # Verifica se encontrou arquivos
    if not arquivos_ricardo:
        print(f"Erro: Nenhum arquivo encontrado com o padrão '{padrao_arquivo}'.")
    else:
        print(f"Arquivos encontrados: {arquivos_ricardo}")

        # Cria uma lista vazia para armazenar os dataframes de cada arquivo
        lista_dataframes = []

        # Loop para ler cada arquivo e adicionar à lista
        for arquivo in arquivos_ricardo:
            print(f"Lendo o arquivo: {arquivo}")
            df_temp = pd.read_csv(arquivo, delimiter=';')
            lista_dataframes.append(df_temp)

        # Concatena todos os dataframes em um único dataframe grande
        df_ricardo_todos = pd.concat(lista_dataframes, ignore_index=True)

        # 3. Limpar e consolidar o dataframe unificado

        # Limpar a coluna 'Vl. Customedio'
        df_ricardo_todos['Vl. Customedio'] = df_ricardo_todos['Vl. Customedio'].astype(str).str.replace(r'R\$', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
        df_ricardo_todos['Vl. Customedio'] = pd.to_numeric(df_ricardo_todos['Vl. Customedio'], errors='coerce')

        # Agrupar por 'Solicitação' e consolidar os dados
        df_ricardo_consolidado = df_ricardo_todos.groupby('Solicitação').agg(
            Empresa=('Empresa', 'first'),
            Data=('Data', 'first'),
            Vl_Customedio_Total=('Vl. Customedio', 'sum')
        ).reset_index()

        print("-" * 50)
        print("Todos os arquivos RICARDOALMEIDA foram consolidados e somados.")
        print("-" * 50)

        # 4. Fazer a junção (merge) dos dataframes usando um 'outer join'
        # Isso garante que todas as solicitações de ambos os arquivos estejam no resultado.
        df_final = pd.merge(df_ricardo_consolidado, df_servicos,
                            left_on='Solicitação',
                            right_on='Solicitação',
                            how='outer',
                            suffixes=('_ricardo', '_servicos'))

        # 5. Selecionar e renomear as colunas para o resultado final.
        colunas_finais = [
            'Empresa',
            'Tipo',
            'Serviço',
            'Razão Social',
            'NF',
            'Vl_Customedio_Total',
            'Solicitação',
            'Data',
            'Natureza da Solicitação',
            'Usuário'
        ]

        df_final = df_final[colunas_finais]

        # 6. Salvar o resultado em um novo arquivo CSV.
        df_final.to_csv('Analise_Consolidada_Multiplos_Usuarios.csv', index=False, sep=';')

        print("Processo concluído. O arquivo de saída contém todas as solicitações consolidadas.")
        print("O resultado foi salvo no arquivo 'Analise_Consolidada_Multiplos_Usuarios.csv'.")

except FileNotFoundError:
    print("Erro: Verifique se os nomes dos arquivos e o padrão de busca estão corretos.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
