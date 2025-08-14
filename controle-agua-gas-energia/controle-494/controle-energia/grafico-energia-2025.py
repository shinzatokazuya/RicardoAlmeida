import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import calendar # Para obter os nomes dos meses

# Caminho para o arquivo Excel
file_path = 'controle-agua-gas-energia/controle-494/controle-energia/Planilha-energia-atual.xlsx'

try:
    # --- Passo 1: Ler as linhas do cabeçalho separadamente ---
    df_all_headers = pd.read_excel(file_path, sheet_name='Controle Enel', header=None, nrows=3, skiprows=19)
    df_all_headers_filled = df_all_headers.ffill(axis=1)

    h0 = df_all_headers_filled.iloc[0].values
    h1 = df_all_headers_filled.iloc[1].values
    h2 = df_all_headers_filled.iloc[2].values

    # --- Passo 2: Construir o MultiIndex de colunas ---
    combined_header_tuples = []
    num_cols = len(h0)

    # Tratar as colunas 'DATA' e 'DIA DA SEM'
    combined_header_tuples.append(('', '', str(h2[0]).strip())) # DATA
    combined_header_tuples.append(('', '', str(h2[1]).strip())) # DIA DA SEM

    # Iterar pelas outras colunas
    for i in range(2, num_cols):
        top_level = h0[i]
        middle_level = h1[i]
        bottom_level = h2[i]

        if pd.isna(top_level) and i > 0:
            for j in range(i-1, -1, -1):
                if pd.notna(h0[j]):
                    top_level = h0[j]
                    break
        if pd.isna(middle_level) and i > 0:
            for j in range(i-1, -1, -1):
                if pd.notna(h1[j]) and str(h1[j]).strip() != '':
                    middle_level = h1[j]
                    break

        top_level = str(top_level).strip() if pd.notna(top_level) else ''
        middle_level = str(middle_level).strip() if pd.notna(middle_level) else ''
        bottom_level = str(bottom_level).strip() if pd.notna(bottom_level) else f"Unnamed_col_{i}"

        if top_level == 'nan' and middle_level == 'nan' and bottom_level.startswith('Unnamed'):
            continue

        if top_level == 'nan': top_level = ''
        if middle_level == 'nan': middle_level = ''

        combined_header_tuples.append((top_level, middle_level, bottom_level))

    new_cols = pd.MultiIndex.from_tuples(combined_header_tuples)

    # --- Passo 3: Ler os dados reais e atribuir o novo MultiIndex ---
    df = pd.read_excel(file_path, sheet_name='Controle Enel', header=None, skiprows=22)
    df.columns = new_cols
    df = df.iloc[:, :len(new_cols)]

    # A coluna 'DATA' é agora um MultiIndex com níveis vazios e o nome 'DATA' no terceiro nível.
    data_col_tuple = ('', '', 'DATA')

    # Converta a coluna 'DATA' para o tipo de dado datetime
    df[data_col_tuple] = pd.to_datetime(df[data_col_tuple], errors='coerce')
    df.dropna(subset=[data_col_tuple], inplace=True)

    # Filtrar dados para o ano de 2025
    df_2025_multiindex = df[df[data_col_tuple].dt.year == 2025].copy()

    # --- 1. Substituir NaN por '' para exibição (apenas na cópia para impressão) ---
    df_display = df_2025_multiindex.copy()
    df_display = df_display.fillna('')

    # --- Exibir o DataFrame com o MultiIndex de colunas e dados filtrados para 2025 ---
    print("\n--- DataFrame filtrado para 2025 com MultiIndex de colunas original (NaN como vazios) ---")
    print(df_display.head())
    print("...")
    print(df_display.tail())
    print("\nColunas do DataFrame (MultiIndex):")
    print(df_2025_multiindex.columns)
    print("-" * 50)

    # --- Preparação dos dados para os gráficos ---

    # Achar as colunas de "Consumo" e garantir que são numéricas
    # Excluindo as colunas de DATA e DIA DA SEM do MultiIndex para facilitar
    # a iteração pelos setores.
    setores_consumo = {}
    for col_tuple in df_2025_multiindex.columns:
        if 'Consumo' in col_tuple[-1] and col_tuple[0] not in ['', 'DATA', 'DIA DA SEM']:
            setor_nome = col_tuple[0]
            if setor_nome not in setores_consumo:
                setores_consumo[setor_nome] = []
            setores_consumo[setor_nome].append(col_tuple)

    # Converter todas as colunas de consumo para numérico (coerce para transformar erros em NaN)
    for setor, cols in setores_consumo.items():
        for col_tuple in cols:
            df_2025_multiindex[col_tuple] = pd.to_numeric(df_2025_multiindex[col_tuple], errors='coerce')

    # Definir 'DATA' como índice para facilitar o agrupamento por mês
    df_plot_base = df_2025_multiindex.set_index(data_col_tuple)

    # --- Gráfico de Média de Consumo Mensal Total (como antes, mas com nomes dos meses) ---
    df_plot_base['Consumo Total Diário'] = df_plot_base[[col for sublist in setores_consumo.values() for col in sublist]].sum(axis=1)
    consumo_mensal_medio_total = df_plot_base['Consumo Total Diário'].resample('ME').mean()

    plt.figure(figsize=(12, 7)) # Aumentei um pouco o tamanho para mais espaço
    ax_total = consumo_mensal_medio_total.plot(kind='bar', color='skyblue')
    plt.title('Média de Consumo Mensal Total - Energia (Todas as Áreas) - 2025')
    plt.xlabel('Mês')
    plt.ylabel('Média de Consumo Diário (Unidade)')

    # Definir os rótulos do eixo X para os nomes dos meses
    # O índice já está no formato de data, podemos pegar o nome do mês diretamente
    ax_total.set_xticklabels([idx.strftime('%B').capitalize() for idx in consumo_mensal_medio_total.index], rotation=45, ha='right')
    # Para Português:
    # ax_total.set_xticklabels([calendar.month_name[idx.month].capitalize() for idx in consumo_mensal_medio_total.index], rotation=45, ha='right')
    # Se quiser o nome do mês em português, a linha acima funciona, mas certifique-se que o locale do seu ambiente Python está configurado para pt_BR.
    # Alternativamente, você pode criar um dicionário de mapeamento:
    nomes_meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
        7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    ax_total.set_xticklabels([nomes_meses_pt[idx.month] for idx in consumo_mensal_medio_total.index], rotation=45, ha='right')


    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    output_image_path_total = 'grafico_consumo_mensal_total_2025.png'
    plt.savefig('controle-agua-gas-energia/controle-494/controle-energia/' + output_image_path_total)
    print(f"\nGráfico total salvo como: {output_image_path_total}")
    plt.show()


    # --- Gráficos de Média de Consumo Mensal por Setor ---
    for setor_nome, cols_setor in setores_consumo.items():
        # Calcular a soma diária de consumo para este setor
        df_plot_base[f'Consumo Diário {setor_nome}'] = df_plot_base[cols_setor].sum(axis=1)
        # Agrupar por mês e calcular a média
        consumo_mensal_medio_setor = df_plot_base[f'Consumo Diário {setor_nome}'].resample('ME').mean()

        if not consumo_mensal_medio_setor.empty and consumo_mensal_medio_setor.sum() > 0: # Apenas plotar se houver dados
            plt.figure(figsize=(12, 7))
            ax_setor = consumo_mensal_medio_setor.plot(kind='bar', color='lightcoral')
            plt.title(f'Média de Consumo Mensal - Energia - Setor: {setor_nome} (2025)')
            plt.xlabel('Mês')
            plt.ylabel('Média de Consumo Diário (Unidade)')

            # Definir os rótulos do eixo X para os nomes dos meses
            ax_setor.set_xticklabels([nomes_meses_pt[idx.month] for idx in consumo_mensal_medio_setor.index], rotation=45, ha='right')

            plt.grid(True, linestyle='--', alpha=0.6)

            # Adicionar rótulos nas barras (valores de consumo)
            # A função .bar_label() é a melhor opção para isso
            # Ajuste o padding para colocar o texto um pouco acima/dentro da barra
            for container in ax_setor.containers:
                ax_setor.bar_label(container, fmt='%.1f', padding=3, fontsize=8) # Formata com 1 casa decimal

            plt.tight_layout()
            output_image_path_setor = f'grafico_consumo_mensal_{setor_nome.replace(" ", "_")}_2025.png'
            plt.savefig('controle-agua-gas-energia/controle-494/controle-energia/' + output_image_path_setor)
            print(f"Gráfico do setor '{setor_nome}' salvo como: {output_image_path_setor}")
            plt.show()
        else:
            print(f"Não há dados de consumo significativos para o setor '{setor_nome}' em 2025 para gerar o gráfico.")


except FileNotFoundError:
    print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
except Exception as e:
    print(f"Ocorreu um erro ao ler ou processar o arquivo Excel: {e}")
