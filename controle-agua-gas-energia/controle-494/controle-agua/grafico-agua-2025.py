import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import calendar
from collections import Counter

# Caminho para o arquivo Excel
file_path = 'controle-agua-gas-energia/controle-494/controle-agua/Planilha-agua (version 1).xlsx'

try:
    # --- Passo 1: Ler as linhas do cabeçalho separadamente ---
    df_all_headers = pd.read_excel(file_path, sheet_name='SABESP_2024', header=None, nrows=3, skiprows=11)
    df_all_headers_filled = df_all_headers.ffill(axis=1)

    # # --- DEBUG: Imprimir cabeçalhos preenchidos para inspeção --- (Comentado)
    # print("\n--- Cabeçalhos após ffill (df_all_headers_filled) ---")
    # print(df_all_headers_filled)
    # print("-" * 50)

    h0 = df_all_headers_filled.iloc[0].values
    h1 = df_all_headers_filled.iloc[1].values
    h2 = df_all_headers_filled.iloc[2].values

    # --- Passo 2: Construir o MultiIndex de colunas de forma mais robusta e única ---
    combined_header_tuples = []
    num_cols_from_headers = len(h2)
    seen_tuple_counts = Counter()

    # Processar 'DATA' (coluna A) e 'DIA DA SEM' (coluna B)
    # Garante que as tuplas para 'DATA' e 'DIA DA SEM' sejam criadas corretamente

    # Processar 'DATA' (h2[0])
    data_bottom = str(h2[0]).strip()
    current_tuple_data = ('', '', data_bottom)
    seen_tuple_counts[current_tuple_data] += 1
    if seen_tuple_counts[current_tuple_data] > 1:
        data_bottom = f"{data_bottom}_{seen_tuple_counts[current_tuple_data]}"
        current_tuple_data = ('', '', data_bottom)
    combined_header_tuples.append(current_tuple_data)

    # Processar 'DIA DA SEM' (h2[1])
    # Forçamos o nome para 'DIA DA SEM' no MultiIndex
    diasem_bottom = 'DIA DA SEM'
    current_tuple_diasem = ('', '', diasem_bottom)
    seen_tuple_counts[current_tuple_diasem] += 1
    if seen_tuple_counts[current_tuple_diasem] > 1:
        diasem_bottom = f"{diasem_bottom}_{seen_tuple_counts[current_tuple_diasem]}"
        current_tuple_diasem = ('', '', diasem_bottom)
    combined_header_tuples.append(current_tuple_diasem)

    # Iterar pelas outras colunas começando do índice 2 (coluna C em diante)
    for i in range(2, num_cols_from_headers):
        top_level = str(h0[i]).strip()
        if top_level == 'nan': top_level = ''

        middle_level = str(h1[i]).strip()
        if middle_level == 'nan': middle_level = ''

        bottom_level = str(h2[i]).strip()

        if bottom_level == 'nan' or bottom_level == '':
            bottom_level_base = f"Coluna_{i}"
        else:
            bottom_level_base = bottom_level

        current_tuple = (top_level, middle_level, bottom_level_base)

        seen_tuple_counts[current_tuple] += 1
        if seen_tuple_counts[current_tuple] > 1:
            bottom_level_final = f"{bottom_level_base}_{seen_tuple_counts[current_tuple]}"
            final_tuple = (top_level, middle_level, bottom_level_final)
        else:
            final_tuple = current_tuple

        combined_header_tuples.append(final_tuple)

    new_cols = pd.MultiIndex.from_tuples(combined_header_tuples)

    # # --- DEBUG: Imprimir as tuplas geradas e o MultiIndex final --- (Comentado)
    # print("\n--- Tuplas de Cabeçalho Geradas (combined_header_tuples) ---")
    # for t in combined_header_tuples:
    #     print(t)
    # print("\n--- MultiIndex Final (new_cols) ---")
    # print(new_cols)
    # print("-" * 50)


    # --- Passo 3: Ler os dados reais e atribuir o novo MultiIndex ---
    df = pd.read_excel(file_path, sheet_name='SABESP_2024', header=None, skiprows=14)

    if df.shape[1] > len(new_cols):
        df = df.iloc[:, :len(new_cols)]
        # print(f"\nAVISO: DataFrame lido com {df.shape[1]} colunas, truncado para {len(new_cols)} para corresponder ao MultiIndex.") # (Comentado)
    elif df.shape[1] < len(new_cols):
        raise ValueError(f"O número de colunas do DataFrame lido ({df.shape[1]}) é MENOR que o MultiIndex gerado ({len(new_cols)}). Verifique a planilha e as configurações de skiprows.")

    df.columns = new_cols

    # A coluna 'DATA' é agora um MultiIndex com níveis vazios e o nome 'DATA' no terceiro nível.
    found_data_col_tuple = None
    for col in new_cols:
        if col[2].startswith('DATA') and col[0] == '' and col[1] == '':
            found_data_col_tuple = col
            break
    if found_data_col_tuple is None:
        raise ValueError("Não foi possível encontrar a coluna 'DATA' no MultiIndex gerado. Verifique a estrutura do cabeçalho.")
    data_col_tuple = found_data_col_tuple

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
    # print("\nColunas do DataFrame (MultiIndex):") # (Comentado)
    # print(df_2025_multiindex.columns) # (Comentado)
    # print("-" * 50) # (Comentado)

    # --- Preparação dos dados para os gráficos ---
    setores_consumo = {}
    for col_tuple in df_2025_multiindex.columns:
        if 'CONSUMO' in str(col_tuple[-1]).upper() and col_tuple not in [found_data_col_tuple, ('', '', 'DIA DA SEM')]:

            setor_name_candidate = col_tuple[0]
            if setor_name_candidate == '':
                if col_tuple[1] != '':
                    setor_name_candidate = col_tuple[1]
                else:
                    setor_name_candidate = col_tuple[-1]
                    if setor_name_candidate.startswith('Coluna_') or setor_name_candidate.startswith('CONSUMO_'):
                        setor_name_candidate = 'Outros Consumos'

            setor_nome = setor_name_candidate

            if setor_nome not in setores_consumo:
                setores_consumo[setor_nome] = []
            setores_consumo[setor_nome].append(col_tuple)

    for setor, cols in setores_consumo.items():
        for col_tuple in cols:
            df_2025_multiindex[col_tuple] = pd.to_numeric(df_2025_multiindex[col_tuple], errors='coerce')

    df_plot_base = df_2025_multiindex.set_index(data_col_tuple)

    nomes_meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
        7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }

    # Os gráficos ainda serão gerados e salvos em arquivos PNG.
    # As mensagens de "Gráfico salvo como..." ainda aparecerão.

    # --- Gráfico de Média de Consumo Mensal Total de Água ---
    all_consumo_cols_for_sum = [col for sublist in setores_consumo.values() for col in sublist]
    if all_consumo_cols_for_sum:
        df_plot_base['Consumo Total Diário'] = df_plot_base[all_consumo_cols_for_sum].sum(axis=1)
        consumo_mensal_medio_total = df_plot_base['Consumo Total Diário'].resample('ME').mean()

        plt.figure(figsize=(12, 7))
        ax_total = consumo_mensal_medio_total.plot(kind='bar', color='skyblue')
        plt.title('Média de Consumo Mensal Total - Água (Todas as Áreas) - 2025')
        plt.xlabel('Mês')
        plt.ylabel('Média de Consumo Diário (Unidade)')

        ax_total.set_xticklabels([nomes_meses_pt[idx.month] for idx in consumo_mensal_medio_total.index], rotation=45, ha='right')

        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        output_image_path_total = 'grafico_consumo_mensal_total_agua_2025.png'
        plt.savefig('controle-agua-gas-energia/controle-494/controle-agua/' + output_image_path_total)
        print(f"\nGráfico total de água salvo como: {output_image_path_total}") # Esta linha permanecerá
        plt.show(block=False) # Usar block=False para não pausar a execução se rodar em ambiente interativo
    else:
        print("Não foram encontradas colunas de 'Consumo' para o cálculo do consumo total de água.")


    # --- Gráficos de Média de Consumo Mensal por Setor de Água ---
    for setor_nome, cols_setor in setores_consumo.items():
        if cols_setor:
            df_plot_base[f'Consumo Diário {setor_nome}'] = df_plot_base[cols_setor].sum(axis=1)
            consumo_mensal_medio_setor = df_plot_base[f'Consumo Diário {setor_nome}'].resample('ME').mean()

            if not consumo_mensal_medio_setor.empty and consumo_mensal_medio_setor.sum() > 0:
                plt.figure(figsize=(12, 7))
                ax_setor = consumo_mensal_medio_setor.plot(kind='bar', color='lightcoral')
                plt.title(f'Média de Consumo Mensal - Água - Setor: {setor_nome} (2025)')
                plt.xlabel('Mês')
                plt.ylabel('Média de Consumo Diário (Unidade)')

                ax_setor.set_xticklabels([nomes_meses_pt[idx.month] for idx in consumo_mensal_medio_setor.index], rotation=45, ha='right')

                plt.grid(True, linestyle='--', alpha=0.6)

                for container in ax_setor.containers:
                    ax_setor.bar_label(container, fmt='%.1f', padding=3, fontsize=8)

                plt.tight_layout()
                output_image_path_setor = f'grafico_consumo_mensal_agua_{setor_nome.replace(" ", "_")}_2025.png'
                plt.savefig('controle-agua-gas-energia/controle-494/controle-agua/' + output_image_path_setor)
                print(f"Gráfico do setor '{setor_nome}' de água salvo como: {output_image_path_setor}") # Esta linha permanecerá
                plt.show(block=False) # Usar block=False para não pausar a execução se rodar em ambiente interativo
            else:
                print(f"Não há dados de consumo significativos para o setor '{setor_nome}' de água em 2025 para gerar o gráfico.")
        else:
            print(f"A lista de colunas para o setor '{setor_nome}' está vazia. Verifique a lógica de identificação de colunas.")

    plt.show() # Garante que todas as janelas de plotagem sejam exibidas no final

except FileNotFoundError:
    print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
except Exception as e:
    print(f"Ocorreu um erro ao ler ou processar o arquivo Excel: {e}")
