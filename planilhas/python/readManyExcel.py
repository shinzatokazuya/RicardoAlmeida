import pandas as pd
import glob
import os
from datetime import datetime

def processar_arquivo_individual(arquivo):
    """
    Processa um único arquivo CSV, agrupando solicitações duplicadas
    e somando seus valores.

    Este é o primeiro filtro: elimina duplicatas DENTRO do mesmo arquivo.
    """
    try:
        print(f"\n  → Lendo: {os.path.basename(arquivo)}")

        # Lê o arquivo com as configurações corretas
        df = pd.read_csv(arquivo, delimiter=';', encoding='utf-8')

        # Normaliza o número da solicitação para garantir que seja numérico
        df['Solicitação'] = pd.to_numeric(df['Solicitação'], errors='coerce')

        # Normaliza o valor da solicitação
        # Importante: valores brasileiros vêm como "1.234,56" e precisam virar 1234.56
        df['Vl.Solicitação'] = (
            df['Vl.Solicitação']
            .astype(str)
            .str.replace('.', '', regex=False)  # Remove separador de milhar
            .str.replace(',', '.', regex=False)  # Vírgula decimal vira ponto
            .str.strip()
        )
        df['Vl.Solicitação'] = pd.to_numeric(df['Vl.Solicitação'], errors='coerce')

        # Remove linhas inválidas
        df_limpo = df.dropna(subset=['Solicitação'])

        # Agrupa por solicitação, somando os valores duplicados
        # Cada solicitação pode ter múltiplos itens, então somamos os valores
        # mas mantemos apenas o primeiro registro das outras informações
        df_agrupado = df_limpo.groupby('Solicitação').agg(
            Empresa=('Empresa', 'first'),
            Data=('Data', 'first'),
            Situacao=('Situação', 'first'),
            Usuario=('Usuário', 'first'),
            Nr_nf=('Nr. Nf', 'first'),
            Sku=('Sku', 'first'),
            Dt_Preventrega=('Dt. Preventrega', 'first'),
            Pedido=('Pedido', 'first'),
            Ds_Prioridade=('Ds. Prioridade', 'first'),
            Ds_Compra=('Ds. Compra', 'first'),
            Vl_Solicitacao_Total=('Vl.Solicitação', 'sum'),
            Cod_Ccusto=('Cod. Ccusto', 'first'),
            Obs_lin1=('Obs lin1', 'first'),
            Obs_lin2=('Obs lin2', 'first'),
            Obs_lin3=('Obs lin3', 'first'),
            Obs_lin4=('Obs lin4', 'first')
        ).reset_index()

        # Arredonda para 2 casas decimais
        df_agrupado['Vl_Solicitacao_Total'] = df_agrupado['Vl_Solicitacao_Total'].round(2)

        print(f"     ✓ {len(df)} linhas → {len(df_agrupado)} solicitações únicas")

        return df_agrupado

    except Exception as e:
        print(f"     ✗ Erro ao processar {arquivo}: {e}")
        return None


def consolidar_multiplos_arquivos(padrao_arquivos, arquivo_saida):
    """
    Consolida múltiplos arquivos CSV em uma única base, eliminando duplicatas
    entre arquivos (isso resolve o problema de datas sobrepostas).

    Este é o segundo filtro: elimina duplicatas ENTRE arquivos diferentes.
    Quando a mesma solicitação aparecer em múltiplos arquivos, mantemos
    apenas a versão mais recente (a do último arquivo processado).
    """
    try:
        # Busca todos os arquivos que correspondem ao padrão
        arquivos = glob.glob(padrao_arquivos)

        if not arquivos:
            print(f"Erro: Nenhum arquivo encontrado com o padrão '{padrao_arquivos}'")
            return None

        # Ordena os arquivos por nome (assumindo que têm data no nome)
        # Isso garante que processamos do mais antigo para o mais recente
        arquivos.sort()

        print(f"\n{'='*60}")
        print(f"CONSOLIDANDO {len(arquivos)} ARQUIVO(S)")
        print(f"{'='*60}")

        lista_dataframes = []

        # Processa cada arquivo individualmente
        for arquivo in arquivos:
            df_processado = processar_arquivo_individual(arquivo)
            if df_processado is not None:
                lista_dataframes.append(df_processado)

        if not lista_dataframes:
            print("Erro: Nenhum arquivo foi processado com sucesso")
            return None

        print(f"\n{'='*60}")
        print("ELIMINANDO DUPLICATAS ENTRE ARQUIVOS")
        print(f"{'='*60}")

        # Junta todos os dataframes em um só
        df_completo = pd.concat(lista_dataframes, ignore_index=True)
        print(f"  Total de linhas antes de remover duplicatas: {len(df_completo)}")

        # Remove duplicatas mantendo a última ocorrência
        # Isso é crucial: se a solicitação 12345 aparece no arquivo de 20/10
        # e também no arquivo de 27/10, mantemos a do arquivo de 27/10 (mais recente)
        df_final = df_completo.drop_duplicates(subset=['Solicitação'], keep='last')
        print(f"  Total de linhas após remover duplicatas: {len(df_final)}")
        print(f"  → Foram eliminadas {len(df_completo) - len(df_final)} solicitações duplicadas")

        # Ordena por número de solicitação para facilitar consultas futuras
        df_final = df_final.sort_values('Solicitação').reset_index(drop=True)

        # Reordena as colunas
        colunas_ordenadas = [
            'Empresa', 'Data', 'Situacao', 'Usuario', 'Solicitação',
            'Nr_nf', 'Sku', 'Dt_Preventrega', 'Pedido', 'Ds_Prioridade',
            'Ds_Compra', 'Vl_Solicitacao_Total', 'Cod_Ccusto',
            'Obs_lin1', 'Obs_lin2', 'Obs_lin3', 'Obs_lin4'
        ]
        df_final = df_final[colunas_ordenadas]

        # Salva o resultado
        df_final.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')

        print(f"\n{'='*60}")
        print(f"ARQUIVO FINAL SALVO: {arquivo_saida}")
        print(f"Total de solicitações únicas: {len(df_final)}")
        print(f"{'='*60}\n")

        return df_final

    except Exception as e:
        print(f"Erro ao consolidar arquivos: {e}")
        return None


def adicionar_novos_dados_semanais(arquivo_base, padrao_novos_arquivos, arquivo_saida):
    """
    Adiciona novos dados semanais a uma base existente.

    Use esta função quando você já tem uma base consolidada e quer adicionar
    dados da semana seguinte. A função garante que não haverá duplicatas
    mesmo se houver sobreposição de datas.
    """
    try:
        print(f"\n{'='*60}")
        print("ATUALIZANDO BASE EXISTENTE COM NOVOS DADOS")
        print(f"{'='*60}")

        # Lê a base existente
        print(f"\n  → Carregando base existente: {arquivo_base}")
        df_base = pd.read_csv(arquivo_base, delimiter=';', encoding='utf-8')
        print(f"     ✓ Base tem {len(df_base)} solicitações")

        # Processa os novos arquivos
        arquivos_novos = glob.glob(padrao_novos_arquivos)

        if not arquivos_novos:
            print(f"Erro: Nenhum arquivo novo encontrado com o padrão '{padrao_novos_arquivos}'")
            return None

        arquivos_novos.sort()
        print(f"\n  → Processando {len(arquivos_novos)} arquivo(s) novo(s)")

        lista_novos = []
        for arquivo in arquivos_novos:
            df_processado = processar_arquivo_individual(arquivo)
            if df_processado is not None:
                lista_novos.append(df_processado)

        if not lista_novos:
            print("Erro: Nenhum arquivo novo foi processado com sucesso")
            return None

        # Consolida os novos arquivos (elimina duplicatas entre eles)
        df_novos = pd.concat(lista_novos, ignore_index=True)
        df_novos = df_novos.drop_duplicates(subset=['Solicitação'], keep='last')
        print(f"     ✓ Total de solicitações novas/atualizadas: {len(df_novos)}")

        # Junta base antiga com dados novos
        df_completo = pd.concat([df_base, df_novos], ignore_index=True)

        # Remove duplicatas mantendo sempre a versão mais recente (keep='last')
        # Isso garante que se uma solicitação já existia, ela será atualizada
        df_final = df_completo.drop_duplicates(subset=['Solicitação'], keep='last')

        print(f"\n  → Base anterior: {len(df_base)} solicitações")
        print(f"  → Dados novos: {len(df_novos)} solicitações")
        print(f"  → Base final: {len(df_final)} solicitações")
        print(f"  → Novas solicitações adicionadas: {len(df_final) - len(df_base)}")

        # Ordena e salva
        df_final = df_final.sort_values('Solicitação').reset_index(drop=True)
        df_final.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')

        print(f"\n{'='*60}")
        print(f"BASE ATUALIZADA SALVA: {arquivo_saida}")
        print(f"{'='*60}\n")

        return df_final

    except Exception as e:
        print(f"Erro ao adicionar novos dados: {e}")
        return None


# ==================== EXEMPLOS DE USO ====================

# CENÁRIO 1: Primeira vez - processar múltiplos arquivos históricos
# Use quando estiver começando e tiver vários arquivos para consolidar
print("\n" + "🔷" * 30)
print("CENÁRIO 1: CONSOLIDAÇÃO INICIAL")
print("🔷" * 30)
"""
# Exemplo: você tem arquivos de diferentes semanas na pasta planilhas/csv/
# Todos seguem o padrão RICARDOALMEIDA*.csv
resultado = consolidar_multiplos_arquivos(
    padrao_arquivos='planilhas/csv/planilhas_semanais/*/RICARDOALMEIDA*.csv',
    arquivo_saida='planilhas/csv/planilhas_relatorios/relatorio_ate_27-10-2025.csv'
)
"""
# CENÁRIO 2: Atualizações semanais
print("\n" + "🔶" * 30)
print("CENÁRIO 2: ATUALIZAÇÃO SEMANAL (EXEMPLO)")
print("🔶" * 30)

# Atualização semanal
resultado_atualizado = adicionar_novos_dados_semanais(
    arquivo_base='planilhas/csv/planilha_geral/planilha_geral_ate_20-10-2025.csv',
    padrao_novos_arquivos='planilhas/csv/planilhas_semanais/*/RICARDOALMEIDA*.csv',
    arquivo_saida='planilhas/csv/planilhas_relatorios/relatorio_ate_27-10-2025.csv'
)

