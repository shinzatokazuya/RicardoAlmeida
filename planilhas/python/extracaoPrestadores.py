import pandas as pd
import re

def extrair_nome_prestador(obs1, obs2, obs3, obs4):
    """
    Extrai o nome do prestador das colunas de observação.
    Procura por padrões comuns como:
    - PRESTADOR: NOME
    - NF XXX - ... PRESTADOR: NOME
    - Linhas que começam com nomes (geralmente em maiúsculas)
    """
    # Junta todas as observações em uma lista
    observacoes = [obs1, obs2, obs3, obs4]

    # Remove valores vazios (NaN) e converte para string
    observacoes = [str(obs).strip() for obs in observacoes if pd.notna(obs) and str(obs).strip() != '']

    if not observacoes:
        return None

    # Padrão 1: Procura por "PRESTADOR:" seguido do nome
    for obs in observacoes:
        match = re.search(r'PRESTADOR[:\s]+([A-Z][A-Z\s&\.]+?)(?:;|$|\n)', obs)
        if match:
            nome = match.group(1).strip()
            # Remove pontos e ponto-e-vírgulas extras do final
            nome = nome.rstrip(';.').strip()
            if len(nome) > 3:  # Nome deve ter pelo menos 3 caracteres
                return nome

    # Padrão 2: Procura por linha que começa com nome (palavras em maiúsculas)
    for obs in observacoes:
        # Procura por sequência de palavras em maiúsculas no início
        match = re.match(r'^([A-Z][A-Z\s&\.]{3,}?)(?:\s*[-;]|\s*$)', obs)
        if match:
            nome = match.group(1).strip()
            # Remove pontos e ponto-e-vírgulas extras
            nome = nome.rstrip(';.').strip()
            # Verifica se não é apenas uma palavra (muito curto)
            if len(nome.split()) >= 2:
                return nome

    # Se não encontrou nada, retorna a primeira observação
    # (assumindo que geralmente o nome está na primeira linha)
    primeira_obs = observacoes[0]
    # Pega até o primeiro ponto-e-vírgula, hífen ou final da linha
    nome = re.split(r'[;\-]', primeira_obs)[0].strip()

    # Limpa o nome removendo prefixos comuns
    nome = re.sub(r'^(NF|NOTA|VENCIMENTO|PAGAMENTO|REF|REFERENTE).*?:', '', nome, flags=re.IGNORECASE)
    nome = nome.strip()

    return nome if nome else None


def processar_solicitacoes_para_analise(arquivo_entrada, arquivo_saida=None):
    """
    Processa o arquivo de solicitações extraindo informações relevantes
    e organizando para análise visual.
    """
    try:
        print(f"Lendo o arquivo: {arquivo_entrada}")
        df = pd.read_csv(arquivo_entrada, delimiter=';', encoding='utf-8')

        print(f"Total de registros lidos: {len(df)}")

        # Extrai o nome do prestador das observações
        print("\nExtraindo nomes dos prestadores...")
        df['Prestador'] = df.apply(
            lambda row: extrair_nome_prestador(
                row['Obs_lin1'],
                row['Obs_lin2'],
                row['Obs_lin3'],
                row['Obs_lin4']
            ),
            axis=1
        )

        # Conta quantos prestadores foram identificados
        prestadores_identificados = df['Prestador'].notna().sum()
        print(f"Prestadores identificados: {prestadores_identificados} de {len(df)}")

        # Normaliza o valor da solicitação
        df['Vl_Solicitacao_Total'] = pd.to_numeric(df['Vl_Solicitacao_Total'], errors='coerce')

        # Converte a data para formato datetime para facilitar análises futuras
        df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')

        # Seleciona e reordena as colunas para análise
        colunas_analise = [
            'Empresa',           # Nome da filial
            'Data',             # Data da solicitação
            'Prestador',        # Nome do prestador (extraído)
            'Solicitação',      # Número da solicitação
            'Pedido',           # Número do pedido
            'Vl_Solicitacao_Total',  # Valor
            'Dt_Preventrega',        # Data prevista de entrega
            'Ds_Prioridade',       # Prioridade
            'Usuario',          # Nome do usuário
            'Situacao',         # Situação atual
            'Ds_Compra'        # Tipo de compra
            #'Obs_lin1',         # Observações (mantidas para referência)
            #'Obs_lin2',
            #'Obs_lin3',
            #'Obs_lin4'
        ]

        # Cria o dataframe final apenas com as colunas selecionadas
        df_analise = df[colunas_analise].copy()

        # Ordena por data (mais recente primeiro) e depois por empresa
        df_analise = df_analise.sort_values(['Data', 'Empresa'], ascending=[False, True])

        # Estatísticas básicas
        print(f"\n{'='*60}")
        print("ESTATÍSTICAS DO PROCESSAMENTO")
        print(f"{'='*60}")
        print(f"Total de solicitações: {len(df_analise)}")
        print(f"Período: {df_analise['Data'].min().strftime('%d/%m/%Y')} até {df_analise['Data'].max().strftime('%d/%m/%Y')}")
        print(f"Valor total: R$ {df_analise['Vl_Solicitacao_Total'].sum():,.2f}")
        print(f"Empresas/Filiais: {df_analise['Empresa'].nunique()}")
        print(f"Prestadores únicos identificados: {df_analise['Prestador'].nunique()}")

        # Mostra os 10 prestadores mais frequentes
        print(f"\n{'='*60}")
        print("TOP 10 PRESTADORES MAIS FREQUENTES")
        print(f"{'='*60}")
        top_prestadores = df_analise['Prestador'].value_counts().head(10)
        for prestador, count in top_prestadores.items():
            if pd.notna(prestador):
                print(f"{prestador}: {count} solicitações")

        # Salva o arquivo se foi fornecido um caminho
        if arquivo_saida:
            df_analise.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')
            print(f"\n{'='*60}")
            print(f"Arquivo salvo: {arquivo_saida}")
            print(f"{'='*60}")

        return df_analise

    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
        return None


def criar_base_prestadores(df_analise, arquivo_saida='planilhas/base_prestadores.csv'):
    """
    Cria uma planilha base de prestadores únicos para você preencher
    manualmente com CNPJ e outras informações.
    """
    try:
        # Extrai lista única de prestadores
        prestadores_unicos = df_analise['Prestador'].dropna().unique()

        # Cria dataframe com estrutura para preenchimento
        df_prestadores = pd.DataFrame({
            'Nome_Prestador': sorted(prestadores_unicos),
            'CNPJ': '',  # Para você preencher
            'Contato': '',  # Para você preencher
            'Email': '',  # Para você preencher
            'Telefone': '',  # Para você preencher
            'Observacoes': ''  # Para você preencher
        })

        # Salva
        df_prestadores.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')

        print(f"\n{'='*60}")
        print("BASE DE PRESTADORES CRIADA")
        print(f"{'='*60}")
        print(f"Arquivo: {arquivo_saida}")
        print(f"Total de prestadores únicos: {len(df_prestadores)}")
        print("\nPreencha manualmente as colunas CNPJ, Contato, Email, Telefone")
        print("Depois, use a função relacionar_com_prestadores() para enriquecer seus dados")

        return df_prestadores

    except Exception as e:
        print(f"Erro ao criar base de prestadores: {e}")
        return None


def relacionar_com_prestadores(df_analise, arquivo_prestadores, arquivo_saida=None):
    """
    Relaciona as solicitações com a base de prestadores usando o nome.
    No futuro, quando tiver CNPJ nas observações, pode usar esse campo.
    """
    try:
        # Lê a base de prestadores
        df_prestadores = pd.read_csv(arquivo_prestadores, delimiter=';', encoding='utf-8')

        print(f"Base de prestadores carregada: {len(df_prestadores)} registros")

        # Faz o merge pelo nome do prestador
        df_enriquecido = pd.merge(
            df_analise,
            df_prestadores,
            left_on='Prestador',
            right_on='Nome_Prestador',
            how='left'
        )

        # Remove coluna duplicada
        df_enriquecido = df_enriquecido.drop('Nome_Prestador', axis=1)

        # Reordena as colunas colocando informações do prestador logo após o nome
        colunas_ordenadas = [
            'Empresa', 'Data', 'Prestador', 'CNPJ', 'Contato', 'Email', 'Telefone',
            'Solicitação', 'Pedido', 'Vl_Solicitacao_Total', 'Data_Prev',
            'Prioridade', 'Usuario', 'Situacao', 'Descricao',
            'Obs_lin1', 'Obs_lin2', 'Obs_lin3', 'Obs_lin4', 'Observacoes'
        ]

        df_enriquecido = df_enriquecido[colunas_ordenadas]

        # Estatísticas
        prestadores_com_cnpj = df_enriquecido['CNPJ'].notna().sum()
        print(f"\nSolicitações com CNPJ identificado: {prestadores_com_cnpj} de {len(df_enriquecido)}")

        # Salva se fornecido caminho
        if arquivo_saida:
            df_enriquecido.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')
            print(f"Arquivo enriquecido salvo: {arquivo_saida}")

        return df_enriquecido

    except Exception as e:
        print(f"Erro ao relacionar com prestadores: {e}")
        return None


# ==================== EXEMPLO DE USO ====================

# PASSO 1: Processar o arquivo de solicitações e extrair prestadores
print("="*60)
print("PASSO 1: PROCESSANDO SOLICITAÇÕES")
print("="*60)

df_analise = processar_solicitacoes_para_analise(
    arquivo_entrada='planilhas/csv/relatorio_ate_20-10-2025.csv',
    arquivo_saida='planilhas/solicitacoes_para_analise.csv'
)

# PASSO 2: Criar base de prestadores para preenchimento manual
if df_analise is not None:
    print("\n" + "="*60)
    print("PASSO 2: CRIANDO BASE DE PRESTADORES")
    print("="*60)

    df_prestadores = criar_base_prestadores(
        df_analise,
        arquivo_saida='planilhas/base_prestadores.csv'
    )

# PASSO 3: Depois que você preencher a base de prestadores manualmente,
# use este código para relacionar tudo
"""
print("\n" + "="*60)
print("PASSO 3: RELACIONANDO COM BASE DE PRESTADORES")
print("="*60)

df_final = relacionar_com_prestadores(
    df_analise,
    arquivo_prestadores='planilhas/base_prestadores.csv',
    arquivo_saida='planilhas/solicitacoes_completas.csv'
)
"""
