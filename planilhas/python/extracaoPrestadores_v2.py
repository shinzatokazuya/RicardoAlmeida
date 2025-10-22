import pandas as pd
import re

# =========================================================
#  FUNÇÕES DE EXTRAÇÃO E CLASSIFICAÇÃO
# =========================================================

def extrair_nome_prestador(obs1, obs2, obs3, obs4):
    """
    Extrai o nome do prestador das colunas de observação.
    Ignora linhas com NF, compra, serviço ou produto.
    Procura por padrões:
      - PRESTADOR: NOME
      - Linhas que começam com nomes em maiúsculas
    """
    observacoes = [str(obs).strip() for obs in [obs1, obs2, obs3, obs4] if pd.notna(obs) and str(obs).strip() != '']
    if not observacoes:
        return None

    texto_completo = " ".join(observacoes).lower()

    # Ignora se contiver NF, compra, serviço ou produto
    if re.search(r'\bNF\s*\d+', texto_completo, re.IGNORECASE):
        return None
    if any(p in texto_completo for p in ['favor seguir', 'link', 'email', 'compra', 'servico', 'serviço', 'produto']):
        return None

    # Padrão 1: PRESTADOR: NOME
    for obs in observacoes:
        match = re.search(r'PRESTADOR[:\s]+([A-Z][A-Z\s&\.]+?)(?:;|$|\n)', obs)
        if match:
            nome = match.group(1).strip().rstrip(';. ')
            if len(nome) > 3:
                return nome.title()

    # Padrão 2: linha começando com palavras maiúsculas
    for obs in observacoes:
        match = re.match(r'^([A-Z][A-Z\s&\.]{3,}?)(?:\s*[-;:]|\s*$)', obs)
        if match:
            nome = match.group(1).strip().rstrip(';. ')
            if len(nome.split()) >= 2:
                return nome.title()

    # Caso não encontre, tenta limpar a primeira linha
    primeira_obs = observacoes[0]
    nome = re.split(r'[;\-]', primeira_obs)[0].strip()
    nome = re.sub(r'^(NF|NOTA|VENCIMENTO|PAGAMENTO|REF|REFERENTE).*?:', '', nome, flags=re.IGNORECASE).strip()
    if len(nome) < 3 or nome.isnumeric():
        return None
    return nome.title() if nome else None


def identificar_tipo(obs_list):
    """
    Define o tipo da solicitação: Compra / Serviço / Produto / Outro
    """
    texto = " ".join([str(o).lower() for o in obs_list if pd.notna(o)])
    if any(p in texto for p in ['compra', 'favor seguir', 'link', 'email']):
        return 'Compra'
    elif 'servico' in texto or 'serviço' in texto:
        return 'Serviço'
    elif 'produto' in texto:
        return 'Produto'
    return 'Outro'


def extrair_nf(texto):
    match = re.search(r'\bNF\s*(\d+)', texto, re.IGNORECASE)
    return match.group(1) if match else None


def extrair_vencimento(texto):
    match = re.search(r'vencimento\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)', texto, re.IGNORECASE)
    return match.group(1) if match else None


def extrair_descricao_item(obs_list):
    """
    Extrai o texto que vem após 'servico:', 'produto:' ou 'compra:',
    até encontrar um traço (-), barra (/), ponto e vírgula (;) ou quebra de linha.
    Exemplo: 'servico: limpeza geral - vencimento 28/10' -> 'limpeza geral'
    """
    texto = " ".join([str(o) for o in obs_list if pd.notna(o)]).lower()
    match = re.search(r'(servico|serviço|produto|compra)\s*:\s*([^-/;\n]+)', texto)
    if match:
        descricao = match.group(2).strip()
        descricao = re.sub(r'\s+', ' ', descricao)
        return descricao.capitalize()
    return None


# =========================================================
#  PROCESSAMENTO PRINCIPAL
# =========================================================

def processar_solicitacoes_para_analise(arquivo_entrada, arquivo_saida=None):
    """
    Processa o arquivo CSV de solicitações e gera uma versão analítica com:
    - Prestador
    - Tipo (Compra / Serviço / Produto / Outro)
    - NF e vencimento
    - Descrição do item (quando houver)
    """
    try:
        print(f"Lendo o arquivo: {arquivo_entrada}")
        df = pd.read_csv(arquivo_entrada, delimiter=';', encoding='utf-8')
        print(f"Total de registros lidos: {len(df)}")

        # -------------------------------
        # EXTRAÇÃO DE CAMPOS ADICIONAIS
        # -------------------------------
        print("\nExtraindo nomes dos prestadores...")
        df['Prestador'] = df.apply(
            lambda row: extrair_nome_prestador(
                row.get('Obs_lin1'), row.get('Obs_lin2'),
                row.get('Obs_lin3'), row.get('Obs_lin4')
            ),
            axis=1
        )

        df['Tipo'] = df.apply(
            lambda row: identificar_tipo([
                row.get('Obs_lin1'), row.get('Obs_lin2'),
                row.get('Obs_lin3'), row.get('Obs_lin4')
            ]),
            axis=1
        )

        print("Extraindo informações de NF e vencimento...")
        df['Numero_NF'] = df.apply(
            lambda r: next(
                (extrair_nf(str(v)) for v in [r.get('Obs_lin1'), r.get('Obs_lin2'), r.get('Obs_lin3'), r.get('Obs_lin4')]
                 if v and extrair_nf(str(v))), None),
            axis=1
        )

        df['Vencimento_NF'] = df.apply(
            lambda r: next(
                (extrair_vencimento(str(v)) for v in [r.get('Obs_lin1'), r.get('Obs_lin2'), r.get('Obs_lin3'), r.get('Obs_lin4')]
                 if v and extrair_vencimento(str(v))), None),
            axis=1
        )

        print("Extraindo descrições de serviço/produto/compra...")
        df['Descricao_Item'] = df.apply(
            lambda row: extrair_descricao_item([
                row.get('Obs_lin1'), row.get('Obs_lin2'),
                row.get('Obs_lin3'), row.get('Obs_lin4')
            ]),
            axis=1
        )

        # -------------------------------
        # NORMALIZAÇÃO DE CAMPOS
        # -------------------------------
        df['Vl_Solicitacao_Total'] = pd.to_numeric(df['Vl_Solicitacao_Total'], errors='coerce')
        df['Data'] = pd.to_datetime(df['Data'], format='%d/%m/%Y', errors='coerce')

        # -------------------------------
        # ORGANIZAÇÃO DAS COLUNAS
        # -------------------------------
        colunas_analise = [
            'Empresa', 'Data', 'Prestador', 'Tipo', 'Descricao_Item',
            'Solicitação', 'Pedido', 'Vl_Solicitacao_Total',
            'Dt_Preventrega', 'Ds_Prioridade', 'Usuario',
            'Situacao', 'Numero_NF', 'Vencimento_NF'
        ]

        df_analise = df[colunas_analise].copy()
        df_analise = df_analise.sort_values(['Data', 'Empresa'], ascending=[False, True])

        # -------------------------------
        # ESTATÍSTICAS
        # -------------------------------
        print(f"\n{'='*60}")
        print("RESUMO DO PROCESSAMENTO")
        print(f"{'='*60}")
        print(f"Total de solicitações: {len(df_analise)}")
        print(f"Prestadores identificados: {df_analise['Prestador'].notna().sum()}")
        print(f"Tipos: {df_analise['Tipo'].value_counts().to_dict()}")
        print(f"Valor total: R$ {df_analise['Vl_Solicitacao_Total'].sum():,.2f}")
        print(f"Empresas únicas: {df_analise['Empresa'].nunique()}")
        print(f"Prestadores únicos: {df_analise['Prestador'].nunique()}")

        # -------------------------------
        # EXPORTAÇÃO
        # -------------------------------
        if arquivo_saida:
            df_analise.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8')
            print(f"\nArquivo analítico salvo em: {arquivo_saida}")

        return df_analise

    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
        return None


# =========================================================
#  EXECUÇÃO PRINCIPAL
# =========================================================

if __name__ == "__main__":
    print("="*60)
    print("PROCESSANDO RELATÓRIO ANALÍTICO")
    print("="*60)

    df_analise = processar_solicitacoes_para_analise(
        arquivo_entrada='planilhas/csv/relatorio_ate_20-10-2025.csv',
        arquivo_saida='planilhas/relatorio_analitico.csv'
    )

    if df_analise is not None:
        print("\nProcessamento concluído com sucesso!")
        print(f"Arquivo final contém {len(df_analise)} registros.")
