import os
import glob
import sqlite3
import streamlit as st
import pandas as pd
import pyarrow.parquet as pq  
import datetime

# Helper function to check if a table exists in SQLite
def table_exists(conn, table_name):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cur.fetchone() is not None

def generate_sqlite_from_parquet(parquet_pattern, sqlite_db_path):
    st.write("Iniciando a leitura dos arquivos Parquet em batches...")
    files = glob.glob(parquet_pattern)
    st.write(f"Arquivos encontrados: {len(files)}")
    if not files:
        st.error("Nenhum arquivo encontrado com o padrão especificado.")
        return
    progress_bar = st.progress(0)
    conn = sqlite3.connect(sqlite_db_path)
    first_file = True
    total_files = len(files)
    file_counter = 0
    for file in files:
        st.write(f"Lendo arquivo: {file}")
        pf = pq.ParquetFile(file)
        total_groups = pf.num_row_groups
        for i in range(total_groups):
            st.write(f"Processando batch {i+1} de {total_groups} do arquivo {file}...")
            batch = pf.read_row_group(i)
            pdf = batch.to_pandas()
            if first_file:
                pdf.to_sql('despesas', conn, if_exists='replace', index=False)
                first_file = False
            else:
                pdf.to_sql('despesas', conn, if_exists='append', index=False)
        file_counter += 1
        progress_bar.progress(file_counter / total_files)
    conn.close()
    st.write("Leitura concluída. Banco de dados criado com sucesso!")
    return

def load_data_from_sqlite(sqlite_db_path):
    conn = sqlite3.connect(sqlite_db_path)
    df = pd.read_sql_query("SELECT * FROM despesas", conn)
    conn.close()
    return df

def load_data_from_sqlite_paginated(sqlite_db_path, limit=1000, offset=0):
    conn = sqlite3.connect(sqlite_db_path)
    df = pd.read_sql_query(f"SELECT * FROM despesas LIMIT {limit} OFFSET {offset}", conn)
    conn.close()
    return df

# Nova função para obter valores distintos de uma coluna
def get_distinct_values(sqlite_db_path, field):
    conn = sqlite3.connect(sqlite_db_path)
    q = f"SELECT DISTINCT [{field}] as valor FROM despesas"
    df = pd.read_sql_query(q, conn)
    conn.close()
    return sorted(df["valor"].dropna().unique())

# Nova função para obter pares distintos de nome e código
def get_distinct_key_values(sqlite_db_path, nome_field, codigo_field):
    conn = sqlite3.connect(sqlite_db_path)
    q = f"SELECT DISTINCT [{nome_field}] as nome, [{codigo_field}] as codigo FROM despesas"
    df = pd.read_sql_query(q, conn)
    conn.close()
    # Retorna lista de tuplas (nome, código)
    return sorted(list(df.dropna(subset=["codigo"]).apply(lambda row: (row["nome"], row["codigo"]), axis=1).unique()), key=lambda x: x[0])

# Função para extrair opções de data (Ano, Trimestre, Mês) a partir de "Ano e mês do lançamento"
def extract_date_filters(date_str_list):
    anos = set()
    meses = set()
    trimestres = set()
    for d in date_str_list:
        try:
            # Assume formato "YYYY-MM"
            dt = datetime.datetime.strptime(d, "%Y-%m")
            anos.add(dt.strftime("%Y"))
            # mês numérico para facilitar filtros (formato '01','02',…)
            meses.add(dt.strftime("%m"))
            # calcula trimestre: mês 1-3 => Q1, etc.
            if dt.month <= 3:
                trimestres.add("Q1")
            elif dt.month <= 6:
                trimestres.add("Q2")
            elif dt.month <= 9:
                trimestres.add("Q3")
            else:
                trimestres.add("Q4")
        except Exception:
            continue
    return sorted(list(anos)), sorted(list(trimestres)), sorted(list(meses))

# Função para extrair opções de data (já não usaremos distinct da query)
def manual_date_options():
    anos = [str(x) for x in range(2014, 2026)]
    meses_full = {"01": "Janeiro", "02": "Fevereiro", "03": "Março",
                  "04": "Abril", "05": "Maio", "06": "Junho",
                  "07": "Julho", "08": "Agosto", "09": "Setembro",
                  "10": "Outubro", "11": "Novembro", "12": "Dezembro"}
    return anos, meses_full

def init_session_state():
    # Initialize dynamic facets
    for key in ("dynamic_acoes", "dynamic_orgaos", "dynamic_planos", "dynamic_categorias", "dynamic_autores"):
        if key not in st.session_state:
            st.session_state[key] = []
    # Initialize filters
    if "filter_ano" not in st.session_state:
        st.session_state.filter_ano = "Todas"
    if "filter_trimestre" not in st.session_state:
        st.session_state.filter_trimestre = "Todos"
    if "filter_mes" not in st.session_state:
        st.session_state.filter_mes = "Todos"
    for key in ("filter_acao", "filter_orgao", "filter_plano", "filter_categoria", "filter_autor"):
        if key not in st.session_state:
            st.session_state[key] = []

def main():
    st.title("Dashboard de Dados Crus - Despesas")
    init_session_state()  # Ensure defaults are set early

    sqlite_db = "despesas.db"
    parquet_dir = "/Users/leonardodias/Documents/Arvor/GastosDesgov2023ateJan2025/data/Despesas"
    
    # Debug: lista todo o conteúdo da pasta
    if os.path.isdir(parquet_dir):
        folder_contents = os.listdir(parquet_dir)
        st.write("Conteúdo da pasta dos Parquet:", folder_contents)
    else:
        st.error(f"Pasta não encontrada: {parquet_dir}")
        return

    # Tenta o padrão "*.crc" primeiro e, se nada for encontrado, tenta "*.parquet"
    parquet_pattern = os.path.join(parquet_dir, "*.crc")
    files = glob.glob(parquet_pattern)
    if not files:
        st.warning("Nenhum arquivo .crc encontrado. Tentando padrão .parquet...")
        parquet_pattern = os.path.join(parquet_dir, "*.parquet")
        files = glob.glob(parquet_pattern)
        st.write(f"Arquivos encontrados com novo padrão: {len(files)}")
    else:
        st.write(f"Arquivos .crc encontrados: {len(files)}")
    
    if os.path.exists(sqlite_db):
        conn = sqlite3.connect(sqlite_db)
        if table_exists(conn, "despesas"):
            st.write("Arquivo SQLite encontrado e tabela 'despesas' existente. Carregando dados...")
        else:
            st.write("Arquivo SQLite encontrado, mas tabela 'despesas' inexistente. Gerando a partir dos arquivos Parquet...")
            conn.close()
            generate_sqlite_from_parquet(parquet_pattern, sqlite_db)
        conn.close()
    else:
        st.write("Arquivo SQLite não encontrado. Gerando a partir dos arquivos Parquet...")
        generate_sqlite_from_parquet(parquet_pattern, sqlite_db)
    
    # Paginação para visualização rápida (opcional)
    limit = st.number_input("Número de linhas para exibir (visualização paginada)", min_value=1, value=1000, step=1)
    offset = st.number_input("Offset (linha inicial)", min_value=0, value=0, step=1000)
    df_paginated = load_data_from_sqlite_paginated(sqlite_db, limit, offset)
    st.write("Exibindo os dados (página atual):")
    st.dataframe(df_paginated)
    
    st.markdown("## Filtros Avançados")
    st.info("Selecione os filtros para montar a query e exibir somente os dados desejados.")
    
    # Obter os valores distintos para os filtros diretamente do SQLite
    distinct_dates = get_distinct_values(sqlite_db, "Ano e mês do lançamento")
    anos, trimestres, meses = extract_date_filters(distinct_dates)
    acoes = get_distinct_values(sqlite_db, "Nome Ação")
    orgaos = get_distinct_values(sqlite_db, "Nome Órgão Superior")
    planos = get_distinct_values(sqlite_db, "Plano Orçamentário")
    categorias = get_distinct_values(sqlite_db, "Nome Categoria Econômica")
    autores = get_distinct_values(sqlite_db, "Nome Autor Emenda")
    
    # Substitua a obtenção dos filtros de data:
    # Remova as chamadas get_distinct_values e extract_date_filters para datas
    # ...existing code...
    # Obter opções manuais para datas:
    anos, meses_dict = manual_date_options()
    trimestres = ["Q1", "Q2", "Q3", "Q4"]
    meses = sorted(list(meses_dict.keys()))

    # ...existing code until sidebar filters...

    # Nova função para atualizar facets dinamicamente para um campo, excluindo seu próprio filtro
    def get_dynamic_facet(sqlite_db_path, facet_key, current_filters):
        # current_filters é um dicionário com chaves: 'Ano', 'Mês', 'Trimestre', 'Nome Ação', etc.
        conditions = []
        params = []
        # Para cada filtro aplicado, se não for o facet atual, adiciona condição
        if facet_key != "Ano" and current_filters.get("Ano") not in (None, "Todas", ""):
            conditions.append("strftime('%Y', [Ano e mês do lançamento]) = ?")
            params.append(current_filters["Ano"])
        if facet_key != "Mês" and current_filters.get("Mês") not in (None, "Todos", ""):
            # Assume que o filtro "Mês" vem com nome, converter usando meses_dict
            meses_dict = {
                "01": "Janeiro", "02": "Fevereiro", "03": "Março",
                "04": "Abril", "05": "Maio", "06": "Junho",
                "07": "Julho", "08": "Agosto", "09": "Setembro",
                "10": "Outubro", "11": "Novembro", "12": "Dezembro"
            }
            rev_mes = {v: k for k, v in meses_dict.items()}
            mes_num = rev_mes.get(current_filters["Mês"], current_filters["Mês"])
            conditions.append("strftime('%m', [Ano e mês do lançamento]) = ?")
            params.append(mes_num)
        if facet_key != "Trimestre" and current_filters.get("Trimestre") not in (None, "Todos", ""):
            qt = current_filters["Trimestre"]
            if qt == "Q1":
                conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('01','02','03')")
            elif qt == "Q2":
                conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('04','05','06')")
            elif qt == "Q3":
                conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('07','08','09')")
            elif qt == "Q4":
                conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('10','11','12')")

        # Para os filtros categóricos, mescle condições se não for o facet atual
        for key in ["Nome Ação", "Nome Órgão Superior", "Plano Orçamentário", "Nome Categoria Econômica", "Nome Autor Emenda"]:
            if facet_key != key and current_filters.get(key):
                placeholders = ",".join("?" * len(current_filters[key]))
                conditions.append(f"[{key}] IN ({placeholders})")
                params.extend(current_filters[key])
        query = f"SELECT DISTINCT [{facet_key}] as val FROM despesas"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        conn = sqlite3.connect(sqlite_db_path)
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return sorted(df["val"].dropna().unique())

    # ...existing code until sidebar filters...

    # Preparar filtros manuais para data
    anos_manual, meses_dict = manual_date_options()
    trimestres_manual = ["Q1", "Q2", "Q3", "Q4"]
    meses_keys_sorted = sorted(list(meses_dict.keys()))
    # Carregar os filtros iniciais estáticos (antes de atualizar facets)
    filtros_iniciais = {
        "Ano": "Todas",
        "Mês": "Todos",
        "Trimestre": "Todos",
        "Nome Ação": [],
        "Nome Órgão Superior": [],
        "Plano Orçamentário": [],
        "Nome Categoria Econômica": [],
        "Nome Autor Emenda": []
    }

    # Callback para atualizar os facets automaticamente quando um filtro mudar
    def update_facets():
        current_filters = {
            "Ano": st.session_state.filter_ano,
            "Trimestre": st.session_state.filter_trimestre,
            "Mês": st.session_state.filter_mes,
            "Nome Ação": st.session_state.filter_acao,
            "Nome Órgão Superior": st.session_state.filter_orgao,
            "Plano Orçamentário": st.session_state.filter_plano,
            "Nome Categoria Econômica": st.session_state.filter_categoria,
            "Nome Autor Emenda": st.session_state.filter_autor
        }
        st.session_state.dynamic_acoes = get_dynamic_facet(sqlite_db, "Nome Ação", current_filters)
        st.session_state.dynamic_orgaos = get_dynamic_facet(sqlite_db, "Nome Órgão Superior", current_filters)
        st.session_state.dynamic_planos = get_dynamic_facet(sqlite_db, "Plano Orçamentário", current_filters)
        st.session_state.dynamic_categorias = get_dynamic_facet(sqlite_db, "Nome Categoria Econômica", current_filters)
        st.session_state.dynamic_autores = get_dynamic_facet(sqlite_db, "Nome Autor Emenda", current_filters)
        st.sidebar.info("Atualizando filtros...")

    # Única sidebar: filtros são widgets sem formulário; cada alteração dispara update_facets
    st.sidebar.header("Filtros")
    st.sidebar.selectbox("Ano", options=["Todas"] + anos, index=0,
                         key="filter_ano", on_change=update_facets)
    st.sidebar.selectbox("Trimestre", options=["Todos"] + trimestres, index=0,
                         key="filter_trimestre", on_change=update_facets)
    st.sidebar.selectbox("Mês", options=["Todos"] + [meses_dict[m] for m in meses_keys_sorted], index=0,
                         key="filter_mes", on_change=update_facets)
    st.sidebar.multiselect("Nome Ação", options=st.session_state.dynamic_acoes,
                           key="filter_acao", on_change=update_facets)
    st.sidebar.multiselect("Nome Órgão Superior", options=st.session_state.dynamic_orgaos,
                           key="filter_orgao", on_change=update_facets)
    st.sidebar.multiselect("Plano Orçamentário", options=st.session_state.dynamic_planos,
                           key="filter_plano", on_change=update_facets)
    st.sidebar.multiselect("Nome Categoria Econômica", options=st.session_state.dynamic_categorias,
                           key="filter_categoria", on_change=update_facets)
    st.sidebar.multiselect("Nome Autor Emenda", options=st.session_state.dynamic_autores,
                           key="filter_autor", on_change=update_facets)
    
    # Botão único para exibir os resultados com os filtros selecionados
    exibir = st.sidebar.button("Exibir resultados")
    
    if exibir:
        # Verifica se ao menos um filtro foi modificado
        if (st.session_state.filter_ano == "Todas" and st.session_state.filter_trimestre == "Todos" and 
            st.session_state.filter_mes == "Todos" and not st.session_state.filter_acao and 
            not st.session_state.filter_orgao and not st.session_state.filter_plano and 
            not st.session_state.filter_categoria and not st.session_state.filter_autor):
            st.error("Aplique ao menos um filtro antes de exibir os resultados.")
        else:
            conditions = []
            params = []
            if st.session_state.filter_ano != "Todas":
                conditions.append("strftime('%Y', [Ano e mês do lançamento]) = ?")
                params.append(st.session_state.filter_ano)
            if st.session_state.filter_mes != "Todos":
                rev_mes = {v: k for k, v in meses_dict.items()}
                mes_num = rev_mes.get(st.session_state.filter_mes, st.session_state.filter_mes)
                conditions.append("strftime('%m', [Ano e mês do lançamento]) = ?")
                params.append(mes_num)
            if st.session_state.filter_trimestre != "Todos":
                qt = st.session_state.filter_trimestre
                if qt == "Q1":
                    conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('01','02','03')")
                elif qt == "Q2":
                    conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('04','05','06')")
                elif qt == "Q3":
                    conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('07','08','09')")
                elif qt == "Q4":
                    conditions.append("strftime('%m', [Ano e mês do lançamento]) IN ('10','11','12')")
            for key, col in [("filter_acao", "Nome Ação"),
                             ("filter_orgao", "Nome Órgão Superior"),
                             ("filter_plano", "Plano Orçamentário"),
                             ("filter_categoria", "Nome Categoria Econômica"),
                             ("filter_autor", "Nome Autor Emenda")]:
                if st.session_state.get(key):
                    placeholders = ",".join("?" * len(st.session_state[key]))
                    conditions.append(f"[{col}] IN ({placeholders})")
                    params.extend(st.session_state[key])
            query = "SELECT * FROM despesas"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            st.markdown("### Dados Filtrados")
            st.code(query)  # Debug: mostra a query gerada
            conn = sqlite3.connect(sqlite_db)
            df_filtered = pd.read_sql_query(query, conn, params=params)
            conn.close()
            st.dataframe(df_filtered)
            if "Ano e mês do lançamento" in df_filtered.columns and "Valor Pago (R$)" in df_filtered.columns:
                st.write("Gráfico de Séries Temporais:")
                df_line = df_filtered.sort_values("Ano e mês do lançamento")
                st.line_chart(df_line.set_index("Ano e mês do lançamento")["Valor Pago (R$)"])
    
if __name__ == "__main__":
    main()