import sqlite3
import pandas as pd

def review_table_structure(db_path, table):
    conn = sqlite3.connect(db_path)
    query = f"PRAGMA table_info({table})"
    df_structure = pd.read_sql_query(query, conn)
    conn.close()
    return df_structure

def create_indexes_on_integer_date_fields(db_path, table, structure):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Percorre as colunas e cria índice para as que são INTEGER ou DATE
    for _, row in structure.iterrows():
        column_name = row['name']
        col_type = str(row['type']).upper()
        
        if "INTEGER" in col_type or "DATE" in col_type:
            # Substitui espaços por _ para o nome do índice
            index_name = f"idx_{table}_{column_name.replace(' ', '_')}"
            # Envolve o nome da coluna em aspas para evitar erros com espaços
            sql = f'CREATE INDEX IF NOT EXISTS {index_name} ON {table}("{column_name}")'
            print(f"Criando índice: {sql}")
            cursor.execute(sql)
    
    conn.commit()
    conn.close()

def main():
    sqlite_db = "despesas.db"
    table_name = "despesas"
    
    # Revisar a estrutura da tabela
    structure = review_table_structure(sqlite_db, table_name)
    print("Estrutura da tabela:", table_name)
    print(structure)
    
    # Criar índices para campos INTEGER e DATE
    create_indexes_on_integer_date_fields(sqlite_db, table_name, structure)
    print("Índices criados com sucesso!")

if __name__ == "__main__":
    main()