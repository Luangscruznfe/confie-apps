import sqlite3

conn = sqlite3.connect('pontos.db')
cur = conn.cursor()

cur.execute('''
    CREATE TABLE IF NOT EXISTS expedicao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data TEXT,
        A INTEGER,
        B INTEGER,
        C INTEGER,
        D INTEGER,
        E INTEGER,
        extras TEXT,
        observacao TEXT,
        total INTEGER
    )
''')

conn.commit()
conn.close()
print("Tabela 'expedicao' criada com sucesso.")
