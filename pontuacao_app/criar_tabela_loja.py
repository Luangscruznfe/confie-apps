import sqlite3

conn = sqlite3.connect('pontos.db')
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS loja (
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

print("✅ Tabela 'loja' criada com sucesso!")
