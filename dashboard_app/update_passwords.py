from werkzeug.security import generate_password_hash

# Script preenchido com as últimas senhas que você definiu
users_to_update = {
    'MARCELO': 'marcelo18',
    'EVERTON': 'everton57',
    'MARCOS':  'marcos32',
    'PEDRO':   'pedro70',
    'RODOLFO': 'rodolfo42',
    'SILVANA': 'silvana40',
    'THYAGO':  'thyago04',
    'TIAGO':   'tiago40',
    'LUIZ':    'ConfiDis320*',
    'LUAN':    'ConfiDis320*'
}

print("-- Copie e execute estes comandos no seu banco de dados para ATUALIZAR as senhas:")
for username, new_password in users_to_update.items():
    password_hash = generate_password_hash(new_password)
    # Este script gera o comando UPDATE, que é o correto para modificar um registro existente.
    print(f"UPDATE usuarios SET password_hash = '{password_hash}' WHERE username = '{username.upper()}';")