from werkzeug.security import generate_password_hash
import getpass

# Pede para o usuário digitar a senha de forma segura (não mostra na tela)
senha = getpass.getpass("Digite a nova senha: ")

# Gera o hash da senha
hash_senha = generate_password_hash(senha)

# Imprime o resultado
print("\nSenha criptografada (hash):")
print(hash_senha)