#!/usr/bin/env python3
# ==============================================================================
# ABInBev Case - Secrets Management CLI
# ==============================================================================
#
# CLI para gerenciar credenciais de forma segura usando o keyring do SO.
#
# Uso:
#   python scripts/manage_secrets.py set AZURE_STORAGE_ACCOUNT_KEY
#   python scripts/manage_secrets.py get AZURE_STORAGE_ACCOUNT_KEY
#   python scripts/manage_secrets.py delete AZURE_STORAGE_ACCOUNT_KEY
#   python scripts/manage_secrets.py list
#   python scripts/manage_secrets.py import-from-env
#   python scripts/manage_secrets.py status
#
# ==============================================================================

import argparse
import getpass
import os
import sys

# Adiciona o diretorio src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from config.secrets import KNOWN_SECRETS, SERVICE_NAME, SecretsManager


def cmd_set(args):
    """Armazena uma credencial no keyring."""
    secrets = SecretsManager()

    if not secrets.is_keyring_available:
        print("ERRO: Keyring nao disponivel neste sistema.")
        print("Use variaveis de ambiente no arquivo .env como alternativa.")
        return 1

    key = args.key
    if args.value:
        value = args.value
    else:
        value = getpass.getpass(f"Digite o valor para '{key}': ")

    if not value:
        print("ERRO: Valor nao pode ser vazio.")
        return 1

    if secrets.set_secret(key, value):
        print(f"OK: Credencial '{key}' armazenada com sucesso no keyring.")
        return 0
    else:
        print(f"ERRO: Falha ao armazenar credencial '{key}'.")
        return 1


def cmd_get(args):
    """Obtem uma credencial."""
    secrets = SecretsManager()
    key = args.key

    value = secrets.get_secret(key)
    source = secrets.get_secret_source(key)

    if value is None:
        print(f"Credencial '{key}' nao encontrada.")
        return 1

    if args.show:
        print(f"{key}={value} (fonte: {source})")
    else:
        # Mostra apenas parte do valor por seguranca
        masked = value[:4] + "*" * (len(value) - 4) if len(value) > 4 else "****"
        print(f"{key}={masked} (fonte: {source})")

    return 0


def cmd_delete(args):
    """Remove uma credencial do keyring."""
    secrets = SecretsManager()

    if not secrets.is_keyring_available:
        print("ERRO: Keyring nao disponivel neste sistema.")
        return 1

    key = args.key

    if secrets.delete_secret(key):
        print(f"OK: Credencial '{key}' removida do keyring.")
        return 0
    else:
        print(f"ERRO: Falha ao remover credencial '{key}'.")
        return 1


def cmd_list(args):
    """Lista credenciais conhecidas e seu status."""
    secrets = SecretsManager()

    print(f"Servico: {SERVICE_NAME}")
    print(f"Keyring disponivel: {'Sim' if secrets.is_keyring_available else 'Nao'}")
    print()
    print("Credenciais conhecidas:")
    print("-" * 50)

    for key in KNOWN_SECRETS:
        source = secrets.get_secret_source(key)
        if source:
            status = f"[{source.upper()}]"
        else:
            status = "[NAO DEFINIDA]"
        print(f"  {key}: {status}")

    print()
    return 0


def cmd_import_from_env(args):
    """Importa credenciais do arquivo .env para o keyring."""
    secrets = SecretsManager()

    if not secrets.is_keyring_available:
        print("ERRO: Keyring nao disponivel neste sistema.")
        return 1

    # Carrega o arquivo .env
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if not os.path.exists(env_path):
        print(f"ERRO: Arquivo .env nao encontrado em {env_path}")
        return 1

    # Le o arquivo .env
    env_vars = {}
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key in KNOWN_SECRETS and value and not value.startswith("your_"):
                    env_vars[key] = value

    if not env_vars:
        print("Nenhuma credencial valida encontrada no .env para importar.")
        print("(Valores que comecam com 'your_' sao ignorados)")
        return 0

    print(f"Importando {len(env_vars)} credencial(is) do .env para o keyring...")
    print()

    success = 0
    for key, value in env_vars.items():
        if secrets.set_secret(key, value):
            print(f"  OK: {key}")
            success += 1
        else:
            print(f"  ERRO: {key}")

    print()
    print(f"Importadas: {success}/{len(env_vars)}")

    if success > 0:
        print()
        print("IMPORTANTE: Remova os valores sensiveis do arquivo .env!")
        print("Mantenha apenas os valores de exemplo (your_xxx_here).")

    return 0 if success == len(env_vars) else 1


def cmd_status(args):
    """Mostra o status geral do gerenciador de credenciais."""
    secrets = SecretsManager()

    print("=" * 50)
    print("Status do Gerenciador de Credenciais")
    print("=" * 50)
    print()
    print(f"Servico: {SERVICE_NAME}")
    print(f"Keyring disponivel: {'Sim' if secrets.is_keyring_available else 'Nao'}")

    if secrets.is_keyring_available:
        import keyring

        print(f"Backend: {keyring.get_keyring().__class__.__name__}")

    print()
    print("Resumo de credenciais:")
    print("-" * 50)

    keyring_count = 0
    env_count = 0
    missing_count = 0

    for key in KNOWN_SECRETS:
        source = secrets.get_secret_source(key)
        if source == "keyring":
            keyring_count += 1
        elif source == "env":
            env_count += 1
        else:
            missing_count += 1

    print(f"  No keyring: {keyring_count}")
    print(f"  Em variaveis de ambiente: {env_count}")
    print(f"  Nao definidas: {missing_count}")
    print()

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Gerenciador de credenciais para ABInBev Case",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  %(prog)s set AZURE_STORAGE_ACCOUNT_KEY        # Armazena credencial (pede valor)
  %(prog)s set AZURE_STORAGE_ACCOUNT_KEY -v xxx # Armazena com valor especificado
  %(prog)s get AZURE_STORAGE_ACCOUNT_KEY        # Mostra credencial (mascarada)
  %(prog)s get AZURE_STORAGE_ACCOUNT_KEY --show # Mostra credencial completa
  %(prog)s delete AZURE_STORAGE_ACCOUNT_KEY     # Remove credencial
  %(prog)s list                                  # Lista todas as credenciais
  %(prog)s import-from-env                       # Importa do .env para keyring
  %(prog)s status                                # Mostra status geral
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Comando")

    # set
    set_parser = subparsers.add_parser("set", help="Armazena uma credencial")
    set_parser.add_argument("key", help="Nome da credencial")
    set_parser.add_argument("-v", "--value", help="Valor (se nao informado, sera solicitado)")

    # get
    get_parser = subparsers.add_parser("get", help="Obtem uma credencial")
    get_parser.add_argument("key", help="Nome da credencial")
    get_parser.add_argument("--show", action="store_true", help="Mostra valor completo")

    # delete
    delete_parser = subparsers.add_parser("delete", help="Remove uma credencial")
    delete_parser.add_argument("key", help="Nome da credencial")

    # list
    subparsers.add_parser("list", help="Lista credenciais conhecidas")

    # import-from-env
    subparsers.add_parser("import-from-env", help="Importa credenciais do .env")

    # status
    subparsers.add_parser("status", help="Mostra status geral")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "set": cmd_set,
        "get": cmd_get,
        "delete": cmd_delete,
        "list": cmd_list,
        "import-from-env": cmd_import_from_env,
        "status": cmd_status,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
