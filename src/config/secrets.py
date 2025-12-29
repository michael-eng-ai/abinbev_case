# ==============================================================================
# ABInBev Case - Secrets Manager Module
# ==============================================================================
#
# Este modulo fornece gerenciamento seguro de credenciais usando o keyring
# do sistema operacional (macOS Keychain, Windows Credential Manager, etc.)
#
# Fallback automatico para variaveis de ambiente quando keyring nao disponivel.
#
# ==============================================================================

import os
from typing import List, Optional

SERVICE_NAME = "abinbev-case"

# Lista de chaves de credenciais conhecidas
KNOWN_SECRETS = [
    "AZURE_STORAGE_ACCOUNT_KEY",
    "OPENMETADATA_TOKEN",
    "HDINSIGHT_PASSWORD",
]


def _keyring_available() -> bool:
    """Verifica se o keyring esta disponivel."""
    try:
        import keyring

        # Tenta uma operacao simples para verificar se funciona
        keyring.get_keyring()
        return True
    except Exception:
        return False


class SecretsManager:
    """
    Gerenciador de credenciais seguro.

    Usa o keyring do sistema operacional para armazenar credenciais de forma segura.
    Fallback automatico para variaveis de ambiente quando keyring nao disponivel.

    Uso:
        secrets = SecretsManager()
        key = secrets.get_secret("AZURE_STORAGE_ACCOUNT_KEY")
        secrets.set_secret("AZURE_STORAGE_ACCOUNT_KEY", "my-secret-key")
    """

    def __init__(self):
        """Inicializa o gerenciador de credenciais."""
        self._keyring_available = _keyring_available()
        if self._keyring_available:
            import keyring

            self._keyring = keyring
        else:
            self._keyring = None

    @property
    def is_keyring_available(self) -> bool:
        """Retorna True se o keyring esta disponivel."""
        return self._keyring_available

    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Obtem uma credencial.

        Ordem de prioridade:
        1. Keyring do sistema (se disponivel)
        2. Variavel de ambiente
        3. Valor padrao

        Args:
            key: Nome da credencial
            default: Valor padrao se nao encontrado

        Returns:
            Valor da credencial ou None
        """
        # Tenta keyring primeiro
        if self._keyring_available:
            try:
                value = self._keyring.get_password(SERVICE_NAME, key)
                if value is not None:
                    return value
            except Exception:
                pass

        # Fallback para variavel de ambiente
        env_value = os.getenv(key)
        if env_value is not None:
            return env_value

        return default

    def set_secret(self, key: str, value: str) -> bool:
        """
        Armazena uma credencial no keyring.

        Args:
            key: Nome da credencial
            value: Valor da credencial

        Returns:
            True se armazenado com sucesso, False caso contrario
        """
        if not self._keyring_available:
            print(f"[WARN] Keyring nao disponivel. Use variaveis de ambiente para '{key}'.")
            return False

        try:
            self._keyring.set_password(SERVICE_NAME, key, value)
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao armazenar credencial '{key}': {e}")
            return False

    def delete_secret(self, key: str) -> bool:
        """
        Remove uma credencial do keyring.

        Args:
            key: Nome da credencial

        Returns:
            True se removido com sucesso, False caso contrario
        """
        if not self._keyring_available:
            print("[WARN] Keyring nao disponivel.")
            return False

        try:
            self._keyring.delete_password(SERVICE_NAME, key)
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao remover credencial '{key}': {e}")
            return False

    def list_secrets(self) -> List[str]:
        """
        Lista as credenciais conhecidas e seu status.

        Returns:
            Lista de nomes de credenciais que tem valor definido
        """
        available = []
        for key in KNOWN_SECRETS:
            if self.get_secret(key) is not None:
                available.append(key)
        return available

    def get_secret_source(self, key: str) -> Optional[str]:
        """
        Retorna a fonte de uma credencial.

        Args:
            key: Nome da credencial

        Returns:
            "keyring", "env", ou None
        """
        # Verifica keyring
        if self._keyring_available:
            try:
                value = self._keyring.get_password(SERVICE_NAME, key)
                if value is not None:
                    return "keyring"
            except Exception:
                pass

        # Verifica variavel de ambiente
        if os.getenv(key) is not None:
            return "env"

        return None


# Singleton global
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """Retorna o gerenciador de credenciais como singleton."""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager


def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Funcao de conveniencia para obter uma credencial.

    Args:
        key: Nome da credencial
        default: Valor padrao se nao encontrado

    Returns:
        Valor da credencial ou None
    """
    return get_secrets_manager().get_secret(key, default)
