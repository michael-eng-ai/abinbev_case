"""
Control Module

Modulo para controle de processos, quarentena e metricas.
"""

from .process_control import ProcessControl, QuarantineManager

__all__ = ["ProcessControl", "QuarantineManager"]
