"""
Główny moduł CLI - deleguje do zaawansowanego interfejsu.
"""

from .advanced import run as run_advanced


def run():
    """Entry point - używa zaawansowanego CLI."""
    run_advanced()


