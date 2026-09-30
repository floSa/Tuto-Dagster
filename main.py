"""Point d'entrée du tutoriel Dagster (Tuto-Dagster)."""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("tuto-dagster")


def main() -> None:
    """Affiche les instructions de démarrage rapide du pipeline Dagster."""
    logger.info("=== Bienvenue dans Tuto-Dagster ===")
    logger.info("Pipeline d'apprentissage Dagster : Streamify-Ops")
    logger.info("Pour démarrer le serveur de développement web Dagster :")
    logger.info("  uv run dagster dev -m streamify_ops")
    logger.info("Pour exécuter la suite de tests :")
    logger.info("  uv run pytest")


if __name__ == "__main__":
    main()
