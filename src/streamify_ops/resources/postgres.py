import psycopg
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """
    Connexion PostgreSQL injectee dans les assets qui la demandent.
    Le mot de passe vient de l'environnement, jamais du code.
    """

    host: str = "localhost"
    port: int = 5434
    user: str = "streamify"
    dbname: str = "streamify"
    password: str

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            dbname=self.dbname,
            password=self.password,
            connect_timeout=3,
        )
