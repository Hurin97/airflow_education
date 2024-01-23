import postgresql

class Database:
    def __init__(self, cfg) -> None:
        self.hostname = cfg['postgresql']['hostname']
        self.username = cfg['postgresql']['username']
        self.password = cfg['postgresql']['password']
        self.database_name = cfg['postgresql']['db_name']
        self.port = cfg['postgresql']['port']

    def get_connection(self):
        database_connection = postgresql.open(f"pq://{self.username}:{self.password}@{self.hostname}/{self.database_name}",
                                              port = self.port)
        return database_connection