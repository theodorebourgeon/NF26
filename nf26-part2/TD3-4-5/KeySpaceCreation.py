import cassandra
import cassandra.cluster

class createKeySpace():
    def __init__(self,KEYSPACE):
        cluster = cassandra.cluster.Cluster()
        session = cluster.connect()
        session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '2' }}
                """)

        session.set_keyspace(KEYSPACE)
    
    def __str__(self):
        return "Keyspace créé"

