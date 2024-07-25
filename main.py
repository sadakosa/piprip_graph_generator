from db.db_client import DBClient
from db import db_operations
from global_methods import load_from_csv, load_yaml_config

from colbert import ColBERT
from semantic_node_loader import SemanticNodeLoader



def setup_db():
    config = load_yaml_config('config/config.yaml')
    rds_db = config['RDS_DB']
    
    # PostgreSQL database connection details
    psql_user = config['PSQL_USER'] if rds_db else config['LOCAL_PSQL_USER']
    psql_password = config['PSQL_PASSWORD'] if rds_db else config['LOCAL_PSQL_PASSWORD']
    psql_host = config['PSQL_HOST'] if rds_db else config['LOCAL_PSQL_HOST']
    psql_port = config['PSQL_PORT'] if rds_db else config['LOCAL_PSQL_PORT']
    psql_read_host = config['PSQL_READ_HOST'] if rds_db else config['LOCAL_PSQL_HOST']

    dbclient = DBClient("postgres", psql_user, psql_password, psql_host, psql_port)
    dbclient_read = DBClient("postgres", psql_user, psql_password, psql_read_host, psql_port)

    db_operations.create_semantic_nodes_table(dbclient)
    db_operations.create_semantic_paper_edges_table(dbclient)

    return dbclient, dbclient_read



def main():
    dbclient, dbclient_read = setup_db()

    sn_loader = SemanticNodeLoader(dbclient, dbclient_read)

    sn_loader.load_semantic_nodes_from_csv()

    sn_loader.insert_semantic_nodes()

    # Define your papers and keywords
    # ss_ids = [1, 2, 3]
    # combined_texts = ["Paper 1 content...", "Paper 2 content...", "Paper 3 content..."]
    # titles = ["Title 1", "Title 2", "Title 3"]
    # abstracts = ["Abstract 1", "Abstract 2", "Abstract 3"]
    # keywords = ["keyword1", "keyword2", "keyword3"]

    # colbert = ColBERT()
    # similarities_df = colbert.get_all_embeddings(self, keywords, titles, abstracts, combined_texts, ss_ids)

    # save_to_csv(similarities_df, 'similarities_test', 'similarities')

    


if __name__ == '__main__':
    main()