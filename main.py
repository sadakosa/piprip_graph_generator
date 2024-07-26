from db.db_client import DBClient
from db import db_operations
from global_methods import load_from_csv, load_yaml_config

from colbert import ColBERT
from semantic_node_loader import SemanticNodeLoader
import time
from logger.logger import Logger



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

    db_operations.create_topics_table(dbclient)
    db_operations.create_topic_paper_edges_table(dbclient)
    db_operations.create_topic_topic_edges_table(dbclient)
    db_operations.create_paper_paper_edges_table(dbclient)

    db_operations.create_scibert_topic_paper_edges_table(dbclient)
    db_operations.create_scibert_topic_topic_edges_table(dbclient)
    db_operations.create_scibert_paper_paper_edges_table(dbclient)

    return dbclient, dbclient_read



def main():
    start_time = time.time()
    dbclient, dbclient_read = setup_db()
    logger = Logger()


    # =========== Load semantic nodes from CSV and insert into the database =========== 
    # sn_loader = SemanticNodeLoader(dbclient, dbclient_read)
    # sn_loader.load_semantic_nodes_from_csv()
    # sn_loader.insert_semantic_nodes()


    # =========== Load papers from database and run through ColBERT together with KW to get embeddings ===========  
    papers = db_operations.get_all_papers(dbclient_read)
    ss_ids = [paper[0] for paper in papers]
    titles = [paper[1] for paper in papers]
    abstracts = [paper[2] for paper in papers]
    combined_texts = [title + ' ' + abstract for title, abstract in zip(titles, abstracts)]
    print(len(ss_ids), len(titles), len(abstracts), len(combined_texts))
    
    topics = db_operations.get_topics(dbclient_read)
    topic_ids = [node[0] for node in topics]
    topics = [node[1] for node in topics]
    print(len(topic_ids), len(topics))

    # got_data = time.time()
    # got_data_runtime = got_data - start_time
    # print("Got data in: ", got_data_runtime)
    # logger.log_message("Got data in: " + str(got_data_runtime))
    
    model_type = 'scibert'
    colbert = ColBERT(logger, model_type)
    colbert.get_topic_paper_embeddings(topics, topic_ids, titles, abstracts, combined_texts, ss_ids)
    # colbert.get_topic_topic_embeddings(topics, topic_ids)
    # colbert.get_paper_paper_embeddings(titles, abstracts, combined_texts, ss_ids)


    # print("Total time to run colbert: ", time.time() - start_time)


    
    # =========== save colbert embeddings to db ===========
    chunk_size = 100000

    if model_type == 'scibert':
        # model_type = 'SciBERT'

        topic_paper_similarities_df = load_from_csv(f'topic_paper_similarities_{model_type}', 'similarities')
        topic_paper_similarities = list(topic_paper_similarities_df.itertuples(index=False, name=None))
        db_operations.batch_insert_scibert_topic_paper_edges(dbclient, logger, topic_paper_similarities, chunk_size)

        # topic_topic_similarities_df = load_from_csv(f'topic_topic_similarities_{model_type}', 'similarities')
        # topic_topic_similarities = list(topic_topic_similarities_df.itertuples(index=False, name=None))
        # db_operations.batch_insert_scibert_topic_topic_edges(dbclient, logger, topic_topic_similarities, chunk_size)

        # paper_paper_similarities_df = load_from_csv(f'paper_paper_similarities_{model_type}', 'similarities')
        # paper_paper_similarities = list(paper_paper_similarities_df.itertuples(index=False, name=None))
        # db_operations.batch_insert_scibert_paper_paper_edges(dbclient, logger, paper_paper_similarities, chunk_size)

        logger.log_message("Saved SciBERT embeddings to database")
        print("Saved SciBERT embeddings to database")
    else:
        topic_paper_similarities_df = load_from_csv(f'topic_paper_similarities_{model_type}', 'similarities')
        topic_paper_similarities = list(topic_paper_similarities_df.itertuples(index=False, name=None))
        db_operations.batch_insert_topic_paper_edges(dbclient, logger, topic_paper_similarities, chunk_size)

        # topic_topic_similarities_df = load_from_csv(f'topic_topic_similarities_{model_type}', 'similarities')
        # topic_topic_similarities = list(topic_topic_similarities_df.itertuples(index=False, name=None))
        # db_operations.batch_insert_topic_topic_edges(dbclient, logger, topic_topic_similarities, chunk_size)

        # paper_paper_similarities_df = load_from_csv(f'paper_paper_similarities_{model_type}', 'similarities')
        # paper_paper_similarities = list(paper_paper_similarities_df.itertuples(index=False, name=None))
        # db_operations.batch_insert_paper_paper_edges(dbclient, logger, paper_paper_similarities, chunk_size)

        logger.log_message("Saved embeddings to database")
        print("Saved embeddings to database")


    



    


if __name__ == '__main__':
    main()