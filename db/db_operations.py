
import psycopg2
from psycopg2.extras import execute_values

# ======================== TABLE CREATION ========================

def create_topics_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS topics (
        id SERIAL PRIMARY KEY,
        topic TEXT NOT NULL,
        level INTEGER NOT NULL,
        category TEXT NOT NULL
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_topic_paper_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS topic_paper_edges (
        topic_id INTEGER NOT NULL,
        ss_id TEXT NOT NULL,
        title_similarity NUMERIC NOT NULL,
        abstract_similarity NUMERIC NOT NULL,
        combined_similarity NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(topic_id) REFERENCES topics(id),
        CONSTRAINT fk_paper FOREIGN KEY(ss_id) REFERENCES papers(ss_id),
        PRIMARY KEY (ss_id, topic_id)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_topic_topic_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS topic_topic_edges (
        topic_id_one INTEGER NOT NULL,
        topic_id_two INTEGER NOT NULL,
        weight NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(topic_id_one) REFERENCES topics(id),
        CONSTRAINT fk_paper FOREIGN KEY(topic_id_two) REFERENCES topics(id),
        PRIMARY KEY (topic_id_one, topic_id_two)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_paper_paper_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS paper_paper_edges (
        ss_id_one TEXT NOT NULL,
        ss_id_two TEXT NOT NULL,
        title_similarity NUMERIC NOT NULL,
        abstract_similarity NUMERIC NOT NULL,
        combined_similarity NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(ss_id_one) REFERENCES papers(ss_id),
        CONSTRAINT fk_paper FOREIGN KEY(ss_id_two) REFERENCES papers(ss_id),
        PRIMARY KEY (ss_id_one, ss_id_two)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_scibert_topic_paper_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS scibert_topic_paper_edges (
        topic_id INTEGER NOT NULL,
        ss_id TEXT NOT NULL,
        title_similarity NUMERIC NOT NULL,
        abstract_similarity NUMERIC NOT NULL,
        combined_similarity NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(topic_id) REFERENCES topics(id),
        CONSTRAINT fk_paper FOREIGN KEY(ss_id) REFERENCES papers(ss_id),
        PRIMARY KEY (ss_id, topic_id)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_scibert_topic_topic_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS scibert_topic_topic_edges (
        topic_id_one INTEGER NOT NULL,
        topic_id_two INTEGER NOT NULL,
        weight NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(topic_id_one) REFERENCES topics(id),
        CONSTRAINT fk_paper FOREIGN KEY(topic_id_two) REFERENCES topics(id),
        PRIMARY KEY (topic_id_one, topic_id_two)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_scibert_paper_paper_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS scibert_paper_paper_edges (
        ss_id_one TEXT NOT NULL,
        ss_id_two TEXT NOT NULL,
        title_similarity NUMERIC NOT NULL,
        abstract_similarity NUMERIC NOT NULL,
        combined_similarity NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(ss_id_one) REFERENCES papers(ss_id),
        CONSTRAINT fk_paper FOREIGN KEY(ss_id_two) REFERENCES papers(ss_id),
        PRIMARY KEY (ss_id_one, ss_id_two)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()
# ======================== INSERTION OPERATIONS - BERT ========================

def batch_insert_topics(db_client, topics): # semantic_nodes = [(name, level, category), ...]
    insert_query = """
    INSERT INTO topics (topic, level, category)
    VALUES %s;
    """
    execute_values(db_client.cur, insert_query, topics)
    db_client.commit()

def batch_insert_topic_paper_edges(db_client, logger, topic_paper_edges, chunk_size): # semantic_paper_edges = [(ss_id, semantic_node_id, weight), ...]
    insert_query = """
    INSERT INTO topic_paper_edges (topic_id, ss_id, title_similarity, abstract_similarity, combined_similarity)
    VALUES %s
    ON CONFLICT (ss_id, topic_id) DO NOTHING;
    """
    for i in range(0, len(topic_paper_edges), chunk_size):
        chunk = topic_paper_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_paper_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_paper_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return

def batch_insert_topic_topic_edges(db_client, logger, topic_topic_edges, chunk_size): 
    insert_query = """
    INSERT INTO topic_topic_edges (topic_id_one, topic_id_two, weight)
    VALUES %s
    ON CONFLICT (topic_id_one, topic_id_two) DO NOTHING;
    """
    for i in range(0, len(topic_topic_edges), chunk_size):
        chunk = topic_topic_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_topic_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_topic_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return

def batch_insert_paper_paper_edges(db_client, logger, paper_paper_edges, chunk_size): 
    insert_query = """
    INSERT INTO paper_paper_edges (ss_id_one, ss_id_two, title_similarity, abstract_similarity, combined_similarity)
    VALUES %s
    ON CONFLICT (ss_id_one, ss_id_two) DO NOTHING;
    """
    for i in range(0, len(paper_paper_edges), chunk_size):
        chunk = paper_paper_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(paper_paper_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(paper_paper_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return


# ======================== INSERTION OPERATIONS - SCIBERT ========================

def batch_insert_scibert_topic_paper_edges(db_client, logger, topic_paper_edges, chunk_size): # semantic_paper_edges = [(ss_id, semantic_node_id, weight), ...]
    insert_query = """
    INSERT INTO scibert_topic_paper_edges (topic_id, ss_id, title_similarity, abstract_similarity, combined_similarity)
    VALUES %s
    ON CONFLICT (ss_id, topic_id) DO NOTHING;
    """
    for i in range(0, len(topic_paper_edges), chunk_size):
        chunk = topic_paper_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_paper_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_paper_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return

def batch_insert_scibert_topic_topic_edges(db_client, logger, topic_topic_edges, chunk_size): 
    insert_query = """
    INSERT INTO scibert_topic_topic_edges (topic_id_one, topic_id_two, weight)
    VALUES %s
    ON CONFLICT (topic_id_one, topic_id_two) DO NOTHING;
    """
    for i in range(0, len(topic_topic_edges), chunk_size):
        chunk = topic_topic_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_topic_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(topic_topic_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return

def batch_insert_scibert_paper_paper_edges(db_client, logger, paper_paper_edges, chunk_size): 
    insert_query = """
    INSERT INTO scibert_paper_paper_edges (ss_id_one, ss_id_two, title_similarity, abstract_similarity, combined_similarity)
    VALUES %s
    ON CONFLICT (ss_id_one, ss_id_two) DO NOTHING;
    """
    for i in range(0, len(paper_paper_edges), chunk_size):
        chunk = paper_paper_edges[i:i+chunk_size]
        print("len(chunk):", len(chunk))    
        
        print(f"Inserting chunk {i//chunk_size + 1}/{-(-len(paper_paper_edges)//chunk_size)}: {chunk[:5]}")
        logger.log_message(f"Inserting chunk {i//chunk_size + 1}/{-(-len(paper_paper_edges)//chunk_size)}: {chunk[:5]}")
        
        try:
            psycopg2.extras.execute_values(db_client.cur, insert_query, chunk)
            db_client.commit()  # Commit after each chunk insertion
        except Exception as e:
            print(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            logger.log_message(f"Error inserting chunk {i//chunk_size + 1}: {e}")
            db_client.rollback()  # Rollback the transaction
    
    print("Batch insertion complete.")
    logger.log_message("Batch insertion complete.")
    return


# ======================== SELECTION OPERATIONS ========================

def get_all_papers(db_client):
    select_query = """
    SELECT ss_id, clean_title, clean_abstract FROM papers
    WHERE is_cleaned = TRUE
    ORDER BY id;
    """
    cursor = db_client.execute(select_query)
    return cursor.fetchall()

def get_topics(db_client):
    select_query = """
    SELECT id, topic
    FROM topics;
    """
    cursor = db_client.execute(select_query)
    return cursor.fetchall()    







def get_all_paper_ids_with_params(db_client, search_term, num_hops):
    select_query = """
    SELECT id, ss_id, is_processed 
    FROM papers
    WHERE search_term = %s AND num_hops = %s
    ORDER BY id;
    """
    cursor = db_client.execute(select_query, (search_term, num_hops))
    return cursor.fetchall()

def get_papers_to_clean(db_client, search_term, batch_size):
    select_query = """
    SELECT ss_id, title, abstract, is_cleaned
    FROM papers
    WHERE is_processed = TRUE AND is_cleaned = FALSE AND search_term = %s
    LIMIT %s;
    """
    cursor = db_client.execute(select_query, (search_term, batch_size))
    return cursor.fetchall()




