

from psycopg2.extras import execute_values

# ======================== TABLE CREATION ========================

def create_semantic_nodes_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS semantic_nodes (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        level INTEGER NOT NULL,
        category TEXT NOT NULL
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()

def create_semantic_paper_edges_table(db_client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS semantic_paper_edges (
        ss_id TEXT NOT NULL,
        semantic_node_id INTEGER NOT NULL,
        weight NUMERIC NOT NULL,
        CONSTRAINT fk_concept FOREIGN KEY(semantic_node_id) REFERENCES semantic_nodes(id),
        CONSTRAINT fk_paper FOREIGN KEY(ss_id) REFERENCES papers(ss_id),
        PRIMARY KEY (ss_id, semantic_node_id)
    );
    """
    db_client.execute(create_table_query)
    db_client.commit()


# ======================== INSERTION OPERATIONS ========================

def batch_insert_semantic_nodes(db_client, semantic_nodes): # semantic_nodes = [(name, level, category), ...]
    insert_query = """
    INSERT INTO semantic_nodes (name, level, category)
    VALUES %s;
    """
    
    execute_values(db_client.cur, insert_query, semantic_nodes)
    db_client.commit()

def batch_insert_semantic_paper_edges(db_client, semantic_paper_edges): # semantic_paper_edges = [(ss_id, semantic_node_id, weight), ...]
    insert_query = """
    INSERT INTO semantic_paper_edges (ss_id, semantic_node_id, weight)
    FROM (VALUES %s) AS v(ss_id, semantic_node_id, weight)
    ON CONFLICT (ss_id, semantic_node_id) DO NOTHING;
    """

    execute_values(db_client.cur, insert_query, semantic_paper_edges)
    db_client.commit()




def insert_cleaned_paper(db_client, ss_id, title, abstract, url, search_term=None, num_hops=None):
    if ss_id is None or title is None:
        # print("Invalid paper data")
        # print(ss_id, title, abstract, url)
        return
    
    if abstract is None:
        abstract = "No abstract available"
    
    # print(f"Inserting paper: {ss_id}")
    
    insert_query = """
    INSERT INTO papers (ss_id, title, abstract, url, search_term, num_hops)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (ss_id) DO NOTHING;
    """
    db_client.execute(insert_query, (ss_id, title, abstract, url, search_term, num_hops))
    db_client.commit()

def insert_reference(db_client, ss_id, reference_id):
    # print(f"Inserting reference: {ss_id} -> {reference_id}")
    insert_query = """
    INSERT INTO "references" (ss_id, reference_id)
        VALUES (%s, %s)
        ON CONFLICT (ss_id, reference_id) DO NOTHING;
    """
    db_client.execute(insert_query, (ss_id, reference_id))
    db_client.commit()

def update_is_cleaned(db_client, ss_id):
    update_query = """
    UPDATE papers
    SET is_cleaned = TRUE
    WHERE ss_id = %s;
    """
    db_client.execute(update_query, (ss_id,))
    db_client.commit()

def batch_update_cleaned_papers(db_client, papers):
    # Construct the update query
    update_query = """
    UPDATE papers AS p
    SET clean_title = v.clean_title,
        clean_abstract = v.clean_abstract,
        is_cleaned = v.is_cleaned
    FROM (VALUES %s) AS v(ss_id, clean_title, clean_abstract, is_cleaned)
    WHERE p.ss_id = v.ss_id;
    """
    
    # Execute the batch update
    execute_values(db_client.cur, update_query, papers)
    db_client.commit()
# papers = [
#     ('ss_id_1', 'Cleaned Title 1', 'Cleaned Abstract 1', True),
#     ('ss_id_2', 'Cleaned Title 2', 'Cleaned Abstract 2', True),
#     ('ss_id_3', 'Cleaned Title 3', 'Cleaned Abstract 3', True),
#     # More records
# ]


def batch_insert_concepts(db_client, concepts):
    # Construct the insert query
    insert_query = """
    INSERT INTO concepts (concept)
    VALUES %s
    ON CONFLICT (concept) DO NOTHING
    RETURNING id, concept;
    """
    
    # Execute the batch insert
    execute_values(db_client.cur, insert_query, [(concept,) for concept in concepts])
    db_client.execute("SELECT id, concept FROM concepts WHERE concept = ANY(%s);", (list(concepts),))
    inserted_concepts = db_client.cur.fetchall()
    db_client.commit()
    return {concept: id for id, concept in inserted_concepts}


def batch_insert_concept_edges(db_client, concept_edges):
    # Construct the insert query
    insert_query = """
    INSERT INTO paper_concept_edges (concept_id, ss_id, weight)
    VALUES %s
    ON CONFLICT (ss_id, concept_id) DO NOTHING;
    """
    
    # Execute the batch insert
    execute_values(db_client.cur, insert_query, concept_edges)
    db_client.commit()


# ======================== SELECTION OPERATIONS ========================

def get_all_paper_ids(db_client):
    select_query = """
    SELECT id, ss_id, is_processed FROM papers
    WHERE is_cleaned = TRUE
    ORDER BY id;
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




