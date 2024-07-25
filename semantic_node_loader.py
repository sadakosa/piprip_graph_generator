from global_methods import *
from db import db_operations
import re

class SemanticNodeLoader:
    def __init__(self, db_client, dbclient_read):
        self.db_client = db_client
        self.dbclient_read = dbclient_read
        self.semantic_nodes = []
    
    def load_semantic_nodes_from_csv(self):
        for i in range(1, 38):
            file_name = '2024-07-25 - Search Terms - ' + str(i)
            folder_name = 'semantic_nodes'
            
            temp_df = load_from_csv(file_name, folder_name)

            if temp_df is not None:
                temp_df.iloc[:, 0] = temp_df.iloc[:, 0].apply(self.__remove_number_in_string)
                category = temp_df.iloc[0, 0]
                
                print(f"Category: {category}")
                temp_df['category'] = category

                # print the second column of the database if any of the values are not an integer
                if not temp_df.iloc[:, 1].apply(lambda x: isinstance(x, int)).all():
                    print("Non-integer values in the second column:")
                    print(temp_df.iloc[:, 1])

                self.semantic_nodes.append(temp_df.values.tolist())



    def insert_semantic_nodes(self): # 2841 nodes
        for semantic_nodes in self.semantic_nodes:
            db_operations.batch_insert_topics(self.db_client, semantic_nodes)
            print(f"Inserted {len(semantic_nodes)} semantic nodes")

    def __remove_number_in_string(self, text):
        pattern = r'\d+\. |: Computer Science-Based Breakdown'
        cleaned_text = re.sub(pattern, '', text)
        return cleaned_text

