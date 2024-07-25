from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd
import time
from global_methods import save_to_csv

class ColBERT:
    def __init__(self, logger):
        self.tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
        self.model = AutoModel.from_pretrained('bert-base-uncased')
        self.logger = logger

    def get_topic_paper_embeddings(self, topics, topic_ids, titles, abstracts, combined_texts, ss_ids): # these are all lists of strings
        start_time = time.time()
        title_embeddings = [self.__get_embeddings(title) for title in titles]
        abstract_embeddings = [self.__get_embeddings(abstract) for abstract in abstracts]
        combined_text_embeddings = [self.__get_embeddings(combined_text) for combined_text in combined_texts]
        topic_embeddings = [self.__get_embeddings(topic) for topic in topics]

        similarities = {
            'topic_id': [],
            'paper_ss_id': [],
            'title_similarity': [],
            'abstract_similarity': [],
            'combined_similarity': []
        }

        for i in range(len(topic_embeddings)):
            for j in range(i, len(title_embeddings)):
                cossim_title = torch.nn.functional.cosine_similarity(topic_embeddings[i], title_embeddings[j])
                cossim_abstract = torch.nn.functional.cosine_similarity(topic_embeddings[i], abstract_embeddings[j])
                cossim_combined = torch.nn.functional.cosine_similarity(topic_embeddings[i], combined_text_embeddings[j])
                similarities['topic_id'].append(topic_ids[i])
                similarities['paper_ss_id'].append(ss_ids[j])
                similarities['title_similarity'].append(cossim_title.item())
                similarities['abstract_similarity'].append(cossim_abstract.item())
                similarities['combined_similarity'].append(cossim_combined.item())

        similarities_df = pd.DataFrame(similarities)

        print(similarities_df) # DataFrame with topics as columns and papers as rows

        # save to csv
        save_to_csv(similarities_df, 'topic_paper_similarities', 'similarities')
        self.logger.log_message("Saved topic-paper similarities to CSV")
        end_time = time.time()
        print("Time taken to get topic-paper similarities: ", end_time - start_time)

        # save to database
        # db_operations.batch_insert_similarities(dbclient, similarities_df)


        return similarities_df

    def get_topic_topic_embeddings(self, topics, topic_ids): # these are all lists of strings
        start_time = time.time()    
        topic_embeddings = [self.__get_embeddings(topic) for topic in topics]

        similarities = {
            'topic_id_one': [],
            'topic_id_two': [],
            'similarity': [],
        }

        for i in range(len(topic_embeddings)):
            for j in range(i, len(topic_embeddings)):
                cossim = torch.nn.functional.cosine_similarity(topic_embeddings[i], topic_embeddings[j])
                similarities['topic_id_one'].append(topic_ids[i])
                similarities['topic_id_two'].append(topic_ids[j])
                similarities['similarity'].append(cossim.item())

        similarities_df = pd.DataFrame(similarities)

        save_to_csv(similarities_df, 'topic_topic_similarities', 'similarities')

        self.logger.log_message("Saved topic-topic similarities to CSV")
        end_time = time.time()
        print("Time taken to get topic-topic similarities: ", end_time - start_time)
        self.logger.log_message("Time taken to get topic-topic similarities: " + str(end_time - start_time))

        print(similarities_df)

    def get_paper_paper_embeddings(self, titles, abstracts, combined_texts, ss_ids): # these are all lists of strings
        start_time = time.time()
        title_embeddings = [self.__get_embeddings(title) for title in titles]
        abstract_embeddings = [self.__get_embeddings(abstract) for abstract in abstracts]
        combined_text_embeddings = [self.__get_embeddings(combined_text) for combined_text in combined_texts]

        similarities = {
            'paper_ss_id1': [],
            'paper_ss_id2': [],
            'title_similarity': [],
            'abstract_similarity': [],
            'combined_similarity': []
        }

        for i in range(len(title_embeddings)):
            for j in range(i, len(title_embeddings)):
                cossim_title = torch.nn.functional.cosine_similarity(title_embeddings[i], title_embeddings[j])
                cossim_abstract = torch.nn.functional.cosine_similarity(abstract_embeddings[i], abstract_embeddings[j])
                cossim_combined = torch.nn.functional.cosine_similarity(combined_text_embeddings[i], combined_text_embeddings[j])
                similarities['paper_ss_id1'].append(ss_ids[i])
                similarities['paper_ss_id2'].append(ss_ids[j])
                similarities['title_similarity'].append(cossim_title.item())
                similarities['abstract_similarity'].append(cossim_abstract.item())
                similarities['combined_similarity'].append(cossim_combined.item())

        similarities_df = pd.DataFrame(similarities)
        save_to_csv(similarities_df, 'paper_paper_similarities', 'similarities')

        self.logger.log_message("Saved paper-paper similarities to CSV")
        end_time = time.time()
        print("Time taken to get paper-paper similarities: ", end_time - start_time)
        self.logger.log_message("Time taken to get paper-paper similarities: " + str(end_time - start_time))

        print(similarities_df)
    
    def __get_embeddings(self, text):
        inputs = self.tokenizer(text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        return outputs.last_hidden_state.mean(dim=1)