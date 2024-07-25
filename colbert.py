from transformers import AutoTokenizer, AutoModel
import torch
import pandas as pd


class ColBERT:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained('bert-base-uncased')
        self.model = AutoModel.from_pretrained('bert-base-uncased')

    def get_all_embeddings(self, keywords, titles, abstracts, combined_texts, ss_ids): # these are all lists of strings
        title_embeddings = [self.__get_embeddings(title) for title in titles]
        abstract_embeddings = [self.__get_embeddings(abstract) for abstract in abstracts]
        combined_text_embeddings = [self.__get_embeddings(combined_text) for combined_text in combined_texts]
        keyword_embeddings = [self.__get_embeddings(keyword) for keyword in keywords]

        similarities = {
            'keyword_id': [],
            'paper_ss_id': [],
            'title_similarity': [],
            'abstract_similarity': [],
            'combined_similarity': []
        }

        for i in range(len(keyword_embeddings)):
            for j in range(i, len(paper_embeddings)):
                cossim_title = torch.nn.functional.cosine_similarity(keyword_embeddings[i], paper_embeddings[j])
                cossim_abstract = torch.nn.functional.cosine_similarity(keyword_embeddings[i], paper_embeddings[j])
                cossim_combined = torch.nn.functional.cosine_similarity(keyword_embeddings[i], paper_embeddings[j])
                similarities['keyword_id'].append(keywords[i])
                similarities['paper_ss_id'].append(ss_id[j])
                similarities['title_similarity'].append(cossim_title.item())
                similarities['abstract_similarity'].append(cossim_abstract.item())
                similarities['combined_similarity'].append(cossim_combined.item())

        similarities_df = pd.DataFrame(similarities)

        print(similarities_df) # DataFrame with keywords as columns and papers as rows

        return similarities_df
    
    def __get_embeddings(text):
        inputs = self.tokenizer(text, return_tensors='pt', truncation=True, padding=True)
        with torch.no_grad():
            outputs = self.model(**inputs)
        return outputs.last_hidden_state.mean(dim=1)