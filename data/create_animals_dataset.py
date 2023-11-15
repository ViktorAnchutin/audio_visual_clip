import json
import pandas as pd

df_train = pd.read_csv("datasets/audioset_train_strong.tsv", sep='\t')
df_eval = pd.read_csv("datasets/audioset_eval_strong.tsv", sep='\t')
ont = json.load(open("datasets/ontology.json"))
ontology = pd.DataFrame(ont)

def getIds(df,id):
    item = df[df.id==id]
    children = item.child_ids.values[0]
    res = [id]
    if len(children)==0:
        return res
    for ch in children:
        res.extend(getIds(df,ch))
    return res
        
animal_ids = getIds(ontology,'/m/0jbk')
#vgg_df = pd.read_csv('vggsound.csv', header=None, names=['segment_id','start_time_seconds','label','split'])


animals_df_train = df_train[df_train.label.isin(animal_ids)].copy()
animals_df_eval = df_eval[df_eval.label.isin(animal_ids)].copy()

id_to_label = {id:ontology[ontology.id==id].name.values[0] for id in set(animal_ids)}

animals_df_train.loc[:,'label_name'] = animals_df_train['label'].apply(lambda id: id_to_label[id])
animals_df_eval.loc[:,'label_name'] = animals_df_eval['label'].apply(lambda id: id_to_label[id])
animals_df_train.loc[:,'len'] = animals_df_train['end_time_seconds'] - animals_df_train['start_time_seconds']
animals_df_eval.loc[:,'len'] = animals_df_eval['end_time_seconds'] - animals_df_eval['start_time_seconds']

animals_df_train = animals_df_train.drop(animals_df_train[animals_df_train.len < 0.3].index)
animals_df_eval = animals_df_eval.drop(animals_df_eval[animals_df_eval.len < 0.3].index)

animals_df_train.to_csv('datasets/animals_dataset_train.csv')
animals_df_eval.to_csv('datasets/animals_dataset_eval.csv')