# Literature QA

Source code and dataset for "LiteratureQA: A Question Answering Corpus with Graph Knowledge on Academic Literature".
This repo is partially based on [BERT](https://github.com/google-research/bert), [Huggingface-transformers](https://github.com/huggingface/pytorch-pretrained-BERT) and [ERNIE](https://github.com/thunlp/ERNIE), and we thank for their great works.

## Requirements
+ Python3.7
+ Pytorch>=0.4.1
+ tqdm
+ boto3
+ requests
+ pymysql
+ tensorflow2
+ nltk
+ tensorboardX

Due to some reasons, we use tensorflow2 to process data in some scripts.

Quick install by following command.
```
pip install -r requirement.txt
```

## Prepare Graph Embeddings And Pre-train Data

### 1. Build KG 
Note that a large version KG with 35x number of papers is also provided, set the IS_LARGE into true in build_kg.py to activate.
This is not recommended because it will notably increase the time consumation of graph embedding process.

```
cd preprocess
python build_kg.py
```

### 2. Compile training execute file of transE
To speed up graph embedding process, we use the highly optimization version of transE from [Fast-TransX](https://github.com/thunlp/Fast-TransX).
In some cases, it may occur warnings when compile, and you can ignore them.

```
g++ transE.cpp -o transE -pthread -march=native
```

### 3. Run graph embedding by transE
It usually takes about 10 ~ 20 hours. 

```
./transE -size 100 -input ./acekg/ -output ./acekg_embed/ -thread 32 -epochs 10000 -alpha 0.001 -margin 2.0
```

### 4. Build corpus
A multi-thread version is also provided.
```
# python build_corpus_mthreads.py
python build_corpus.py
```

### (Optional) 5. Annotate LiteratureQA dataset
This can be skip because we have already put annotated files into code/dataset.
```
python annotate_dataset.py
```

### 6. Create ids
```
# with 4 processes
cd ..
python preprocess/create_ids.py 4
```

### 7. Create instances
```
# with 4 processes
python preprocess/create_insts.py 4
```

### 8. Merge instances
```
python code/merge.py
```

## Pre-train and Finetune
### 1. Download the pretrain model 
[Download](https://drive.google.com/drive/folders/176oFcnH-aRbEqsdJWsXoeLtkFt-o5a_S) the (12/768 BERT-Base) binary file and move it to code/models/pretrain_base.

### 2. Re-pretrain from BERT-base
```
python code/run_pretrain.py --do_train --data_dir preprocess/merge --bert_model code/models/pretrain_base --output_dir code/models/pretrain_out/ --task_name pretrain --max_seq_length 256
```

### 3. Finetune for QA task on LiteratureQA dataset
```
cp code/models/pretrain_base/{*.txt,*.json} code/models/pretrain_out/
python code/run_qa.py --do_train
```

### 4. Run prediction on the test set
Uncomment and run the first line if no config files in the model folder.
```
# cp code/models/pretrain_base/{*.txt,*.json} code/models/qa_out/
python code/run_qa.py --do_predict --qa_model code/models/qa_out
```

### 5. Evaluate the prediction result of LiteratureQA test set to reproduce the F1 & EM score
```
cd code
python evaluate.py dataset/test.json models/qa_out/predictions.json
```

## (Optional) Download the finetuned model
You can also reproduce the results by [downloading](https://drive.google.com/drive/folders/176oFcnH-aRbEqsdJWsXoeLtkFt-o5a_S) our finetuned models and trained graph embeddings (quick_check.zip), and then follow the steps. This can save your time.

### 1. Unzip and move the files
Unzip the zip file, move the folders to corresponding location (code/models/qa_out, preprocess/acekg, preprocess/acekg_embed) and cover the original folders (if have).

### 2. Run prediction on the test set and Evaluate the prediction result

```
python code/run_qa.py --do_predict --qa_model code/models/qa_out
cd code
python evaluate.py dataset/test.json models/qa_out/predictions.json
```

The EM/F1(%) should be 67.63/72.75 as shown in the paper.

## About the dataset
Currently only the old version literatureQA dataset that includes 7,000 abstracts and 140,000 QA pairs is provided at this repo, and you can find it in code/raw_dataset/.
