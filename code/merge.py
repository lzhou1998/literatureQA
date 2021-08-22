import indexed_dataset
import os

builder = indexed_dataset.IndexedDatasetBuilder('preprocess/merge.bin')
for filename in os.listdir("preprocess/data"):
    if filename[-4:] == '.bin':
        builder.merge_file_("preprocess/data/"+filename[:-4])
builder.finalize("preprocess/merge.idx")