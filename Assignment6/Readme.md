## Introduction
https://bioinf.nl/~martijn/master/programming3/assignment6.html

In this assignment, I will develop scikit-learn machine learning models to predict the function of the proteins in the specific dataset. 

This model will use small InterPro_annotations_accession to predict large InterPro_annotations_accession.

The definition of small InterPro_annotations_accession and large InterPro_annotations_accession is defined as below:

If InterPro_annotations_accession's feature length(Stop_location-Start_location) / Sequence_length > 0.9, it is large InterPro_annotations_accession.

Otherwise, it is a small InterPro_annotations_accession.

We can briefly rewrite as:

            |(Stop - Start)|/Sequence >  0.9 --> Large

            |(Stop - Start)|/Sequence <= 0.9 --> small

I will also check the "bias" and "noise" that does not make sense from the dataset.

ie. lines(-) from the TSV file which don't contain InterPRO numbers

ie. proteins which don't have a large feature (according to the criteria above)

## Goal

Predict large InterPro_annotations_accession by small InterPro_annotations_accession.

I will use the dataset from /data/dataprocessing/interproscan/all_bacilli.tsv file on assemblix2012 and assemblix2019. 

However, this file contains ~4,200,000 protein annotations, so I will put a subset of all_bacilli.tsv on GitHub and on local for code testing.

## How it works?

python3 assignment6.py

## Output
Best prediction model, train data, test data
