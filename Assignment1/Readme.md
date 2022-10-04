## 1. Goal
The goal of this assignment is to get used to programmatically querying NCBI and use the multiprocessing.

Pool construct for concurrent programming in Python.

I will use the esearch, elink, efetch functions in Biopython Querying facilities to download the XML data for 10 articles.

## 2. Deliverables

A script called "assignment1.py" that given 1 starting article's Pubmed ID, downloads 10 articles referenced by that first article.

It should do this concurrently from PubMed using the Biopython API.

## 3. How to run it?

The only command-line argument you need is a query PubMed ID to ask Entrez about an article.

The script can be called as follows:

python3 assignment1.py <pubmed_id>

## 4. Output

The script script will run, given a valid Pubmed id, and will return a folder called output with the first 10 citations of the input Pubmed id.
