## 1. Goal
The goal of this assignment is to download and analyze the NCBI articles from different "remote" clients

For the time being, start clients by hand using the "ssh" command to connect terminals to different computers on the BIN network.

I will use the esearch, elink, efetch functions in Biopython Querying facilities to download the XML data for 10 articles.

## 2. Deliverables

The script needs to download "numberofarticles" which are referenced by "STARTINGPUBMEDID" using "numberofchildren" processes on the "hosts" computers.
(So, like assignment1, but now from multiple computers). Each child can write to the same folder on the NFS filesystem (your home, or somewhere on the /commons).
The script needs to analyze the XML of each of the references further to extract all the authors of the article. 
It should save the authors in a Python tuple and use the Pickle module to save it to the disk as output/PUBMED_ID.authors.pickle where PUBMEDID is of course the pubmed ID of the article in question.

        assignment2.py -n <number_of_peons_per_client> [-c | -s] --port <portnumber> --host <serverhost> -a <number_of_articles_to_download> STARTING_PUBMED_ID

NB1: if you only specify "localhost" then all processes are run on one computer (good for testing)

NB2: if you want to run truly networked, specify at least two hosts: the first one is assumed to run the server process, all hosts after that are clients

NB3: you need to both save the downloaded xml files and communicate the reference PUBMEDs back to the server
