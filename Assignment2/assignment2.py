from Bio import Entrez
import multiprocessing.dummy as mpd
import multiprocessing as mp
from time import sleep
import sys, time, os,queue
from multiprocessing.managers import BaseManager, SyncManager
import argparse as ap

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
# PORTNUM = 5381
AUTHKEY = b'whathasitgotinitspocketsesss?'

def runserver(fn, data, ip, port):
    # Start a shared manager server and access its queues
    manager = make_server_manager(ip, port, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn': fn, 'arg': d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()
    print(results)

def make_server_manager(ip, port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager

def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(num_processes, ip, port):
    manager = make_client_manager(ip, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)

def get_reference_list(pubmed_id, db_from="pubmed"):
    """
    :param pubmed_id: str, pubmed id we want to get the article
    :param db_from: str, db
    :return: list of
    """
    Entrez.email = "kaikailin0707@gmail.com"
    key = "93040ced45b4650f365f2c9a9da682381f08"
    record = Entrez.read(Entrez.elink(dbfrom=db_from,
                                      id=pubmed_id,
                                      LinkName="pubmed_pmc_refs",
                                      api_key=key))
    reference_list = []
    for data in record[0]["LinkSetDb"]:
        if data["LinkName"] == "pubmed_pmc_refs":
            reference_data = data["Link"]
            for reference in reference_data:
                reference_list.append(reference["Id"])
    return reference_list

def todo(x):
    """
    :param x: str, reference id
    :return: None
    """
    handle = Entrez.esummary(db="pubmed", id=x, retmode="xml")
    record = Entrez.read(handle)
    author = tuple(record[0]["AuthorList"])
    with open(x + "authors.pickle", "wb") as f:
        pickle.dump(author,f)
    sleep(10)

if __name__ == "__main__":
    argparser = ap.ArgumentParser(
        description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently.",
        epilog="This is the end of the description.")
    argparser.add_argument("-n", action="store",
                           dest="n", required=False, type=int, default=1,
                           help="Number of peons per client to download concurrently.")

    argparser.add_argument("-a", action="store",
                           required=False, type=int, default=10,
                           help="Number of references to download concurrently.")

    argparser.add_argument("pubmed_id", action="store", type=str ,nargs=1,
                           help="Pubmed ID of the article to harvest for references to download.")

    argparser.add_argument("-p" ,"--port", action="store", type=int, default= 4235,
                           help="Portnumber")

    argparser.add_argument("-H","--host", action="store",
                           help="Serverhost")

    group = argparser.add_mutually_exclusive_group()
    group.add_argument("-s", "--server", action="store_true",
                       help="This is for server.")
    group.add_argument("-c", "--client", action="store_true",
                       help="This is for client.")
    args = argparser.parse_args()
    print("Getting: ", args.pubmed_id)
    file = args.pubmed_id
    ids = get_reference_list(pubmed_id=file)[:args.a]
    server = mp.Process(target=runserver, args=(todo, ids, args.host,args.port))
    server.start()
    time.sleep(1)
    client = mp.Process(target=runclient, args=(args.n,args.host,args.port))
    client.start()
    server.join()
    client.join()

