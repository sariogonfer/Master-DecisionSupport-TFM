from functools import partial
from multiprocessing import Manager, Pool, Process

from uniko import calculate_reputation_article
from process.kb import create_kb
from semrep.semrep import SemRepGraph


NUMBER_OF_PROCESS = 32


def process_doc(doi, text):
    reputation = calculate_reputation_article(doi)
    sr = SemRepGraph(text)
    sr.process()

    return sr.G_entities(edge_attrs={'doi': doi, 'reputation': reputation})


def process_worker(Q, doi, text):
    g = process_doc(doi, text)
    Q.put(g)
    return True


def callback_worker(Q, callback):
    G = create_kb()
    while True:
        g = Q.get()
        if g is None:
            Q.put(G)
            return
        callback(G, g)


def process_docs_from_iterable(docs, callback):
    Q = Manager().Queue()
    callback_proc = Process(target=callback_worker, args=(Q, callback))
    callback_proc.start()
    with Pool(NUMBER_OF_PROCESS) as p:
        p.starmap_async(partial(process_worker, Q), docs)
        p.close()
        p.join()
        Q.put(None)
        callback_proc.join()
    return Q.get()
