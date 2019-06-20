from functools import partial
from multiprocessing import Manager, Pool, Process

from uniko import calculate_reputation_article
from load.kb import create_kb
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


def update_graph_worker(Q, update_graph):
    G = create_kb()
    while True:
        g = Q.get()
        if g is None:
            Q.put(G)
            return
        update_graph(G, g)


def process_docs_from_iterable(docs, update_graph, n=NUMBER_OF_PROCESS):
    Q = Manager().Queue()
    update_graph_proc = Process(target=update_graph_worker,
                                args=(Q, update_graph))
    update_graph_proc.start()
    with Pool(n) as p:
        p.starmap_async(partial(process_worker, Q), docs)
        p.close()
        p.join()
        Q.put(None)
        update_graph_proc.join()
    return Q.get()
