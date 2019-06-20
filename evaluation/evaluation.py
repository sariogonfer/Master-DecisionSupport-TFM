import networkx as nx
import numpy as np

from semrep.semrep import SemRepGraph


DISEASES_SEMTYPES = [
    'acab', 'anab', 'comd', 'cgab', 'dsyn', 'emod',
    'inpo', 'mobd', 'neop', 'patf'
]


def _avg(l):
    return sum(l) / len(l)


def quantile_3er(l):
    return np.quantile(l, .75, interpolation='higher')


def reputation_degree(G, g, key='reputation'):
    diseases = get_diseases_in_graph(g)
    aux = []
    for d in diseases:
        if d not in G.nodes:
            continue
        foo = [e[key] for n in g[d] if n in G[d] for e in G[d][n].values()]
        aux.append(_avg(foo))

    return _avg(aux) if aux else 0


def common_nodes_degree(G, g):
    diseases = get_diseases_in_graph(g)
    aux = []
    for d in diseases:
        if d not in G.nodes:
            continue
        G_d = nx.ego_graph(G, d)
        aux.append(len(set(G_d.nodes) & set(g.nodes)) / len(set(g.nodes)))
    return _avg(aux) if aux else 0


def get_intensity_relation(G, s, t):
    return G.number_of_edges(s, t)


def get_intensity_disease(G, disease):
    i_d = sorted(
        G[disease], key=lambda x: get_intensity_relation(G, disease, x),
        reverse=True)[0]
    i_d = G.number_of_edges(disease, i_d)
    return i_d


def get_diseases_in_graph(G):
    return [
        n for n in G.nodes
        if set(G.nodes[n]['semtypes']) & set(DISEASES_SEMTYPES)
    ]


def intensity_degree(G, g):
    diseases = get_diseases_in_graph(g)
    aux = []
    for d in diseases:
        if d not in G.nodes:
            continue
        G_d = nx.ego_graph(G, d)
        i_d = get_intensity_disease(G_d, d)
        common_terms = set(g.nodes) & set(G_d.nodes)
        i_r = [get_intensity_relation(G_d, d, n) / i_d for n in common_terms]
        aux.append(sum(i_r) / len(common_terms))
    return _avg(aux) if aux else 0


def process_text(text):
    sr = SemRepGraph(text)
    sr.process()

    return sr.G_entities()


def evaluate_text(G, text, alpha=.5):
    G_text = process_text(text)
    confidence = (
        alpha * common_nodes_degree(G, G_text) +
        (1 - alpha) * intensity_degree(G, G_text)
    )
    reputation = reputation_degree(G, G_text)

    return confidence, reputation
