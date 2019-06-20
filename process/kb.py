import networkx as nx


def create_kb():
    return nx.MultiDiGraph()


def update_kb(G, g):
    return G.update(g)


def save_gpickle(G, path):
    return nx.write_gpickle(G, path)


def load_gpickle(path):
    return nx.read_gpickle(path)
