from collections import ChainMap
from copy import deepcopy
from functools import partial
from itertools import chain, product
from tempfile import NamedTemporaryFile as ntf
from xml.dom import minidom
import os
import subprocess

import networkx as nx
import spacy


class NotProcessedException(Exception):
    """The text is not processed yet. Process it before to continue."""
    pass


def entity2dict(e):
    attr = e.attributes
    return {
        attr.get('id').value: {
            'name': attr.get('name', attr.get('text', '')).value,
            'semtypes': attr.get('semtypes', '').value.split(',')
        }
    }


class SemRepWrapper:
    _doc = None
    _dom = None

    def __init__(self, text, lang='en'):
        nlp = spacy.load(lang)
        self._doc = nlp(text)

    @property
    def doc(self):
        return self._doc

    @property
    def dom(self):
        if not self._dom:
            self._dom = minidom.parseString(self._raw_processed)
        return self._dom

    def _process_semrep(self, resolved_text):
        cmd = '/opt/public_semrep/bin/'
        cmd += 'semrep.v1.8 -L 2018 -Z 2018AA -X {in_} {out}'

        with ntf(mode='w') as in_, ntf('r+') as out:
            in_.write(resolved_text)
            in_.seek(0)
            cmd = cmd.format(in_=in_.name, out=out.name)
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           shell=True)
            foo = '</SemRepAnnotation>'
            self._raw_processed = out.read()
            self._raw_processed += (
                foo if foo not in self._raw_processed else ''
            )

        return

    def process(self):
        return self._process_semrep(self._doc.text)

    @classmethod
    def load(cls, path):
        """Intence an object from the a semrep output file."""
        with open(path, 'r') as f_in:
            obj = cls('')
            obj._raw_processed = f_in.read()
        return obj

    @property
    def _utterances(self):
        for u in self.dom.getElementsByTagName('Utterance'):
            yield u

    @property
    def utterances(self):
        for u in self._utterances:
            yield u

    @property
    def _entities(self):
        for u in self._utterances:
            yield u.getElementsByTagName('Entity')

    @property
    def entities(self):
        for u in self._entities:
            yield dict(ChainMap(*[entity2dict(e) for e in u]))

    @property
    def _predications(self):
        for u in self._utterances:
            yield u.getElementsByTagName('Predication')

    @property
    def predications(self):
        def _entities_map(u):
            return dict(ChainMap(*[entity2dict(e)
                                   for e in u.getElementsByTagName('Entity')]))

        for u in self._utterances:
            predications = list()
            ents = _entities_map(u)

            for pr in u.getElementsByTagName('Predication'):
                s = ents[pr.getElementsByTagName('Subject')[0].attributes.get(
                    'entityID').value]
                p = pr.getElementsByTagName('Predicate')[0].attributes.get(
                    'type').value
                o = ents[pr.getElementsByTagName('Object')[0].attributes.get(
                        'entityID').value]
                predications.append({
                    'subject': s,
                    'predicate': p,
                    'object': o
                })
            if not predications:
                continue
            yield predications


def ent2node(G, e):
    aux = dict(e)
    name = aux.pop('name')
    G.add_node(name, **aux)
    return name


def _set_graph_attributes(G, edge_attrs={}):
    for k, v in edge_attrs.items():
        nx.set_edge_attributes(G, v, k)


class SemRepGraph(SemRepWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._G_e = nx.MultiDiGraph()
        self._G_p = nx.MultiDiGraph()

    def process(self):
        super().process()
        self.process_graphs()

    def _process_entities_graph(self):
        ents = chain.from_iterable(u.values() for u in self.entities)
        for s, t in product(ents, repeat=2):
            if s == t:
                continue
            self._G_e.add_edge(
                ent2node(self._G_e, s),
                ent2node(self._G_e, t)
            )

    def _process_predications_graph(self):
        for p in chain.from_iterable(self.predications):
            self._G_p.add_edge(
                ent2node(self._G_p, p['subject']),
                ent2node(self._G_p, p['object']),
                type=p['predicate']
            )

    def process_graphs(self):
        self._process_entities_graph()
        self._process_predications_graph()

    def G_entities(self, edge_attrs={}):
        aux = deepcopy(self._G_e)
        _set_graph_attributes(aux, edge_attrs)
        return aux

    def G_predications(self, edge_attrs={}):
        aux = deepcopy(self._G_p)
        _set_graph_attributes(aux, edge_attrs)
        return aux
