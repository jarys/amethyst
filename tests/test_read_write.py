from amethyst import Graph
import random

def blow_graph(graph):
    nodes = [graph.new_node() for _ in range(10)]
    edges = []
    for _ in range(10):
        o, p, s = [random.choice(nodes) for _ in range(3)]
        edges.append(graph.new_edge(o, p, s))
    return nodes, edges

def test_new_and_load_singleton():
    graph = Graph.new("./tests/cache")
    array = graph.as_array()

    graph = Graph.load("./tests/cache")
    assert graph.as_array() == array

def test_new_and_load():
    graph = Graph.new("./tests/cache")
    blow_graph(graph)
    array = graph.as_array()

    graph = Graph.load("./tests/cache")
    assert graph.as_array() == array


def test_setters():
    graph = Graph.new("./tests/cache")

    a, b, c, x, y, z = list(map(graph.new_node, "abcxyz"))
    edge = graph.new_edge(a, b, c, "edge")

    assert edge.as_triple() == (a, b, c)

    # == test object setter ==
    edge.o = x
    array = graph.as_array()
    # reload graph from file
    graph = Graph.load("./tests/cache")
    assert array == graph.as_array()
    # reset locals
    a, b, c, x, y, z = list(map(graph.get, "abcxyz"))
    edge = graph.get("edge")
    assert edge.as_triple() == (x, b, c)

    # == test predicate setter ==
    edge.p = y
    array = graph.as_array()
    # reload graph from file
    graph = Graph.load("./tests/cache")
    assert array == graph.as_array()
    # reset locals
    a, b, c, x, y, z = list(map(graph.get, "abcxyz"))
    edge = graph.get("edge")
    assert edge.as_triple() == (x, y, c)

    # == test subject setter ==
    edge.s = z
    array = graph.as_array()
    # reload graph from file
    graph = Graph.load("./tests/cache")
    assert array == graph.as_array()
    # reset locals
    a, b, c, x, y, z = list(map(graph.get, "abcxyz"))
    edge = graph.get("edge")
    assert edge.as_triple() == (x, y, z)

    # == test data setter ==
    assert a.data == None
    for x in ["Hello World!", 42, True, None, False, 4.5]:
        a.data = x
        # reload graph from file
        graph = Graph.load("./tests/cache")
        # reset locals
        a = graph.get("a")
        assert a.data == x
