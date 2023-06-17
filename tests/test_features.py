from amethyst import Graph

def test_attribute_getters():
    graph = Graph.new("./tests/cache")

    age = 23
    person = graph.new_node("david_novak")

    graph.new_edge(
        person,
        graph.new_node("age"),
        graph.new_data(age),
    )

    assert person.age.data == age