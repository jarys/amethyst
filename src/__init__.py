import cbor2
import struct
import json


OBJ_HASH_CONSTANT = 0x2b300000

class GraphObject:
    def __init__(self, graph: "Graph", type_name: str, name=None):
        self.graph = graph
        self.ptr = len(graph.objects)
        self.type_name = type_name
        self._name = name
        self._oof = set()
        self._pof = set()
        self._sof = set()

    def __hash__(self):
        return self.ptr ^ OBJ_HASH_CONSTANT

    @property
    def name(self):
        if self._name is not None:
            return self._name
        else:
            return f"{ self.type_name }_{ self.ptr }"

    @name.setter
    def name(self, value: str):
        ptr = self.graph._append_data(value)
        self.graph._write_field(self, 0, ptr)
        self._name = value

    def __repr__(self) -> str:
        return self.name


class Deleted(GraphObject):
    TYPECODE = b"\x00"
    def __init__(self, graph):
        super().__init__(graph, "deleted")


    def json(self):
        return {
            "ptr": self.ptr,
            "type": self.type_name,
        }


class Node(GraphObject):
    TYPECODE = b"\x01"
    def __init__(self, graph, name=None, data=None):
        super().__init__(graph, "node", name)
        name_ptr = graph._append_data(name)
        data_ptr = graph._append_data(data)
        with open(graph.objects_filepath, "ab") as file:
            file.write(Node.TYPECODE)
            file.write(struct.pack("<II", name_ptr, data_ptr))
            file.write(8*b"\x00")  # padding
        graph.objects.append(self)
        graph.obj_dict[self.name] = self
        self._data = data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        ptr = self.graph._append_data(value)
        self.graph._write_field(self, 1, ptr)
        self._data = value

    def __repr__(self) -> str:
        if self._data is not None:
            data_preview = str(self._data)
            if len(data_preview) > 20:
                data_preview = data_preview[:17]+"..."
            return f"{ self.name } ({ data_preview } :: { type(self._data).__name__ })" 
        else:
            return f"{ self.name }"

    def __getattr__(self, name: str):
        if name[-1] == "s":
            return [x.s for x in self._oof if x.p.name == name[:-1]]
        else:
            items = [x.s for x in self._oof if x.p.name == name]
            if not items:
                raise AttributeError("attribute not defined")
            if len(items) > 1:
                raise AttributeError("duplicate definitions")
            return items[0]

    def json(self):
        return {
            "ptr": self.ptr,
            "type": self.type_name,
            "data": self._data,
            "name": self._name,
        }


class Edge(GraphObject):
    TYPECODE = b"\x02"
    def __init__(self, graph: "Graph", o: GraphObject, p: GraphObject, s: GraphObject, name: str | None = None):
        super().__init__(graph, "edge", name)
        o._oof.add(self)
        p._pof.add(self)
        s._sof.add(self)
        name_ptr = graph._append_data(name)
        with open(graph.objects_filepath, "ab") as file:
            file.write(Edge.TYPECODE)
            file.write(struct.pack("<IIII", name_ptr, o.ptr, p.ptr, s.ptr))
        graph.objects.append(self)
        graph.obj_dict[self.name] = self
        self._o = o
        self._p = p
        self._s = s

    @property
    def o(self):
        return self._o

    @o.setter
    def o(self, value: Node) -> None:
        self._o._oof.remove(self)
        value._oof.add(self)
        self.graph._write_field(self, 1, value.ptr)
        self._o = value

    @property
    def p(self):
        return self._p

    @p.setter
    def p(self, value: Node) -> None:
        self._p._pof.remove(self)
        value._pof.add(self)
        self.graph._write_field(self, 2, value.ptr)
        self._p = value

    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, value: Node) -> None:
        self._s._sof.remove(self)
        value._sof.add(self)
        self.graph._write_field(self, 3, value.ptr)
        self._s = value

    def __repr__(self) -> str:
        return f"({ self._o.name } { self._p.name } { self._s.name })[{ self.name }]"

    def json(self):
        return {
            "ptr": self.ptr,
            "type": self.type_name,
            "name": self._name,
            "o_ptr": self._o.ptr,
            "p_ptr": self._p.ptr,
            "s_ptr": self._s.ptr,
        }


class Graph:
    # node = name_pointer (4 bytes) || data_pointer (4 bytes)
    # edge = name_pointer (4 bytes) || ops_poiters (12 bytes)
    # object = type (1 byte) || node/edge (16 bytes)
    OBJECT_LENGTH = 17

    @staticmethod
    def new(objects_filepath: str, data_filepath: str):
        graph = Graph(objects_filepath, data_filepath)
        graph.new_node("none", None)
        return graph

    @staticmethod
    def load(objects_filepath: str, data_filepath: str):
        graph = Graph(objects_filepath, data_filepath)
        graph._load()
        return graph

    def __init__(self, objects_filepath: str, data_filepath: str):
        self.objects_filepath = objects_filepath
        self.data_filepath = data_filepath
        self.objects = []
        self.obj_dict = dict()

    def _load(self):
        with open(self.objects_filepath, "rb") as file:
            while True:
                obj_type = file.read(0)
                if obj_type == 0:  # EOF
                    break
                elif obj_type == Deleted.TYPECODE:
                    file.read(16) # trash padding zeros
                    Deleted(self)
                elif obj_type == Node.TYPECODE:  # node
                    name_ptr, data_ptr = struct.unpack_from("<II", file.read(8))
                    file.read(8)  # trash padding zeros
                    name = self._deref(name_ptr)
                    data = self._deref(data_ptr)
                    Node(self, name, data)
                elif obj_type == Edge.TYPECODE:  # edge
                    name_ptr, o_ptr, p_ptr, s_ptr = struct.unpack_from("<IIII", file.read(16))
                    name = self._deref(name_ptr)
                    assert isinstance(name, str)
                    # assuming that the graph is DAG
                    o, p, s = list(map(self.get, [o_ptr, p_ptr, s_ptr]))
                    Edge(self, o, p, s, name)
                else:
                    ValueError("unknown object type: {}".format(obj_type))

    def _deref(self, pointer: int):
        if pointer == 0:  # shortcut
            return None
        with open(self.data_filepath, "rb") as file:
            file.seek(pointer, 1)
            data = cbor2.load(file)
        return data

    def _append_data(self, data) -> int:
        if data is None:
            return 0
        with open(self.data_filepath, "ab") as file:
            pointer = file.tell()
            cbor2.dump(data, file)
        return pointer

    def get(self, key: int | str):
        if isinstance(key, int):
            return self.objects[key]
        elif isinstance(key, str):
            return self.obj_dict[key]
        else:
            raise KeyError

    def _write_field(self, obj, offset, ptr) -> None:
        with open(self.objects_filepath, "wb") as file:
            file.seek(obj.ptr * self.OBJECT_LENGTH, 1)
            file.seek(1 + 4 * offset, 0)
            file.write(struct.pack("<I", ptr))

    def new_node(self, name: str | None = None, data=None) -> "Node":
        return Node(self, name, data)

    def new_edge(self, o: GraphObject, p: GraphObject, s, name: str | None = None) -> "Edge":
        return Edge(self, o, p, s, name)

    def refragmentize(self) -> None:
        pass

    def json(self):
        return json.dumps([obj.json() for obj in self.objects], indent=4)


if __name__ == "__main__":
    graph = Graph.new("../cache/graph_objects.db", "../cache/graph_data.db")
    nodes = [graph.new_node() for _ in range(10)]
    import random
    edges = []
    for _ in range(10):
        o, p, s = [random.choice(nodes) for _ in range(3)]
        edges.append(graph.new_edge(o, p, s))
    print(graph.json())