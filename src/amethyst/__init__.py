import cbor2
import struct
import json

OBJ_HASH_CONSTANT = 0x2b30000000000000

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
        ptr = self.graph._alloc(value)
        self.graph._write_field(self, 0, ptr)
        self._name = value

    def __repr__(self) -> str:
        return self.name


class Deleted(GraphObject):
    TYPECODE = b"\x00"
    def __init__(self, graph):
        super().__init__(graph, "deleted")

    @staticmethod
    def load(_obj_data, graph):
        return Deleted(graph)

    def append(self, graph):
        """This seems to be useless"""
        with open(graph.objects_filepath, "wb") as file:
            file.seek(self.ptr * 17, 1)
            file.write(Deleted.TYPECODE)
            file.write(16*b"\x00")  # padding
        graph.objects.append(self)
        graph.obj_dict[self.name] = self

    def as_dict(self):
        return {
            "ptr": self.ptr,
            "type": self.type_name,
        }


class Node(GraphObject):
    TYPECODE = b"\x01"
    def __init__(self, graph, name=None, data=None):
        super().__init__(graph, "node", name)
        self._data = data

    @staticmethod
    def load(obj_data, graph) -> "Node":
        name_ptr, data_ptr = struct.unpack_from("<II", obj_data[:8])
        name = graph._deref(name_ptr)
        data = graph._deref(data_ptr)
        return Node(graph, name, data)

    def append(self, graph):
        print("appending a node")
        name_ptr = graph._alloc(self._name)
        data_ptr = graph._alloc(self._data)
        with open(graph.objects_filepath, "ab") as file:
            print("file.tell() = ", file.tell())
            file.write(Node.TYPECODE)
            file.write(struct.pack("<II", name_ptr, data_ptr))
            file.write(8*b"\x00")  # padding
        graph.objects.append(self)
        graph.obj_dict[self.name] = self

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        ptr = self.graph._alloc(value)
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

    def as_dict(self):
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
        self._o = o
        self._p = p
        self._s = s
        o._oof.add(self)
        p._pof.add(self)
        s._sof.add(self)

    @staticmethod
    def load(obj_data, graph):
        name_ptr, o_ptr, p_ptr, s_ptr = struct.unpack_from("<IIII", obj_data)
        name = graph._deref(name_ptr)
        assert name is None or isinstance(name, str)
        # assuming that the graph is DAG
        o, p, s = list(map(graph.get, [o_ptr, p_ptr, s_ptr]))
        return Edge(graph, o, p, s, name)

    def append(self, graph):
        print("appedning an edge")
        name_ptr = graph._alloc(self._name)
        with open(graph.objects_filepath, "ab") as file:
            print("file.tell() = ", file.tell())
            file.write(Edge.TYPECODE)
            file.write(struct.pack("<IIII", name_ptr, self._o.ptr, self._p.ptr, self._s.ptr))
        graph.objects.append(self)
        graph.obj_dict[self.name] = self

    @property
    def o(self):
        return self._o

    @o.setter
    def o(self, value: GraphObject) -> None:
        self._o._oof.remove(self)
        value._oof.add(self)
        self.graph._write_field(self, 1, value.ptr)
        self._o = value

    @property
    def p(self):
        return self._p

    @p.setter
    def p(self, value: GraphObject) -> None:
        self._p._pof.remove(self)
        value._pof.add(self)
        self.graph._write_field(self, 2, value.ptr)
        self._p = value

    @property
    def s(self):
        return self._s

    @s.setter
    def s(self, value: GraphObject) -> None:
        self._s._sof.remove(self)
        value._sof.add(self)
        self.graph._write_field(self, 3, value.ptr)
        self._s = value

    def __repr__(self) -> str:
        return f"({ self._o.name } { self._p.name } { self._s.name })[{ self.name }]"

    def as_dict(self):
        return {
            "ptr": self.ptr,
            "type": self.type_name,
            "name": self._name,
            "o_ptr": self._o.ptr,
            "p_ptr": self._p.ptr,
            "s_ptr": self._s.ptr,
        }

    def as_triple(self):
        return (self.o, self.p, self.s)


class Graph:
    # node = name_pointer (4 bytes) || data_pointer (4 bytes)
    # edge = name_pointer (4 bytes) || ops_poiters (12 bytes)
    # object = type (1 byte) || node/edge (16 bytes)
    OBJECT_LENGTH = 17

    @staticmethod
    def new(storage_path: str):
        graph = Graph(storage_path)
        open(graph.objects_filepath, "wb").close()
        open(graph.data_filepath, "wb").close()
        return graph

    @staticmethod
    def load(storage_path: str):
        graph = Graph(storage_path)
        graph._load()
        return graph

    def __init__(self, storage_path: str):
        self.objects_filepath = storage_path + "/objects.dat"
        self.data_filepath = storage_path + "/data.dat"
        self.objects = []
        self.obj_dict = dict()
        self.data_cache = dict()

    def _load(self):
        with open(self.objects_filepath, "rb") as file:
            while True:
                obj_type = file.read(1)
                if obj_type == b"":  # EOF
                    break
                obj_data = file.read(16)
                if obj_type == Deleted.TYPECODE:  # deleted
                    obj = Deleted.load(obj_data, self)
                elif obj_type == Node.TYPECODE:  # node
                    obj = Node.load(obj_data, self)
                elif obj_type == Edge.TYPECODE:  # edge
                    obj = Edge.load(obj_data, self)
                else:
                    ValueError("unknown object type: {}".format(obj_type))
                
                self.objects.append(obj)
                self.obj_dict[obj.name] = obj

    def _deref(self, ptr: int):
        with open(self.data_filepath, "rb") as file:
            file.seek(ptr, 1)
            data = cbor2.load(file)
        return data

    def _alloc(self, data) -> int:
        if data in self.data_cache:
            return self.data_cache[data]

        with open(self.data_filepath, "ab") as file:
            ptr = file.tell()
            cbor2.dump(data, file)
        self.data_cache[data] = ptr
        #print("allocated", data, "at", ptr)
        return ptr

    def get(self, key: int | str):
        if isinstance(key, int):
            return self.objects[key]
        elif isinstance(key, str):
            return self.obj_dict[key]
        else:
            raise KeyError

    def _write_field(self, obj, offset, ptr) -> None:
        with open(self.objects_filepath, "rb+") as file:
            file.seek(obj.ptr * Graph.OBJECT_LENGTH + 1 + 4 * offset, 1)
            file.write(struct.pack("<I", ptr))

    def new_node(self, name: str | None = None, data=None) -> "Node":
        node = Node(self, name, data)
        node.append(self)
        return node

    def new_edge(self, o: GraphObject, p: GraphObject, s, name: str | None = None) -> "Edge":
        edge = Edge(self, o, p, s, name)
        edge.append(self)
        return edge

    def new_data(self, data):
        return self.new_node(None, data)

    def refragmentize(self) -> None:
        raise NotImplemented

    def as_array(self):
        return [obj.as_dict() for obj in self.objects]

    def print_db(self):
        with open(self.objects_filepath, "rb") as file:
            while True:
                t = file.read(1)
                if t == b"":
                    break
                print(t, *struct.unpack("<IIII", file.read(16)))