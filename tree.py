class Node(object):
    visited = None
    data = None
    children = None

    def __init__(self, value):
        self.children = []
        self.visited = False
        self.value = value


class Tree(object):
    def __init__(self, root=None):
        self.root = root

    @staticmethod
    def get_unvisited_child(self, tree_node):
        for node in tree_node.children:
            if not node.visited:
                return node
        return

    @staticmethod
    def add_children(self, tree_node, tree_child):
        tree_node.children.add(tree_child)

    def parse_dict(self, di):
        self.root = Node(list(di.keys()[0]))
        pass

    def generate_by_dict(self, di):
        self.parse_dict(di)
