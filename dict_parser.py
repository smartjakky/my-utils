class DictParser(object):

    def __init__(self, root):
        self.root = root
        self.output = []
        self.path = []

    def _get_key(self, d):
        return list(d.keys())[0]

    def parse_item(self, parent, key, node):
        self.path.append(node)
        if isinstance(parent, list):
            parent.pop(key)
        elif isinstance(parent, dict):
            del parent[key]
        self.output.append(self.path)
        self.path = []

    def parse_list(self, parent, key, node):
        if not parent[key]:
            del parent[key]
        else:
            self.path.append(node[0])
            if isinstance(node, dict):
                self.parse(node[0], parent=node, key=0)
            elif isinstance(node, list):
                raise Exception('invalid tree')
            else:
                self.parse_item(node, 0, node[0])
                node.pop(0)

    def parse(self, node=None, parent=None, key=None):
        if not node:
            node = self.root
            key = self._get_key(self.root)
        self.path.append(key)
        if len(node.keys()) > 1:
            raise Exception('invalid tree')
        else:
            if isinstance(node[self._get_key(node)], dict):
                self.parse(node[self._get_key(node)], parent=node, key=self._get_key(node))
            elif isinstance(node[self._get_key(node)], list):
                try:
                    while True:
                        self.parse_list(node, self._get_key(node), node[self._get_key(node)])
                except Exception:
                    if parent:
                        del parent[key]
            else:
                self.parse_item(node, self._get_key(node), node[self._get_key(node)])
        return self.output

if __name__ == '__main__':
    d = {'A':[{'b':[1, 2, 3]}, 2, 3]}
    dp = DictParser(d)
    o = dp.parse()
    print(o)
