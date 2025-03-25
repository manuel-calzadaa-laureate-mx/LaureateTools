from collections import deque, defaultdict


class Node:
    def __init__(self, name: str, data: dict, weight: int, parent=None):
        """
        Initialize a node in the DAG.

        :param name: Name of the node (str).
        :param data: Dictionary containing additional data (type, package, owner, name).
        :param weight: Weight of the node (int).
        :param parent: Parent node (Node or None if root).
        """
        self.name = name
        self.data = data if data is not None else {}
        self.weight = weight
        self.parent = parent
        self.children = []

    def add_child(self, child: 'Node', parent: 'Node' = None):
        """
        Add a child node with these rules:
        - If parent is provided, set it as the child's parent
        - Otherwise, set self as the child's parent
        - Add child to children list if not already present
        """
        if child not in self.children:
            self.children.append(child)

        # Set parent
        if parent:
            child.parent = parent
        else:
            child.parent = self

    def __repr__(self):
        return (f"Node(name={self.name}, "
                f"parent={self.parent.name if self.parent else 'ROOT'}, "
                f"children={[c.name for c in self.children]})")

    @property
    def is_direct_child_of_root(self) -> bool:
        """
        Check if this node is a direct child of the root node.
        """
        return self.parent is not None and self.parent.name == "ROOT"

    def get_nodes_levels_bottom_up(self) -> list[list['Node']]:
        """
        Returns nodes grouped by levels, starting from leaves (level 0) to the level before root.
        Each level is a list of nodes at that depth.

        Example:
            ROOT (level 2)
             /   \
          A (1)  B (1)
           |    /   \
          C (0) D (0) E (0)

        Output: [[C, D, E], [A, B]]
        """
        if not self.children:
            return []

        # Step 1: Compute depth of each node (distance from root)
        depth = defaultdict(int)
        queue = deque([self])
        depth[self] = 0  # Root is depth 0

        while queue:
            node = queue.popleft()
            for child in node.children:
                depth[child] = depth[node] + 1
                queue.append(child)

        # Step 2: Group nodes by their depth (excluding root)
        depth_groups = defaultdict(list)
        for node in depth:
            if node != self:  # Exclude root
                depth_groups[depth[node]].append(node)

        # Step 3: Sort depth groups in reverse (from leaves to root)
        max_depth = max(depth_groups.keys()) if depth_groups else 0
        levels = []
        for d in range(max_depth, -1, -1):
            if d in depth_groups:
                levels.append(depth_groups[d])

        return levels

    def get_nodes_sorted_bottom_up(self) -> list['Node']:
        """
        Returns all nodes from leaves (level 0) to the level before root,
        ordered by level (leaves first, then parents, etc.), and sorted by weight within each level.
        """
        levels = self.get_nodes_levels_bottom_up()
        sorted_nodes = []
        for level in levels:
            # Sort nodes in the current level by weight
            level_sorted = sorted(level, key=lambda x: x.weight)
            sorted_nodes.extend(level_sorted)
        return sorted_nodes


def get_or_create_node(name: str, nodes: dict, data: dict = None, weight: int = 0) -> Node:
    """
    Get a node if it exists, otherwise create it.

    :param name: Name of the node (str).
    :param nodes: Dictionary of existing nodes.
    :param data: Dictionary containing additional data (type, package, owner, name).
    :param weight: Weight of the node (int).
    :return: The node (Node) or None if name is empty.
    """
    if not name:  # empty string check
        return None

    if name not in nodes:
        nodes[name] = Node(name, data, weight)
    else:
        if data is not None:
            nodes[name].data = data
        if weight != 0:
            nodes[name].weight = weight

    return nodes[name]
