from collections import deque
from typing import List, Dict
from typing import Optional


class Node:
    def __init__(self, name: str, data: Optional[dict] = None, parent: 'Node' = None):
        """
        Initialize a node in the DAG.

        :param name: Name of the node (str).
        :param data: Dictionary containing additional data (type, package, owner, name).
        :param parent: Parent node (Node or None if root).
        """
        self.name = name
        self.data = data or {}
        self.parent = parent  ## 1:1 relation
        self.dependencies: List['Node'] = []  ## 1:many relation
        self.reverse_dependency: List['Node'] = []
        self.level = 1

    def add_dependency(self, dependency: 'Node', parent: Optional['Node'] = None) -> None:
        """
        Add a dependency node with these rules:
        - If parent is provided, set it as the dependency's parent
        - Otherwise, set self as the dependency's parent
        - Add dependency to dependencies list if not already present
        """
        if dependency not in self.dependencies:
            self.dependencies.append(dependency)
            dependency.reverse_dependency.append(self)

        # Set parent (if provided, else use self)
        dependency.parent = parent or self

        # Compute depth of dependency (distance from ROOT)
        depth = 0
        current = dependency.parent
        while current and current.name != "ROOT":
            depth += 1
            current = current.parent

    def __repr__(self):
        return (f"Node(name={self.name}, "
                f"data={self.data},"
                f"parent={self.parent.name if self.parent else 'ROOT'}, "
                f"dependencies={[c.name for c in self.dependencies]},"
                f"level={self.level})")

    @property
    def is_direct_child_of_root(self) -> bool:
        """
        Check if this node is a direct dependency of the root node.
        """
        return self.parent is not None


def get_or_create_node(name: str, nodes: Dict[str, Node], data: Optional[dict] = None) -> Optional[
    Node]:
    """
    Get a node if it exists, otherwise create it.

    :param name: Name of the node (str).
    :param nodes: Dictionary of existing nodes.
    :param data: Dictionary containing additional data (type, package, owner, name).
    :return: The node (Node) or None if name is empty.
    """
    if not name:
        return None

    if name not in nodes:
        nodes[name] = Node(name, data)
    elif data is not None:
        nodes[name].data = data

    return nodes[name]


def topological_sort(nodes: Dict[str, Node]) -> List[Node]:
    in_degree = {node.name: 0 for node in nodes.values()}

    # Calculate in-degrees
    for node in nodes.values():
        for dep in node.dependencies:
            in_degree[dep.name] += 1

    # Initialize queue with nodes at level 0
    queue = deque(sorted(
        [n for n in nodes.values() if in_degree[n.name] == 0],
        key=lambda x: x.level
    ))

    result = []
    while queue:
        current = queue.popleft()
        result.append(current)

        for dep in current.dependencies:
            in_degree[dep.name] -= 1
            if in_degree[dep.name] == 0:
                # Insert sorted by level
                inserted = False
                for i, n in enumerate(queue):
                    if dep.level < n.level:
                        queue.insert(i, dep)
                        inserted = True
                        break
                if not inserted:
                    queue.append(dep)

    if len(result) != len(nodes):
        raise ValueError("Cycle detected")

    return result
