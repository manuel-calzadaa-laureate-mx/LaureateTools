from typing import Optional, List, Dict


class Node:
    def __init__(self, name: str, data: Optional[dict] = None, weight: int = 0, parent: 'Node' = None):
        """
        Initialize a node in the DAG.

        :param name: Name of the node (str).
        :param data: Dictionary containing additional data (type, package, owner, name).
        :param weight: Weight of the node (int).
        :param parent: Parent node (Node or None if root).
        """
        self.name = name
        self.data = data or {}
        self.weight = weight
        self.parent = parent
        self.dependencies: List['Node'] = []

    def add_dependency(self, dependency: 'Node', parent: Optional['Node'] = None) -> None:
        """
        Add a dependency node with these rules:
        - If parent is provided, set it as the dependency's parent
        - Otherwise, set self as the dependency's parent
        - Add dependency to dependencies list if not already present
        """
        if dependency not in self.dependencies:
            self.dependencies.append(dependency)

        # Set parent (if provided, else use self)
        dependency.parent = parent or self

        # Compute depth of dependency (distance from ROOT)
        depth = 0
        current = dependency.parent
        while current and current.name != "ROOT":
            depth += 1
            current = current.parent

        # Weight increment: inversely proportional to depth
        dependency.weight += 1 / (depth + 1)

    def __repr__(self):
        return (f"Node(name={self.name}, "
                f"parent={self.parent.name if self.parent else 'ROOT'}, "
                f"dependencies={[c.name for c in self.dependencies]},"
                f"weight={self.weight})")

    @property
    def is_direct_child_of_root(self) -> bool:
        """
        Check if this node is a direct dependency of the root node.
        """
        return self.parent is not None


def get_or_create_node(name: str, nodes: Dict[str, Node], data: Optional[dict] = None, weight: int = 0) -> Optional[
    Node]:
    """
    Get a node if it exists, otherwise create it.

    :param name: Name of the node (str).
    :param nodes: Dictionary of existing nodes.
    :param data: Dictionary containing additional data (type, package, owner, name).
    :param weight: Weight of the node (int).
    :return: The node (Node) or None if name is empty.
    """
    if not name:
        return None

    if name not in nodes:
        nodes[name] = Node(name, data, weight)
    elif data is not None:
        nodes[name].data = data

    return nodes[name]
