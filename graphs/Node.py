from collections import deque
from typing import List, Dict, Deque
from typing import Optional


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
        self.parent = parent  ## 1:1 relation
        self.dependencies: List['Node'] = []  ## 1:many relation

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
                f"data={self.data},"
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


def topological_sort(nodes: Node) -> List[Node]:
    """
    Perform a topological sort of the DAG with weight-based tie resolution.

    :param nodes: Dictionary of nodes in the graph
    :return: List of nodes in topological order
    """
    # Initialize in-degree count for each node
    in_degree: Dict[str, int] = {node.name: 0 for node in nodes.values()}

    # Calculate in-degree for each node
    for node in nodes.values():
        for dependency in node.dependencies:
            in_degree[dependency.name] += 1

    # Initialize queue with nodes that have no incoming edges
    # Using a deque that we'll keep sorted by weight (highest first)
    queue: Deque[Node] = deque()

    # Add nodes with zero in-degree to the queue
    zero_in_degree_nodes = [node for node in nodes.values() if in_degree[node.name] == 0]

    # Sort by weight descending to prioritize higher weights
    zero_in_degree_nodes.sort(key=lambda x: -x.weight)
    queue.extend(zero_in_degree_nodes)

    result: List[Node] = []

    while queue:
        # Get the node with the highest weight (since queue is sorted)
        current_node = queue.popleft()
        result.append(current_node)

        # Reduce in-degree for all dependencies
        for dependency in current_node.dependencies:
            in_degree[dependency.name] -= 1

            # If in-degree becomes zero, add to queue (will be sorted later)
            if in_degree[dependency.name] == 0:
                # Find the position to insert based on weight
                inserted = False
                for i, node in enumerate(queue):
                    if dependency.weight > node.weight:
                        queue.insert(i, dependency)
                        inserted = True
                        break
                if not inserted:
                    queue.append(dependency)

    # Check for cycles (if result doesn't contain all nodes)
    if len(result) != len(nodes):
        raise ValueError("Graph contains a cycle, topological sort not possible")

    return result
