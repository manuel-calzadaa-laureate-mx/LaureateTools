from collections import deque
from typing import Dict

from graphs.node import Node


## Breath-First Search
def print_hierarchy_bfs(root: Node):
    """
    Print the hierarchy using BFS (level-order traversal).

    :param root: The root node of the hierarchy.
    """
    if root is None:
        return

    queue = deque([root])

    while queue:
        node = queue.popleft()
        parent_name = node.parent.name if node.parent else "None"
        print(
            f"{parent_name} -> {node.name} {node.data} (weight={node.weight}) [children: {[c.name for c in node.dependencies]}]")

        # Enqueue children
        for child in node.dependencies:
            queue.append(child)


def collect_all_nodes_using_bfs(root: Node) -> Dict[str, Node]:
    """
    Traverse the DAG starting from root using BFS to collect all nodes.
    Returns a dictionary of {node_name: Node} pairs.
    """
    nodes = {}
    queue = deque([root])

    while queue:
        current = queue.popleft()

        # Skip if we've already processed this node
        if current.name in nodes:
            continue

        # Add current node to the collection
        nodes[current.name] = current

        # Add all dependencies to the queue
        for dependency in current.dependencies:
            queue.append(dependency)

    return nodes
