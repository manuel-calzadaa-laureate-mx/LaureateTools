from typing import Dict

from graphs.node import Node


## Depth-First Search
def print_hierarchy_dfs(node: Node, indent: int = 0):
    """
    Print the hierarchy recursively using DFS (pre-order traversal).

    :param node: The current node to print.
    :param indent: Indentation level for pretty printing.
    """
    if node is None:
        return

    # Print current node with indentation
    parent_name = node.parent.name if node.parent else "None"
    print(
        "  " * indent + f"{parent_name} -> {node.name} (weight={node.weight}) [children: {[c.name for c in node.dependencies]}]")

    # Recursively print children
    for child in node.dependencies:
        print_hierarchy_dfs(child, indent + 1)


def collect_all_nodes_using_dfs(root: Node) -> Dict[str, Node]:
    nodes = {}

    def dfs(node):
        if node.name in nodes:
            return
        nodes[node.name] = node
        for dependency in node.dependencies:
            dfs(dependency)

    dfs(root)
    return nodes
