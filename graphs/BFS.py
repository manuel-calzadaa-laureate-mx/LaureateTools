from collections import deque

from graphs.Node import Node


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
            f"{parent_name} -> {node.name} {node.data} (weight={node.weight}) [children: {[c.name for c in node.children]}]")

        # Enqueue children
        for child in node.children:
            queue.append(child)
