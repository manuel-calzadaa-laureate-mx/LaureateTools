# Directed Acyclic Graph (WEIGHTED)
class Node:
    def __init__(self, name, data, weight, parent=None):
        """
        Initialize a node in the DAG.

        :param name: Name of the node (str).
        :param data: Dictionary containing additional data (type, package, owner, name).
        :param weight: Weight of the node (int).
        :param parent: Parent node (DagNode or None if root).
        """
        self.name = name
        self.data = data
        self.weight = weight
        self.parent = parent
        self.children = []

    def add_child(self, child):
        """
        Add a child node to the current node.

        :param child: The child node to add (DagNode).
        """
        self.children.append(child)
        child.parent = self

    def __repr__(self):
        """
        String representation of the node for debugging.
        """
        return (f"Node(name={self.name}, data={self.data}, weight={self.weight}, "
                f"parent={self.parent.name if self.parent else None}, "
                f"children={[child.name for child in self.children]})")


# Helper function to create or get a node
def get_or_create_node(name, data, weight, nodes):
    """
    Get a node if it exists, otherwise create it.

    :param name: Name of the node (str).
    :param data: Dictionary containing additional data (type, package, owner, name).
    :param weight: Weight of the node (int).
    :param nodes: Dictionary of existing nodes.
    :return: The node (DagNode).
    """
    if name not in nodes:
        nodes[name] = Node(name, data, weight)
    return nodes[name]
