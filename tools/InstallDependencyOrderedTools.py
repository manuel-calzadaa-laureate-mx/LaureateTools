from collections import deque
from typing import Dict

from files.InstallDependencyFile import get_install_dependencies_data
from graphs.BFS import collect_all_nodes_using_bfs
from graphs.Node import get_or_create_node, Node, topological_sort


def create_install_dependency_ordered_manager():
    install_dependency_ordered_data = get_install_dependencies_data()
    nodes_data = build_dag_nodes_from_csv(install_dependency_ordered_data)
    all_nodes = collect_all_nodes_using_bfs(nodes_data)
    calculate_levels(all_nodes)
    sorted_nodes = topological_sort(nodes=all_nodes)
    print(sorted_nodes)


def calculate_levels(nodes: Dict[str, Node]):
    # Initialize queue with leaf nodes (nodes with no dependencies)
    queue = deque()
    for node in nodes.values():
        if not node.dependencies:
            node.level = 0
            queue.append(node)

    # Propagate levels upwards
    while queue:
        current = queue.popleft()

        for parent in current.reverse_dependency:
            # Parent's level must be AT LEAST current level + 1
            if parent.level < current.level + 1:
                parent.level = current.level + 1
                queue.append(parent)

    # Special case: ROOT should be last
    if 'ROOT' in nodes:
        max_level = max(n.level for n in nodes.values() if n.name != 'ROOT')
        nodes['ROOT'].level = max_level + 1


def build_dag_nodes_from_csv(csv_data: list[dict]):
    """
    Build a DAG from CSV data.

    :param csv_data: CSV data as a string.
    :return: The root node of the DAG.
    """
    nodes = {}

    root = Node(name="ROOT", data={"type": "root"})
    nodes["ROOT"] = root

    # Read CSV data
    for row in csv_data:
        # Extract fields
        object_package = row["OBJECT_PACKAGE"]
        object_type = row["OBJECT_TYPE"]
        object_name = row["OBJECT_NAME"]
        dependency_package = row["DEPENDENCY_PACKAGE"]
        dependency_type = row["DEPENDENCY_TYPE"]
        dependency_name = row["DEPENDENCY_NAME"]

        current_node = get_or_create_node(
            name=object_name,
            data={"type": object_type, "package": object_package},
            nodes=nodes
        )

        if not current_node:  ##OBJECT_NAME is empty thus no current_node, skip it
            continue

        """
        CASES: FULL OBJECT (ROOT OR NON-ROOT)
        PACKAGE     OBJECT      DEP_PACKAGE     DEP_OBJECT
        PKG1        OBJ1        PKG1            OBJ2        a)
        PKG1        OBJ1        PKG2            OBJ2        b)
        PKG1        OBJ1        NONE            OBJ2        c) 
        PKG1        OBJ1        NONE            NONE        d)
        """
        if object_package:  ## NON-ROOT OBJECT PARENT
            if dependency_package:  ## NON-ROOT DEPENDENCY PARENT
                if object_package == dependency_package:  ## OBJECT AND DEPENDENCY HAVE THE SAME PARENT
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_dependency(current_node)
                    new_dependency = get_or_create_node(name=dependency_name, data=
                    {"type": dependency_type, "package": dependency_package},
                                                        nodes=nodes)

                    current_node.add_dependency(dependency=new_dependency)
                    continue

                if object_package != dependency_package:  ## OBJECT AND DEPENDENCY HAVE DIFFERENT PARENTS

                    if not current_node.parent:
                        current_node.parent = get_or_create_node(object_package, nodes=nodes)
                        current_node.parent.add_dependency(current_node)
                    new_dependency = get_or_create_node(dependency_name, data=
                    {"type": dependency_type, "package": dependency_package},
                                                        nodes=nodes)
                    dependency_object_parent = get_or_create_node(name=dependency_package,
                                                                  nodes=nodes)

                    current_node.add_dependency(dependency=new_dependency, parent=dependency_object_parent)
                    continue

            if not dependency_package:  ## DEPENDENCY PARENT IS ROOT
                if dependency_name:  ## DEPENDENCY IS LEGAL
                    ## add dependency with root parent
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_dependency(current_node)
                    new_dependency = get_or_create_node(dependency_name,
                                                        data={"type": dependency_type, "package": dependency_package},
                                                        nodes=nodes)
                    current_node.add_dependency(dependency=new_dependency, parent=root)
                    continue

                if not dependency_name:  ## OBJECT IS DEPENDENCY-LESS
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_dependency(current_node)
                    ## this node is dependency-less
                    continue

        """
        CASES: ROOT OBJECT -> FULL DEPENDENCY (ROOT OR NON-ROOT)     
        PACKAGE     OBJECT      DEP_PACKAGE     DEP_OBJECT        
        NONE        OBJ1        PKG1            OBJ2        e)
        NONE        OBJ1        NONE            NONE        f)
        NONE        OBJ1        NONE            OBJ2        g)
        NONE        OBJ1        PKG1            NONE        ERROR!
        
        """
        if not object_package:  ## OBJECT PARENT IS ROOT
            if dependency_package:  ## DEPENDENCY PARENT IS NOT ROOT
                if dependency_package == object_name:  ## DEPENDENCY PARENT IS OBJECT
                    ## add parent
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_dependency(current_node)
                    ## create new dependency
                    new_dependency = get_or_create_node(dependency_name,
                                                        data={"type": dependency_type, "package": dependency_package},
                                                        nodes=nodes)

                    ## if dependency is parentless create placeholder
                    if not new_dependency.parent:
                        new_dependency.parent = get_or_create_node(name=dependency_package,
                                                                   nodes=nodes)
                    current_node.add_dependency(new_dependency)
                    continue

            if not dependency_package:  ## DEPENDENCY PARENT IS ROOT
                if dependency_name:  ## NORMAL DEPENDENCY
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_dependency(current_node)
                    new_dependency = get_or_create_node(name=dependency_name, nodes=nodes)
                    current_node.add_dependency(new_dependency, parent=root)
                    continue

                if not dependency_name:  ## OBJECT IS DEPENDENCY-LESS
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_dependency(current_node)
                    continue

    return root
