from files.InstallDependencyFile import get_install_dependencies_data
from graphs.Node import get_or_create_node, Node


def create_install_dependency_ordered_manager():
    install_dependency_ordered_data = get_install_dependencies_data()
    nodes_data = build_dag_nodes_from_csv(install_dependency_ordered_data)
    ordered_nodes = nodes_data.get_nodes_sorted_bottom_up()
    print(ordered_nodes)


def build_dag_nodes_from_csv(csv_data: list[dict]):
    """
    Build a DAG from CSV data.

    :param csv_data: CSV data as a string.
    :return: The root node of the DAG.
    """
    nodes = {}

    ## THIS IS THE ROOT NODE!!!
    root = Node(name="ROOT", data={"type": "root"}, weight=0)
    nodes["ROOT"] = root

    # Read CSV data
    for row in csv_data:
        # Extract fields
        object_package = row["OBJECT_PACKAGE"]
        object_type = row["OBJECT_TYPE"]
        object_name = row["OBJECT_NAME"]
        object_owner = row["OBJECT_OWNER"]
        dependency_package = row["DEPENDENCY_PACKAGE"]
        dependency_type = row["DEPENDENCY_TYPE"]
        dependency_name = row["DEPENDENCY_NAME"]
        dependency_owner = row["DEPENDENCY_OWNER"]

        ## skip base tables
        if len(object_name) < 9 or len(dependency_name) < 9:
            continue

        current_node = get_or_create_node(
            name=object_name,
            data={"type": object_type, "package": object_package, "owner": object_owner},
            weight=0,
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
        if object_package:
            if dependency_package:
                if object_package == dependency_package:
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_child(current_node)
                    new_child = get_or_create_node(name=dependency_name, data=
                    {"type": dependency_type, "package": dependency_package,
                     "owner": dependency_owner}, weight=0, nodes=nodes)

                    current_node.add_child(child=new_child)
                    continue

                if object_package != dependency_package:

                    if not current_node.parent:
                        current_node.parent = get_or_create_node(object_package, nodes=nodes, weight=0)
                        current_node.parent.add_child(current_node)
                    new_child = get_or_create_node(dependency_name, data=
                    {"type": dependency_type, "package": dependency_package,
                     "owner": dependency_owner}, weight=0, nodes=nodes)
                    dependency_object_parent = get_or_create_node(name=dependency_package, nodes=nodes, weight=0)

                    current_node.add_child(child=new_child, parent=dependency_object_parent)
                    continue

            if not dependency_package:
                if dependency_name:
                    ## add dependency with root parent
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_child(current_node)
                    new_child = get_or_create_node(dependency_name,
                                                   data={"type": dependency_type, "package": dependency_package,
                                                         "owner": dependency_owner}, weight=0, nodes=nodes)
                    current_node.add_child(child=new_child, parent=root)
                    continue

                if not dependency_name:
                    if not current_node.parent:
                        current_node.parent = get_or_create_node(name=object_package, weight=0, nodes=nodes)
                        current_node.parent.add_child(current_node)
                    ## this node is childless
                    continue

        """
        CASES: ROOT OBJECT -> FULL DEPENDENCY (ROOT OR NON-ROOT)     
        PACKAGE     OBJECT      DEP_PACKAGE     DEP_OBJECT        
        NONE        OBJ1        PKG1            OBJ2        e)
        NONE        OBJ1        NONE            NONE        f)
        NONE        OBJ1        NONE            OBJ2        g)
        NONE        OBJ1        PKG1            NONE        ERROR!
        
        """
        if not object_package:
            if dependency_package:
                if dependency_package == object_name:
                    ## add parent
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_child(current_node)
                    ## create new child
                    new_child = get_or_create_node(dependency_name,
                                                   data={"type": dependency_type, "package": dependency_package,
                                                         "owner": dependency_owner}, weight=0, nodes=nodes)

                    ## if child is parentless create placeholder
                    if not new_child.parent:
                        new_child.parent = get_or_create_node(name=dependency_package, weight=0, nodes=nodes)
                    current_node.add_child(new_child)
                    continue

            if not dependency_package:
                if dependency_name:
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_child(current_node)
                    new_child = get_or_create_node(name=dependency_name, weight=0, nodes=nodes)
                    current_node.add_child(new_child, parent=root)
                    continue

                if not dependency_name:
                    if not current_node.parent:
                        current_node.parent = root
                        root.add_child(current_node)
                    continue

    return root
