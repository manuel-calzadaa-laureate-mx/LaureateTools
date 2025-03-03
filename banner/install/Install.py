import csv
import heapq
from collections import defaultdict, deque

from files.B9DependencyFile import get_dependency_file_path
from tools.FileTools import read_csv_file

OBJECT_WEIGHTS = {
    'TABLE': 1000,       # Highest priority (but processed last due to min-heap)
    'SEQUENCE': 1000,    # Highest priority (but processed last due to min-heap)
    'PROCEDURE': 2,      # Lower priority
    'FUNCTION': 2,       # Lower priority
    'PACKAGE': 1         # Lowest priority
}

def assign_package_weights(packages_with_deps):
    # Assign weights based on dependency depth
    package_weights = {}
    weight = 10  # Start with a high weight for root packages

    # For simplicity, assign the same weight to all packages with dependencies
    for pkg in packages_with_deps:
        package_weights[pkg] = weight

    return package_weights

def topological_sort(graph, in_degree, nodes, package_weights):
    heap = []
    for node in nodes:
        if in_degree[node] == 0:
            obj_type, obj_package, obj_name = node
            # Calculate combined weight
            object_weight = OBJECT_WEIGHTS.get(obj_type, 0)
            package_weight = package_weights.get(obj_package, 0) if obj_package else 0
            # Use total_weight directly for min-heap
            total_weight = object_weight + package_weight
            # Push (total_weight, node) to simulate a min-heap
            heapq.heappush(heap, (total_weight, node))

    sorted_order = []

    while heap:
        _, node = heapq.heappop(heap)
        sorted_order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                obj_type, obj_package, obj_name = neighbor
                # Calculate combined weight for neighbor
                object_weight = OBJECT_WEIGHTS.get(obj_type, 0)
                package_weight = package_weights.get(obj_package, 0) if obj_package else 0
                total_weight = object_weight + package_weight
                # Push (total_weight, neighbor) to simulate a min-heap
                heapq.heappush(heap, (total_weight, neighbor))

    if len(sorted_order) == len(nodes):
        return sorted_order
    else:
        raise Exception("Cycle detected in the dependency graph")

def identify_packages_with_dependencies(dependencies):
    packages_with_deps = set()

    for dep in dependencies:
        obj_package = dep['OBJECT_PACKAGE']
        dep_package = dep['DEPENDENCY_PACKAGE']
        obj_type = dep['OBJECT_TYPE']

        # Skip PACKAGE type because they are roots
        if obj_type == 'PACKAGE':
            continue

        # NONE signals that the dependency is a ROOT object
        if dep_package == "NONE":
            continue

        # If the object package depends on another package, function, or procedure, include it
        if obj_package != dep_package:
            packages_with_deps.add(obj_package)

    return packages_with_deps

def parse_csv(file_path):
    dependencies = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['STATUS'] == 'OK':
                if row['DEPENDENCY_TYPE'] == "TABLE" and not row['DEPENDENCY_NAME'].startswith("TZ"):
                    continue
                dependencies.append({
                    'object_package'
                    'object_type': row['OBJECT_TYPE'],
                    'object_name': row['OBJECT_NAME'],
                    'dependency_type': row['DEPENDENCY_TYPE'],
                    'dependency_name': row['DEPENDENCY_NAME']
                })
    return dependencies

def write_csv(file_path, data, header):
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for order, (obj_type, obj_package, obj_name) in enumerate(data, start=1):
            writer.writerow([order, obj_package, obj_type, obj_name])

def build_graph(dependencies, packages_with_deps):
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    nodes = set()

    for dep in dependencies:
        obj = (dep['OBJECT_TYPE'], dep['OBJECT_PACKAGE'], dep['OBJECT_NAME'])
        dep_obj = (dep['DEPENDENCY_TYPE'], dep['DEPENDENCY_PACKAGE'], dep['DEPENDENCY_NAME'])

        obj_package = dep['OBJECT_PACKAGE'] or None
        dep_package = dep['DEPENDENCY_PACKAGE'] or None

        # Skip objects from independent PACKAGES
        if obj_package and dep['OBJECT_TYPE'] != "PACKAGE" and obj_package not in packages_with_deps:
            continue

        if obj_package and dep['OBJECT_TYPE'] == "PACKAGE" and dep['OBJECT_NAME'] not in packages_with_deps:
            continue

        # Skip objects that belong to independent packages and aren't ROOT
        if dep_package and dep_package not in packages_with_deps and dep_package != "NONE":
            continue

        ## Skip BASE objects
        if dep['DEPENDENCY_TYPE'] in ["TABLE","SEQUENCE"] and not dep['DEPENDENCY_NAME'].startswith("TZ"):
            continue

        nodes.add(obj)
        nodes.add(dep_obj)
        if dep_obj not in graph[obj]:
            graph[obj].append(dep_obj)
            in_degree[dep_obj] += 1

    return graph, in_degree, nodes

def main():
    input_csv = get_dependency_file_path()
    install_csv = 'install.csv'
    rollback_csv = 'rollback.csv'

    dependencies = read_csv_file(input_csv)
    packages_with_deps = identify_packages_with_dependencies(dependencies)

    graph, in_degree, nodes = build_graph(dependencies, packages_with_deps)
    package_weights = assign_package_weights(packages_with_deps)
    sorted_order = topological_sort(graph, in_degree, nodes, package_weights)

    # Write rollback.csv
    write_csv(rollback_csv, sorted_order, ['ORDER', 'PACKAGE_TYPE', 'OBJECT_TYPE', 'OBJECT_NAME'])

    # Write install.csv (reverse order)
    write_csv(install_csv, reversed(sorted_order), ['ORDER', 'PACKAGE_TYPE', 'OBJECT_TYPE', 'OBJECT_NAME'])

if __name__ == '__main__':
    main()