from db.DatabaseProperties import DatabaseEnvironment
from files.InstallDependencyFile import write_install_dependencies_file
from files.ObjectDataFile import get_only_migrated_objects


def create_install_dependency_data(migrated_object_data: list[dict]):
    migrated_dependencies_list = []

    for row in migrated_object_data:
        object_name = row.get("name")
        object_type = row.get("type")
        object_package = row.get("package")
        object_owner = row.get("owner")
        all_dependencies = row.get("dependencies")

        # Handle empty or invalid dependencies
        if not all_dependencies or not any(all_dependencies.values()):
            migrated_dependency = {
                "OBJECT_PACKAGE": object_package if object_package else 'NONE',
                "OBJECT_TYPE": object_type,
                "OBJECT_NAME": object_name,
                "OBJECT_OWNER": object_owner,
                "DEPENDENCY_PACKAGE": 'NONE',
                "DEPENDENCY_TYPE": 'NONE',
                "DEPENDENCY_NAME": 'NONE',
                "DEPENDENCY_OWNER": 'NONE',
            }
            migrated_dependencies_list.append(migrated_dependency)
            continue

        for dependency_key, dependency_values in all_dependencies.items():
            for dependency_value in dependency_values:
                if dependency_value:
                    dependency_package = dependency_value.get("package")
                    dependency_type = dependency_value.get("type")
                    dependency_name = dependency_value.get("name")
                    dependency_owner = dependency_value.get("owner")

                    migrated_dependency = {
                        "OBJECT_PACKAGE": object_package if object_package else 'NONE',
                        "OBJECT_TYPE": object_type,
                        "OBJECT_NAME": object_name,
                        "OBJECT_OWNER": object_owner,
                        "DEPENDENCY_PACKAGE": dependency_package if dependency_package else 'NONE',
                        "DEPENDENCY_TYPE": dependency_type,
                        "DEPENDENCY_NAME": dependency_name,
                        "DEPENDENCY_OWNER": dependency_owner if dependency_owner else 'NONE',
                    }
                    migrated_dependencies_list.append(migrated_dependency)

    return migrated_dependencies_list


def create_install_dependency_manager(database_environment: DatabaseEnvironment):
    migrated_object_data = get_only_migrated_objects(database_environment=database_environment)
    install_dependency_data = create_install_dependency_data(migrated_object_data=migrated_object_data)
    write_install_dependencies_file(install_dependencies_data=install_dependency_data)
