from db.DatabaseProperties import DatabaseEnvironment
from files.InstallDependencyFile import create_install_dependency_data, write_install_dependencies_file
from files.ObjectDataFile import get_only_migrated_objects

if __name__ == "__main__":
    migrated_object_data = get_only_migrated_objects(database_environment=DatabaseEnvironment.BANNER9)
    install_dependency_data = create_install_dependency_data(migrated_object_data=migrated_object_data)
    write_install_dependencies_file(install_dependencies_data=install_dependency_data)
