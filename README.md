# LAUREATE PROCESSING

## QUICK START

- Execute setup.py
- Fill

## INTRODUCTION

This process is meant to be a CI/CD tool for

- migration from banner7 to banner9
- automate script generation of procedure dependencies
- automate the installation scripts for banner9 prod

This consist in several use phases:

## Phase One - Find dependencies

- [x] Receive a list of packages and procedures in a file
- [x] Compile a list of all the procedures in the packages (FindAllMissingProcedures)
- [x] Extract all the procedures and functions source code and generate sql files for each procedure (
  FindAllMissingProcedures)
- [x] Find the dependencies in all the procedures and functions (FindAllDependenciesForProceduresAndFunctions)
- [x] Export in json format all the data compiled (ExportDataToJson)

## Phase Two - Build dependencies scripts

### Tables

- [x] Find the elements of each custom table
- [x] Create a json file with each table's elements

### Sequences

- [x] Find the elements of each sequence
- [x] Create a json file with each sequence's element

## Phase Three - Migration

### Tables

- [x] Create name mapping of tables in banner9
- [x] Create new objects required for migration (triggers, sequences, indexes, grants, synonyms)
- [x] Create table definition with new fields in json
- [x] Create triggers, sequences and indexes in json

### Functions

- [] Create name mapping of functions in banner9

## Phase Four - Upgrading

### Procedures

- [] Upgrade tables, sequences, views, and functions from mapping

### Functions

- [] Upgrade tables, sequences, views, and functions from mapping

### Dependencies

- [] Create a json file with procedures, functions, tables, sequences and indexes, and set dependencies as a tree.

## Phase Five - Packaging

- [] Create a list of objects in order of dependency
- [] Create the scripts for each object

## Phase Six - Install script

- [] Create the installation scripts for tables and dependencies
- [] Create the installation scripts for procedures
- [] Create the installation scripts for packages
- [] Create the xls file for order of install

## Phase Seven - Test script

- [] Create Oracle DB in a container
- [] Execute the scripts in the xls file order

## 1.1.X INSTALL FEATURE

- [x] Migrate Functions from DataObject file to MigratedDataObject file
- [x] Migrate Procedures from DataObject file to MigratedDataObject file
- [x] Create install_dependencies.csv file with all ObjectTargetType.INSTALL types from MigratedDataObject File
- [x] Implement direct acyclic graph algorithm
- [x] Implement kahn topologic sorting algorith with weight tie-break
- [] Create install_dependencies_ordered.csv file
- [] Use install_dependencies_ordered.csv file to find the order of install and rollback using graphs
- [] Add filename to install and rollback files

