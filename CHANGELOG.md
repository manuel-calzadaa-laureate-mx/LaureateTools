## 1.1.X INSTALL FEATURE

- [x] Migrate Functions from DataObject file to MigratedDataObject file
- [x] Migrate Procedures from DataObject file to MigratedDataObject file
- [x] Create install_dependencies.csv file with all ObjectTargetType.INSTALL types from MigratedDataObject File
- [x] Implement direct acyclic graph algorithm
- [x] Implement kahn topologic sorting algorith with weight tie-break
- [x] Create install_dependencies_ordered.csv file
- [x] Use install_dependencies_ordered.csv file to find the order of install
- [x] Add filename to install script

## 1.2.X ROLLBACK FEATURE

- [x] Split GRANT addon into GRANT and REVOKE
- [x] Split SYNONYM addon into DROP and CREATE
- [x] Add table delete script
- [x] Add sequence delete script
- [x] Add package delete script
- [x] Add trigger delete script

## 1.3.X TEST INSTALLATION PROCESS

- [x] Rollout ephemeral oracle database using Docker
- [x] Add sqlplus execution tools
- [x] Add script execution tools using sqlplus
- [X] Add environment table setup scripts
- [x] Add environment package setup scripts
- [x] Add environment setup script list
- [x] Execute scripts for minimum target environment
- [x] Execute install scripts in order
- [x] Execute rollback scripts in order
- [x] Add idempotent rollback scripts

## 1.3.1 FIXES

- [x] change filename prefix

## 1.3.2 FIXES

- [x] process private procedures in packages

## 1.4.X IMPROVE PACKAGE DETECTION PROCESS

- [] Find more packages