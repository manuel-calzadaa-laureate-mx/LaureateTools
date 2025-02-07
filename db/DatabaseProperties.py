from enum import Enum

class DatabaseEnvironment(Enum):
    BANNER7 = "banner7"
    BANNER9 = "banner9"

class DatabaseObject(Enum):
    TABLE = "tables"
    SEQUENCE = "sequences"
    PROCEDURE = "procedures"
    FUNCTION = "functions"
    SYNONYM = "synonym"
    TRIGGER = "trigger"
    VIEW = "view"
    GRANT = "grant"

class TableObject(Enum):
    TRIGGER = "triggers"