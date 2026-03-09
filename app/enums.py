from enum import Enum

class SheetStatus(Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    SHEET_DOES_NOT_EXIST = "sheet_does_not_exist"
    TAB_DOES_NOT_EXIST = "tab_does_not_exist"