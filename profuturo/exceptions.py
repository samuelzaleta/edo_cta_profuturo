from typing import Optional


class ProfuturoException(Exception):
    MAPPING = {
        "TABLE_MISSED": "No se encuentra el archivo de origen",
        "TABLE_UNEXPECTED_FORMAT": "El archivo de origen tiene un formato no válido",
        "TABLE_MISSED_VALUES": "Faltan valores o campos requeridos en los datos",
        "TABLE_UNEXPECTED_VALUES": "Los datos contienen valores incorrectos o no esperados",
        "TABLE_DUPLICATED_VALUES": "Se encuentran registros duplicados",
        "TABLE_INVALID_FOREIGN_VALUES": "Las relaciones entre los datos no son coherentes",
        "TABLE_INVALID_CHARSET": "Los caracteres no se interpretan correctamente",
        "TABLE_SLOW_EXTRACTION": "La carga de datos lleva más tiempo del esperado",
        "TABLE_TRANSFORM_ERROR": "Las reglas de transformación no se aplican correctamente",
        "TABLE_SLOW_PERFORMANCE": "Rendimiento lento o ineficiente durante la carga o transformación",
        "DATABASE_CONNECTION_ERROR": "No se puede establecer una conexión con la base de datos",
        "CONTROL_METRICS_NULL_VALUES": "Las cifras de control de los datos cargados tiene existencia de valores nullos",
        "BINNACLE_ERROR": "No se pudo crear o actualizar la bitácora de inicio del proceso de carga de datos",
        "TERMS_ERROR": "No se pudo leer correctamente el período o fecha asociada a los datos a cargar.",
        "TABLE_SWITCH_ERROR": "No se pudo leer correctamente el estado del switch o parámetro necesario para la carga de datos.",
        "TABLE_NO_DATA": "No se encuentraron data en la tabla de origen",
        "UNKNOWN_ERROR": "Error desconocido",
    }

    code: str
    msg: str
    phase: int
    term: Optional[int]

    def __init__(self, code: str, phase: int, term: Optional[int] = None, *args: object):
        super().__init__(args)
        self.code = code
        self.msg = self.MAPPING[code]
        self.phase = phase
        self.term = term
