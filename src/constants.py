"""Константы, используемые в проекте."""

# Имена файлов
META_FILE = "db_meta.json"
DATA_DIR = "data"

# Поддерживаемые типы данных
VALID_TYPES = {"int", "str", "bool"}

# Сообщения об ошибках
ERROR_TABLE_EXISTS = 'Таблица "{}" уже существует.'
ERROR_TABLE_NOT_EXISTS = 'Таблица "{}" не существует.'
ERROR_INVALID_FORMAT = 'Некорректный формат столбца: "{}". Используйте "имя:тип".'
ERROR_UNSUPPORTED_TYPE = 'Неподдерживаемый тип данных: "{}". Допустимые: {}.'
ERROR_WRONG_VALUE_COUNT = 'Неверное количество значений. Ожидается: {}, получено: {}.'
ERROR_TYPE_MISMATCH = "Несоответствие типов данных."

# Сообщения об успехе
SUCCESS_TABLE_CREATED = 'Таблица "{}" успешно создана. Столбцы: {}'
SUCCESS_TABLE_DROPPED = 'Таблица "{}" успешно удалена.'
SUCCESS_RECORD_ADDED = 'Запись с ID={} успешно добавлена в таблицу "{}".'
SUCCESS_RECORD_UPDATED = 'Записи с ID={} в таблице "{}" успешно обновлены.'
SUCCESS_RECORD_DELETED = 'Записи с ID={} успешно удалены из таблицы "{}".'

# Запросы подтверждения
CONFIRM_DROP_TABLE = "удаление таблицы"
CONFIRM_DELETE_RECORDS = "удаление записей"

# Пороги для логирования
LOG_TIME_THRESHOLD = 0.1  # секунд