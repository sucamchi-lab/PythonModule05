from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._data: list[str] = []
        self._count = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> Tuple[int, str]:
        value = self._data.pop(0)
        key = self._count
        self._count += 1
        return (key, value)


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) and not
                       isinstance(x, bool) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            self._data.append(str(data))
        elif isinstance(data, list):
            self._data.extend(str(x) for x in data)


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(s, str) for s in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            self._data.extend(data)
        else:
            self._data.append(data)


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and
                       isinstance(v, str) for
                       k, v in data.items())
        if isinstance(data, list):
            return all(isinstance(x, dict) and
                       all(isinstance(k, str) and isinstance(v, str)
                       for k, v in x.items()) for x in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, dict):
            level = data.get("log_level", "UNKNOWN")
            message = data.get("log_message", "")
            self._data.append(f"{level}: {message}")
        elif isinstance(data, list):
            for x in data:
                level = x.get("log_level", "UNKNOWN")
                message = x.get("log_message", "")
                self._data.append(f"{level}: {message}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    n_processor = NumericProcessor()
    print(f"Trying to validate input '42': {n_processor.validate(42)}")
    print(f"Trying to validate input 'Hello': "
          f"{n_processor.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        n_processor.ingest('foo')  # type: ignore
    except ValueError as e:
        print(f"Got exception: {e}")

    l_num: List[int | float] = [1, 2, 3, 4, 5]
    print(f"Processing data: {l_num}")

    n_processor.ingest(l_num)
    to_extract = l_num[:3]
    print(f"Extracting {len(to_extract)} values...")
    for _ in range(len(to_extract)):
        key, value = n_processor.output()
        print(f"Numeric value {key}: {value}")

    print()
    print("Testing Text Processor...")
    t_processor = TextProcessor()
    print(f"Trying to validate input '42': {t_processor.validate(42)}")
    t_list = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {t_list}")
    t_to_extract = t_list[:1]
    t_processor.ingest(t_to_extract)
    print(f"Extracting {len(t_to_extract)} value...")
    for _ in range(len(t_to_extract)):
        key, value = t_processor.output()
        print(f"Text value {key}: {value}")

    print()
    print("Testing Log Processor...")
    l_processor = LogProcessor()
    print(f"Trying to validate input 'Hello': {l_processor.validate('Hello')}")
    l_list = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    l_to_extract = l_list[:2]
    print(f"Processing data: {l_list}")
    l_processor.ingest(l_to_extract)
    print(f"Extracting {len(l_to_extract)} values...")
    for _ in range(len(l_to_extract)):
        key, value = l_processor.output()
        print(f"Log entry {key}: {value}")
