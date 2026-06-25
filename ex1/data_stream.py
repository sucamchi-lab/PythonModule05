from abc import ABC, abstractmethod
from typing import Any, Tuple


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._data: list[str] = []
        self._count = 0
        self._total = 0

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
            self._total += 1
        elif isinstance(data, list):
            self._data.extend(str(x) for x in data)
            self._total += len(data)


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
            self._total += len(data)
        else:
            self._data.append(data)
            self._total += 1


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
            self._total += 1
        elif isinstance(data, list):
            for x in data:
                level = x.get("log_level", "UNKNOWN")
                message = x.get("log_message", "")
                self._data.append(f"{level}: {message}")
            self._total += len(data)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
            process: DataProcessor
            for process in self._processors:
                if process.validate(data):
                    process.ingest(data)
                    break
            else:
                print(f"DataStream error - Can't process element in stream: "
                      f"{data}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            remaining = len(proc._data)
            print(f"{proc.__class__.__name__}: total "
                  f"{proc._total} items processed, remaining "
                  f"{remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    n_proc = NumericProcessor()
    data_stream.register_processor(n_proc)

    stream = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five'],
    ]
    print(f"Send first batch of data on stream: {stream}")
    data_stream.process_stream(stream)
    data_stream.print_processors_stats()

    print("\nRegistering other data processors")
    t_proc = TextProcessor()
    d_proc = LogProcessor()
    data_stream.register_processor(t_proc)
    data_stream.register_processor(d_proc)

    print("Send the same batch again")
    data_stream.process_stream(stream)
    data_stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        n_proc.output()
    for _ in range(2):
        t_proc.output()
    for _ in range(1):
        d_proc.output()
    data_stream.print_processors_stats()
