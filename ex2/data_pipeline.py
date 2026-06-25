from abc import ABC, abstractmethod
from typing import Any, Tuple, Protocol


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
            for item in data:
                level = item.get("log_level", "UNKNOWN")
                message = item.get("log_message", "")
                self._data.append(f"{level}: {message}")
            self._total += len(data)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVExportPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(value for _, value in data))


class JSONExportPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        json_items = [f'"item_{k}": "{v}"' for k, v in data]
        print("{" + ", ".join(json_items) + "}")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for data in stream:
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            items: list[tuple[int, str]] = []
            for _ in range(nb):
                if not proc._data:
                    break
                items.append(proc.output())
            plugin.process_output(items)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")

    print("Initialize Data Stream...\n")
    data_stream = DataStream()
    data_stream.print_processors_stats()

    print("\nRegistering Processors\n")
    n_proc = NumericProcessor()
    t_proc = TextProcessor()
    d_proc = LogProcessor()
    data_stream.register_processor(n_proc)
    data_stream.register_processor(t_proc)
    data_stream.register_processor(d_proc)

    data_list = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {data_list}\n")
    data_stream.process_stream(data_list)
    data_stream.print_processors_stats()

    csv_plugin = CSVExportPlugin()
    print("\nSend 3 processed data from each processor to a CSV plugin:\n")
    data_stream.output_pipeline(3, csv_plugin)
    data_stream.print_processors_stats()
    print()

    data_list_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE', 'log_message': 'It expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print(f"\nSend another batch of data: {data_list_2}\n")
    data_stream.process_stream(data_list_2)
    data_stream.print_processors_stats()

    json_plugin = JSONExportPlugin()
    print("\nSend 5 processed data from each processor to a JSON plugin:\n")
    data_stream.output_pipeline(5, json_plugin)
    data_stream.print_processors_stats()
