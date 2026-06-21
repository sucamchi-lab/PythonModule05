import typing
import abc

class DataProcessor(abc.ABC):
    def __init__(self):
        self.count: int = 0
        self.message: str = ""
    
    @abc.abstractmethod
    def validate(self, data:typing.Any) -> bool:
        pass
    
    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass
    
    def output(self) -> tuple[int, str]:
        return (self.count, self.message)
        
        

class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        return isinstance(data, (list, int, float))
    def ingest(self, data: int | float | list) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type for NumericProcessor")
        if isinstance(data, list):
            self.count = len(data)
        else:
            self.count = 1
        self.message = "Numeric data processed successfully"

class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        return isinstance(data, (list, str))
    def ingest(self, data: str | list) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type for TextProcessor")
        if isinstance(data, list):
            self.count = len(data)
        else:
            self.count = 1
        self.message = "Text data processed successfully"

class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        return isinstance(data, list) and all(isinstance(item, dict) and "log_level" in item and "log_message" in item for item in data)
    def ingest(self, data: list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Invalid data type for LogProcessor")
        self.count = len(data)
        self.message = "Log data processed successfully"

def main():


if __name__ == "__main__":
    main()
