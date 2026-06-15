#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, Sequence, Protocol


class DataProcessor(ABC):
    def __init__(self):
        self._tuples: list[tuple[int, str]] = []
        self._rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._tuples:
            raise IndexError("No data available")
        return self._tuples.pop(0)


class NumericProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        self._type = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int | float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int | float)) for x in data)
        return False

    def ingest(self, data: int | float | Sequence[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper Numeric data")
        if isinstance(data, (int | float)):
            self._tuples.append((self._rank, str(data)))
            self._rank += 1
        elif isinstance(data, list):
            for x in data:
                self._tuples.append((self._rank, str(x)))
                self._rank += 1


class TextProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        self._type = "Text Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper Text data")
        if isinstance(data, str):
            self._tuples.append((self._rank, data))
            self._rank += 1
        elif isinstance(data, list):
            for x in data:
                self._tuples.append((self._rank, x))
                self._rank += 1


class LogProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        self._type = "Log Processor"

    def validate(self, data: Any) -> bool:
        def check_dict(d: dict):
            if (isinstance(d, dict)
                    and ("log_level" in d)
                    and ("log_message" in d)
                    and isinstance(d["log_level"], str)
                    and isinstance(d["log_message"], str)):
                return True
            else:
                return False

        if isinstance(data, dict):
            return check_dict(data)

        if isinstance(data, list):
            return all(check_dict(d) for d in data)
        return False

    def ingest(self, data: dict | list[dict]) -> None:
        if not self.validate(data):
            raise ValueError("Improper Log data")

        def format_output(d: dict) -> str:
            return f'{d["log_level"]} : {d["log_message"]}'

        if isinstance(data, dict):
            self._tuples.append((self._rank, format_output(data)))
            self._rank += 1
        if isinstance(data, list):
            for d in data:
                self._tuples.append((self._rank, format_output(d)))
                self._rank += 1


# TODO
class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream():
    def __init__(self):
        self._processors = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            if not handled:
                print(f"DataStream error - Can't process element in stream:\
{item}")

    def print_processors_stats(self) -> None:
        print("== DataStream Statistics ==")
        # verify if processors exist
        if not self._processors:
            raise IndexError("No processor found, no data")
        for proc in self._processors:
            processed = proc._rank
            remaining = len(proc._tuples)
            print(f"{proc._type}: total {processed} items processed,\
remaining {remaining} on processor")

    # TODO
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for i in range(nb):
            plugin.process_output()


class CSVPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [value for _, value in data]
        print("CSV Output:")
        print(",".join(values))


def data_stream() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    data = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING',
              'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO',
                'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]
    stream = DataStream()
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("Initialize Data Stream...")
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nRegistering Numeric Processor\n")
    stream.register_processor(num)
    print(f"Send first batch of data on stream: {data}")
    stream.process_stream(data)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nRegistering other data processors")
    stream.register_processor(text)
    stream.register_processor(log)
    print("Send the same batch again")
    stream.process_stream(data)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nConsume some elements from the data processors:\
Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num.output()
    for _ in range(2):
        text.output()
    log.output()
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    data_stream()
