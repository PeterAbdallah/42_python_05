#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, Sequence, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._tuples: list[tuple[int, str]] = []
        self._rank = 0
        self._type: str = ""

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
    def __init__(self) -> None:
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
    def __init__(self) -> None:
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
    def __init__(self) -> None:
        super().__init__()
        self._type = "Log Processor"

    def validate(self, data: Any) -> bool:
        def check_dict(d: dict[str, str]) -> bool:
            return (
                isinstance(d, dict)
                and all(isinstance(k, str) for k in d.keys())
                and all(isinstance(v, str) for v in d.values())
            )

        if isinstance(data, dict):
            return check_dict(data)

        if isinstance(data, list):
            return all(check_dict(d) for d in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper Log data")

        def format_output(d: dict[str, str]) -> str:
            return f'{d["log_level"]} : {d["log_message"]}'

        if isinstance(data, dict):
            self._tuples.append((self._rank, format_output(data)))
            self._rank += 1
        if isinstance(data, list):
            for d in data:
                self._tuples.append((self._rank, format_output(d)))
                self._rank += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataStream():
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

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
        print("\n== DataStream Statistics ==")
        # verify if processors exist
        if not self._processors:
            raise IndexError("No processor found, no data")
        for proc in self._processors:
            processed = proc._rank
            remaining = len(proc._tuples)
            print(f"{proc._type}: total {processed} items processed,\
remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    data.append(proc.output())
                except IndexError:
                    break
            if data:
                plugin.process_output(data)


class CSVPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [value for _, value in data]
        print("CSV Output:")
        print(",".join(values))


class JSONPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        values = [f'"item_{k}": "{v}"' for k, v in data]
        print("JSON Output:")
        print("{" + ", ".join(values) + "}")


def data_pipeline() -> None:
    print("=== Code Nexus - Pipeline ===\n")

    data = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING',
              'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO',
                'log_message': 'User wil isconnected'}], 42, ['Hi', 'five']]

    data2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
             [{'log_level': 'ERROR', 'log_message': '500 server crash'},
              {'log_level': 'NOTICE',
               'log_message': 'Certificate expires in 10 days'}],
             [32, 42, 64, 84, 128, 168], 'World hello']

    stream = DataStream()
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    csv_plug = CSVPlugin()
    json_plug = JSONPlugin()

    print("Initialize Data Stream...")
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nRegistering Processors\n")
    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)

    print("Send first batch of data on stream:")
    stream.process_stream(data)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, csv_plug)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("Send another batch of data:")
    stream.process_stream(data2)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, json_plug)
    try:
        stream.print_processors_stats()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    data_pipeline()
