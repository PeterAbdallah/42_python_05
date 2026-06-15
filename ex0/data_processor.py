#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._tuples = []
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
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int | float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int | float)) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        print(f"Processing data: {data}")
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
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        print(f"Processing data: {data}")
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
        print(f"Processing data: {data}")
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


def data_processor() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    # NUMERIC PROCESSOR TESTING
    print("Testing Numeric Processor...")
    num = NumericProcessor()
    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")

    num_data = [1, 2, 3, 4, 5]
    num.ingest(num_data)
    print("Extracting 3 values...")
    for _ in range(3):
        r, v = num.output()
        print(f"Numeric value {r}: {v}")

    # TEXT PROCESSOR TESTING
    txt = TextProcessor()
    print("\nTesting Text Processor...")
    print(f"Trying to validate input '42': {txt.validate(42)}")

    txt_data = ["Hello", "Nexus", "World"]
    txt.ingest(txt_data)
    print("Extracting 1 value...")
    r, v = txt.output()
    print(f"Text value {r}: {v}")

    # LOG PROCESSOR TESTING
    log = LogProcessor()
    print("\nTesting Log Processor...")
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    log_data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    log.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        r, v = log.output()
        print(f"Log entry {r}: {v}")


if __name__ == "__main__":
    data_processor()
