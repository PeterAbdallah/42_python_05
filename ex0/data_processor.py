#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        pass


class NumericProcessor(DataProcessor):
    # VALIDATION
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int | float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int | float)) for x in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        # Validate first
        if not self.validate(data):
            raise ValueError("Improper numeric data...")
        


class TextProcessor(DataProcessor):
    pass


class LogProcessor(DataProcessor):
    pass


def data_processor() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")


if __name__ == "__main__":
    pass
