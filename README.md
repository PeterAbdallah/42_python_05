_This project has been created as part of the 42 curriculum by pabdalla_

# Code Nexus - Polymorphic Data Streams in the Digital Matrix

## Evaluator Instructions

### Running the scripts
Replace with necessary values
```bash
python3 {exercise.py}
# or directly if the shebang is set:
./{exercise.py}
```

### Checking code standards
```bash
flake8      # style linter
mypy ./     # type checker
```

---

## Overview

This project teaches object-oriented programming in Python through a series of progressive exercises
set in a cyberpunk data processing system. Starting from abstract class design and advancing through
polymorphic stream routing, duck typing, and plugin-based export pipelines,
each exercise builds directly on the previous one, forming a complete data processing pipeline
by the end.

---

## Concepts Covered

- Defining abstract classes with `ABC` and `@abstractmethod` from the `abc` module
- Implementing method overriding in specialized subclasses
- Applying subtype polymorphism to route data without knowing concrete types
- Using `typing.Any` for flexible method signatures while enforcing type safety internally
- Returning structured results as `tuple[int, str]` from shared interface methods
- Implementing duck typing via `typing.Protocol` for export plugin compatibility
- Handling invalid data with custom exceptions raised on bad ingestion
- Using `typing.IO` and comprehensive type annotations verified with `mypy`
- Adhering to `flake8` coding standards throughout

---

## Key Python Concepts

### Abstract Base Classes (ABC)
- `from abc import ABC, abstractmethod` — base tools for defining abstract interfaces
- A class inheriting from `ABC` cannot be instantiated directly
- Methods decorated with `@abstractmethod` must be overridden in every concrete subclass
- Abstract classes define *what* subclasses must do, not *how* — enforcing a shared contract

### Method Overriding and Polymorphism
- Subclasses override abstract methods with their own specific implementations
- Polymorphism allows code to call the same method (`validate`, `ingest`, `output`) on different objects and get type-appropriate behavior
- The caller doesn't need to know the concrete type — only the shared interface matters

### `typing.Protocol` and Duck Typing
- `Protocol` (from `typing`) defines a structural interface without requiring inheritance
- Any class that implements the required methods is automatically compatible — no explicit subclassing needed
- This enables a flexible plugin system where export classes are interchangeable

### Queued Data and the `output` Method
- Each processor maintains an internal ordered store of ingested data
- `output(self) -> tuple[int, str]` extracts the oldest item along with its processing rank (index)
- The item is removed from the processor upon extraction — behaves like a queue

### Exception Handling in Data Ingestion
- If `ingest` is called with invalid data (without prior `validate`), an exception must be raised
- This protects data streams from silent corruption
- Deliberately passing wrong types to `ingest` will trigger a `mypy` warning — this is intentional per the subject

---

## Exercise Summary

| Exercise | File | Concepts |
|----------|------|----------|
| 0 | `ex0/data_processor.py` | `ABC`, `@abstractmethod`, method overriding, internal queue, `output` as tuple |
| 1 | `ex1/data_stream.py` | Polymorphic stream routing, processor registration, stream statistics |
| 2 | `ex2/data_pipeline.py` | `Protocol`, duck typing, CSV/JSON export plugins, `output_pipeline` |

---