# TaskPulse Engine

TaskPulse is an asynchronous, high-throughput task queue and workflow engine built with modern Python. It features priority scheduling, configurable retries with exponential backoff, dead-letter queues (DLQ), and execution telemetry.

## Features
- **Async Execution Engine**: Runs sync and async Python tasks concurrently.
- **Priority Queue**: Lower priority values execute first.
- **Fault Tolerance**: Automatic retry mechanism with configurable limits and exponential backoff.
- **Dead-Letter Queue (DLQ)**: Captures tasks that fail permanently for later inspection.
- **Pydantic Validation**: Robust type safety and structured output reporting.

## Project Structure
```
.
├── .gitignore
├── README.md
├── requirements.txt
├── main.py
├── taskengine/
│   ├── __init__.py
│   ├── engine.py
│   └── models.py
└── tests/
    └── test_main.py
```

## Quick Start

1. **Setup virtual environment & dependencies:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run Demonstration:**
   ```bash
   python main.py
   ```

3. **Run Tests:**
   ```bash
   pytest
   ```
