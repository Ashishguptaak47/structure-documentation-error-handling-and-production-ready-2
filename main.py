import asyncio
import logging
from taskengine import TaskEngine, Task, TaskStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def sync_compute_square(x: int) -> int:
    return x * x


async def async_fetch_data(url: str) -> dict:
    await asyncio.sleep(0.1)
    if "fail" in url:
        raise ValueError(f"Connection refused to URL: {url}")
    return {"url": url, "status": 200, "data": "sample payload"}


async def main():
    engine = TaskEngine(max_concurrent=3)
    await engine.start(num_workers=2)

    print("\n--- Submitting Tasks ---")
    
    # Task 1: Normal sync execution
    t1 = Task(name="Sync Square Task", fn=sync_compute_square, args=(12,), priority=5)
    
    # Task 2: High priority async success
    t2 = Task(name="High Priority Fetch", fn=async_fetch_data, args=("https://api.example.com/ok",), priority=1)
    
    # Task 3: Failing task triggering retries and DLQ
    t3 = Task(name="Flaky Endpoint Fetch", fn=async_fetch_data, args=("https://api.example.com/fail",), priority=2, max_retries=2)

    id1 = await engine.submit(t1)
    id2 = await engine.submit(t2)
    id3 = await engine.submit(t3)

    # Wait for completion
    await engine.stop()

    print("\n--- Execution Results ---")
    for tid, name in [(id1, "Sync Task"), (id2, "High Priority Task"), (id3, "Failing Task")]:
        res = engine.get_result(tid)
        print(f"{name} [{tid[:8]}] -> Status: {res.status.value}, Result: {res.result}, Error: {res.error}, Time: {res.execution_time_ms:.2f}ms")

    print(f"\nDead-Letter Queue Count: {len(engine.get_dlq())}")
    for dlq_task in engine.get_dlq():
        print(f"  - Dead Task: '{dlq_task.name}' ID: {dlq_task.id} Status: {dlq_task.status.value}")


if __name__ == "__main__":
    asyncio.run(main())
