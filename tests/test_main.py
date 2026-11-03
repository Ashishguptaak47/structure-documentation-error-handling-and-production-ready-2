import pytest
import asyncio
from taskengine import TaskEngine, Task, TaskStatus


@pytest.mark.asyncio
async def test_successful_task_execution():
    engine = TaskEngine()
    await engine.start(num_workers=2)

    task = Task(name="Test Success", fn=lambda x: x + 10, args=(5,))
    task_id = await engine.submit(task)

    await engine.stop()
    res = engine.get_result(task_id)

    assert res is not None
    assert res.status == TaskStatus.COMPLETED
    assert res.result == 15
    assert res.error is None


@pytest.mark.asyncio
async def test_task_retries_and_dead_letter_queue():
    engine = TaskEngine()
    await engine.start(num_workers=2)

    def failing_fn():
        raise RuntimeError("Permanent failure")

    task = Task(name="Test Failure", fn=failing_fn, max_retries=1)
    task_id = await engine.submit(task)

    await engine.stop()
    res = engine.get_result(task_id)

    assert res is not None
    assert res.status == TaskStatus.DEAD_LETTER
    assert "Permanent failure" in res.error
    assert len(engine.get_dlq()) == 1
    assert engine.get_dlq()[0].id == task_id


@pytest.mark.asyncio
async def test_priority_ordering():
    engine = TaskEngine(max_concurrent=1)
    execution_order = []

    async def record(val: int):
        execution_order.append(val)

    t_low = Task(name="Low Priority", fn=record, args=(2,), priority=10)
    t_high = Task(name="High Priority", fn=record, args=(1,), priority=1)

    await engine.submit(t_low)
    await engine.submit(t_high)

    await engine.start(num_workers=1)
    await engine.stop()

    assert execution_order == [1, 2]
