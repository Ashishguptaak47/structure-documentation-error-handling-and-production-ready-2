import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

from .models import Task, TaskResult, TaskStatus

logger = logging.getLogger("TaskPulse")


class TaskEngine:
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._results: Dict[str, TaskResult] = {}
        self._dlq: List[Task] = []
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._is_running = False
        self._workers: List[asyncio.Task] = []

    async def submit(self, task: Task) -> str:
        await self._queue.put((task.priority, task.created_at.timestamp(), task))
        logger.info(f"Enqueued task '{task.name}' (ID: {task.id}) with priority {task.priority}")
        return task.id

    async def _execute_task(self, task: Task) -> TaskResult:
        async with self._semaphore:
            task.status = TaskStatus.RUNNING
            start_time = time.perf_counter()
            
            try:
                if asyncio.iscoroutinefunction(task.fn):
                    res = await asyncio.wait_for(task.fn(*task.args, **task.kwargs), timeout=task.timeout_seconds)
                else:
                    res = await asyncio.to_thread(task.fn, *task.args, **task.kwargs)

                elapsed = (time.perf_counter() - start_time) * 1000
                task.status = TaskStatus.COMPLETED
                return TaskResult(
                    task_id=task.id,
                    status=TaskStatus.COMPLETED,
                    result=res,
                    execution_time_ms=elapsed,
                    completed_at=datetime.utcnow()
                )
            except Exception as exc:
                elapsed = (time.perf_counter() - start_time) * 1000
                logger.warning(f"Task '{task.name}' ({task.id}) failed: {exc}")
                
                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    task.status = TaskStatus.RETRYING
                    backoff = 0.05 * (2 ** task.retry_count)
                    await asyncio.sleep(backoff)
                    return await self._execute_task(task)
                else:
                    task.status = TaskStatus.DEAD_LETTER
                    self._dlq.append(task)
                    return TaskResult(
                        task_id=task.id,
                        status=TaskStatus.DEAD_LETTER,
                        error=str(exc),
                        execution_time_ms=elapsed,
                        completed_at=datetime.utcnow()
                    )

    async def _worker(self):
        while self._is_running or not self._queue.empty():
            try:
                _, _, task = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                continue

            res = await self._execute_task(task)
            self._results[task.id] = res
            self._queue.task_done()

    async def start(self, num_workers: int = 3):
        self._is_running = True
        self._workers = [asyncio.create_task(self._worker()) for _ in range(num_workers)]
        logger.info(f"TaskEngine started with {num_workers} worker tasks.")

    async def stop(self):
        self._is_running = False
        await self._queue.join()
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        logger.info("TaskEngine shut down gracefully.")

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self._results.get(task_id)

    def get_dlq(self) -> List[Task]:
        return list(self._dlq)
