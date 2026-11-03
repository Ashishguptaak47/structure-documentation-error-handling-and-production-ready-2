import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class TaskResult(BaseModel):
    task_id: str
    status: TaskStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    completed_at: Optional[datetime] = None


class Task(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    fn: Callable[..., Any]
    args: tuple = Field(default_factory=tuple)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    max_retries: int = 2
    retry_count: int = 0
    priority: int = 10
    timeout_seconds: float = 10.0
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
