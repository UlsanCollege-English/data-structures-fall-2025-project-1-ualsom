"""
Scheduler + QueueRR for Multi-Queue Round-Robin Café project.

Implements:
- QueueRR: a simple FIFO queue without using deque or queue.Queue.
- Scheduler: manages multiple queues, tasks, skipping, and round-robin scheduling.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

# Required menu items
REQUIRED_MENU: Dict[str, int] = {
    "americano": 2,
    "latte": 3,
    "cappuccino": 3,
    "mocha": 4,
    "tea": 1,
    "macchiato": 2,
    "hot_chocolate": 4,
}


@dataclass
class Task:
    task_id: str
    remaining: int


class QueueRR:
    """
    FIFO queue without using collections.deque or queue.Queue.
    Implemented with two stacks.
    """

    def __init__(self, queue_id: str, capacity: int) -> None:
        self.queue_id = queue_id
        self.capacity = capacity
        self.in_stack: List[Task] = []
        self.out_stack: List[Task] = []

    def enqueue(self, task: Task) -> bool:
        if len(self) >= self.capacity:
            return False
        self.in_stack.append(task)
        return True

    def dequeue(self) -> Optional[Task]:
        if not self.out_stack:
            # Transfer all tasks from in_stack to out_stack
            while self.in_stack:
                self.out_stack.append(self.in_stack.pop())
        if self.out_stack:
            return self.out_stack.pop()
        return None

    def __len__(self) -> int:
        return len(self.in_stack) + len(self.out_stack)

    def peek_all(self) -> List[Task]:
        # Returns list of tasks in queue order without modifying queue
        # out_stack is reversed order, so reverse it to front first
        return self.out_stack[::-1] + self.in_stack


class Scheduler:
    """
    Manages multiple queues and round-robin scheduling.
    """

    def __init__(self) -> None:
        self.time = 0
        self.queues_order: List[str] = []
        self.queues: Dict[str, QueueRR] = {}
        self.id_counters: Dict[str, int] = {}
        self.skip_flags: Dict[str, bool] = {}
        self.rr_index = 0
        self.menu_map = REQUIRED_MENU.copy()

    def menu(self) -> Dict[str, int]:
        return self.menu_map.copy()

    def next_queue(self) -> Optional[str]:
        if not self.queues_order:
            return None
        # Skip empty queues or those marked for skip?
        start = self.rr_index
        n = len(self.queues_order)
        for i in range(n):
            idx = (start + i) % n
            qid = self.queues_order[idx]
            if len(self.queues[qid]) > 0 and not self.skip_flags.get(qid, False):
                return qid
        # If none found that are non-empty and not skipped, return None or next queue anyway
        # To avoid confusion, just return the current rr_index queue
        return self.queues_order[self.rr_index] if self.queues_order else None

    def create_queue(self, queue_id: str, capacity: int) -> List[str]:
        logs = []
        if queue_id in self.queues:
            # If already exists, ignore (not specified, but safer)
            return logs
        self.queues_order.append(queue_id)
        self.queues[queue_id] = QueueRR(queue_id, capacity)
        self.id_counters[queue_id] = 0
        self.skip_flags[queue_id] = False
        logs.append(f"time={self.time} event=create queue={queue_id}")
        return logs

    def enqueue(self, queue_id: str, item_name: str) -> List[str]:
        logs = []
        if queue_id not in self.queues:
            logs.append(f"time={self.time} event=error reason=unknown_queue")
            return logs

        if item_name not in self.menu_map:
            print("Sorry, we don't serve that.")
            logs.append(f"time={self.time} event=reject queue={queue_id} item={item_name} reason=unknown_item")
            return logs

        capacity = self.queues[queue_id].capacity
        current_len = len(self.queues[queue_id])

        if current_len >= capacity:
            print("Sorry, we're at capacity.")
            logs.append(f"time={self.time} event=reject queue={queue_id} item={item_name} reason=full")
            return logs

        self.id_counters[queue_id] += 1
        task_id = f"{queue_id}-{self.id_counters[queue_id]:03d}"
        burst = self.menu_map[item_name]
        task = Task(task_id, burst)
        success = self.queues[queue_id].enqueue(task)
        if success:
            logs.append(f"time={self.time} event=enqueue queue={queue_id} task={task_id} remaining={burst}")
        else:
            # Should not reach here due to prior check, but safe fallback
            print("Sorry, we're at capacity.")
            logs.append(f"time={self.time} event=reject queue={queue_id} item={item_name} reason=full")
        return logs

    def mark_skip(self, queue_id: str) -> List[str]:
        logs = []
        if queue_id not in self.queues:
            logs.append(f"time={self.time} event=error reason=unknown_queue")
            return logs
        self.skip_flags[queue_id] = True
        logs.append(f"time={self.time} event=skip queue={queue_id}")
        return logs

    def run(self, quantum: int, steps: Optional[int]) -> List[str]:
        logs = []
        n_queues = len(self.queues_order)
        if n_queues == 0:
            return logs  # Nothing to run

        if steps is not None and (steps < 1 or steps > n_queues):
            logs.append(f"time={self.time} event=error reason=invalid_steps")
            return logs

        # If steps is None, run until all queues empty and no skips pending
        # We'll implement that as a loop

        def all_empty_and_no_skips() -> bool:
            for qid in self.queues_order:
                if len(self.queues[qid]) > 0 or self.skip_flags.get(qid, False):
                    return False
            return True

        steps_to_run = steps if steps is not None else 1000000  # large number for indefinite

        turns_done = 0
        while turns_done < steps_to_run:
            if steps is None and all_empty_and_no_skips():
                break

            qid = self.queues_order[self.rr_index]
            queue = self.queues[qid]

            # Check skip flag
            if self.skip_flags.get(qid, False):
                # Log skip, do not advance time
                logs.append(f"time={self.time} event=run queue={qid}")
                self.skip_flags[qid] = False
                # Advance RR pointer
                self.rr_index = (self.rr_index + 1) % n_queues
                turns_done += 1
                # After each turn, print display logs
                logs.extend(self.display())
                continue

            # If queue empty, zero-time transition
            if len(queue) == 0:
                logs.append(f"time={self.time} event=run queue={qid}")
                # Advance RR pointer
                self.rr_index = (self.rr_index + 1) % n_queues
                turns_done += 1
                logs.extend(self.display())
                continue

            # Work occurs
            logs.append(f"time={self.time} event=run queue={qid}")
            task = queue.dequeue()
            assert task is not None  # safe due to length check

            work_time = min(task.remaining, quantum)
            task.remaining -= work_time
            self.time += work_time

            logs.append(f"time={self.time} event=work queue={qid} task={task.task_id} ran={work_time} remaining={task.remaining}")

            if task.remaining == 0:
                logs.append(f"time={self.time} event=finish queue={qid} task={task.task_id}")
            else:
                # Task still has remaining time, enqueue back
                queue.enqueue(task)

            # Advance RR pointer
            self.rr_index = (self.rr_index + 1) % n_queues
            turns_done += 1

            # After each turn, append display logs
            logs.extend(self.display())

        return logs

    def display(self) -> List[str]:
        lines = []
        lines.append(f"display time={self.time} next={self.next_queue() or 'none'}")

        menu_str = ",".join(f"{name}:{self.menu_map[name]}" for name in sorted(self.menu_map))
        lines.append(f"display menu=[{menu_str}]")

        for qid in self.queues_order:
            queue = self.queues[qid]
            n = len(queue)
            cap = queue.capacity
            skip_str = " skip" if self.skip_flags.get(qid, False) else ""

            tasks = queue.peek_all()
            tasks_str = ",".join(f"{task.task_id}:{task.remaining}" for task in tasks)
            lines.append(f"display {qid} [{n}/{cap}]{skip_str} -> [{tasks_str}]")

        return lines
