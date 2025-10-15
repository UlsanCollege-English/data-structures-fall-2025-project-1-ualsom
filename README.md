[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/JWEh_q2R)
# Multi-Queue Round-Robin Café (Interactive CLI)

## How to run

(Insert directions to run your program here.)


## How to run tests locally
(Insert directions to run your tests here.)


## Complexity Notes
Briefly justify:

- Your queue design (e.g., circular buffer).

- Time complexity: enqueue, dequeue amortized O(1); run is O(#turns + total_minutes_worked).

- Space complexity: O(N) tasks + metadata.


## **Delete this section before submission.**
### Common pitfalls
- Display should print after each RUN turn only.

- Don’t advance time on empty or skipped queues.

- Enforce 1 ≤ steps ≤ #queues for RUN.

- Auto task IDs per queue: <queue_id>-NNN (zero-padded).

- Use exact messages:

    - Sorry, we're at capacity.

    - Sorry, we don't serve that.


### Grading rubric (I will be using this to grade your submission)

**__Correctness (50):__** RR behavior, logs, display-per-turn, auto task ids, menu handling, rejects.

**__Complexity notes (15):__** correct, concise, justified.

**__Student tests (15):__** ≥4 targeted, deterministic tests incl. steps validation.

**__Code quality (10):__** structure, type hints on public surfaces, docstrings, PEP 8.

**__Docs & UX (10):__** README completeness; exact messages; clear CLI.