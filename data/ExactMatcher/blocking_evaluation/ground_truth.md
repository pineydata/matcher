# Blocking Evaluation Ground Truth

Ground truth: 30 pairs. IDs are eval_left_1..eval_left_30 and eval_right_1..eval_right_30.

- Pairs 1–15: same zip on left and right (blocking on zip keeps them).
- Pairs 16–30: same person (same email) but different zip; blocking on zip splits them (recall drops).
- Build ground truth in notebooks: left_id=eval_left_1..30, right_id=eval_right_1..30.
