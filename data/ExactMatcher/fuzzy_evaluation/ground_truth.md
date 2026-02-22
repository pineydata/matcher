# Fuzzy Evaluation Ground Truth

Ground truth: 30 pairs. IDs are eval_left_1..eval_left_30 and eval_right_1..eval_right_30.

- Pairs 1–15: identical on both sides (exact email finds them).
- Pairs 16–30: same person, different emails; name variants on right (typos/spelling). Only fuzzy on full_name finds these.
- Build ground truth in notebooks: left_id=eval_left_1..30, right_id=eval_right_1..30.
