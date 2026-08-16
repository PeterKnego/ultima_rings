# Explanation

Background reading — the *why* behind the crate, for study rather than
mid-task lookup.

The cornerstone is the [design document](../design.md): the invariant,
memory ordering, and justification for every atomic operation in the crate,
the wake protocol, disconnect semantics, the alternatives that were weighed
(including the sharded prototype), and a soundness-pitfall checklist mapping
other channel crates' historical bugs to this crate's mitigations.

Alongside it:

- [About reading this crate's benchmark numbers](reading-the-benchmarks.md)
  — why no ratio is quoted without a machine, core count, and thread
  placement attached, and what a reader can safely trust.
