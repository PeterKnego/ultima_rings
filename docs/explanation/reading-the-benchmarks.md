# About reading this crate's benchmark numbers

Why doesn't a performance-obsessed channel crate just say how many times
faster it is? The README quotes a ratio and immediately hedges it with a
machine, a core count, and a floor; the results directory is a pile of dated
lab reports rather than one triumphant table. This page explains why the
numbers are presented that way, and what a reader can and cannot safely take
from them.

## The number that wouldn't hold still

The project's benchmark program started the way most do: one machine, one
table, one headline ratio. The headline did not survive contact with its own
reruns. The same code on the same box measured 20% slower two days later —
and a third-party crate fell in lockstep, so it was the box, not a
regression. Later the box ran 2.4× slow for a day. Absolute throughput on a
shared VM turned out to be weather, not climate — which is why every report
in `docs/bench-results/` compares ratios within one session and never
absolutes across sessions.

Then the ratios failed too. A 1.88× MPSC lead over crossbeam-channel on the
dev box became a tie on a 16-core Xeon at the same topology
([`2026-08-15-bakeoff-rig.md`](../bench-results/2026-08-15-bakeoff-rig.md)).
SPSC "parity with rtrb" — measured four sessions running — became a 1.7–1.8×
lead on different silicon. Neither machine's answer generalizes; that
finding, not any particular ratio, is the durable result.

The final variable was the quietest. Pinning two threads to SMT siblings of
one core, then to two separate cores — same machine, same code — flipped
which crate wins the SPSC comparison outright, and moved crossbeam by 5.53×
([`2026-08-15-thread-placement.md`](../bench-results/2026-08-15-thread-placement.md)).
Every number recorded before that experiment had thread placement as an
uncontrolled variable, decided silently by the scheduler. Handoff benchmarks
are cache-line-transport benchmarks: with only two or three threads, extra
cores add no parallelism — they only lengthen the path a cache line travels.
The scheduler's placement decision *is* part of the system under test,
whether or not the harness admits it.

Hence the house rule: **a competitor ratio means nothing without a machine,
a core count, and a thread placement attached.**

## Why "versus crossbeam" is the noisiest comparison

Competing designs don't just differ in speed; they differ in how much the
environment moves them. This crate's cells reproduce within a few percent on
a quiet box. crossbeam-channel's cells swing across sessions by factors, are
the most placement-sensitive in the roster, and posted a 118% spread on the
16-core rig — a range so wide that quoting any single ratio against it is
storytelling. That asymmetry is itself informative (a design that ping-pongs
more cache lines pays every distance penalty more often — a plausible
mechanism, though unmeasured), but it means the popular question, "how many
times faster than crossbeam?", has the widest error bars of any question the
program can ask. The honest answer is a floor quoted against crossbeam's
best day, which is how the README states it.

It would be comfortable to conclude that noise is something other crates
have. It isn't. Push this crate's own `mpsc` past the core count — sixteen
producers on sixteen cores, then thirty-two, then sixty-four — and its cells
start swinging by a factor of three between rounds of the same session
([`2026-08-16-sharded-ladder-skew.md`](../bench-results/2026-08-16-sharded-ladder-skew.md)).
The reason is the same mechanism in a different costume: once spinning
producers outnumber the CPUs that can run them, throughput depends on which
threads the scheduler happens to favour, and that is not a property of the
code. The reports handle those rows the way they handle crossbeam's — quote
a direction, refuse a point estimate, and say which cells cannot support one.

## Assumptions are measured here, including flattering ones

Twice the program caught its own thumb on the scale. Three bake-offs
compared this crate's batched `drain` against competitors popping one item
at a time — an asymmetry in this crate's favor, or so it was assumed; when a
like-for-like cell was finally built, the batched API measured 10% *slower*
than single-item consumption
([`2026-08-14-bakeoff-v4.md`](../bench-results/2026-08-14-bakeoff-v4.md)).
And the long-advertised "batched claim" optimization for MPSC was retired
when the crate shipping exactly that design measured well behind. The
pattern to trust is not that the numbers always flatter the crate — they
don't — but that each assumption eventually got a dated experiment, and the
unflattering results stayed in the record.

That `drain` finding has since acquired an instructive twist. Batched
consumption lost by 10% on `spsc`, and the guidance written from it — don't
reach for `drain` expecting speed — was correct for the configuration it was
measured in and wrong as a general claim. On the `sharded` flavor the same
API won by 6.1×, because there the consumer pays per-item sweep bookkeeping
that batching amortizes, and the per-item path was the bottleneck all along
([`2026-08-16-sharded-string-drain.md`](../bench-results/2026-08-16-sharded-string-drain.md)).
One API, two flavors, opposite conclusions — both measured, both true. It is
a useful reminder that a benchmark result is a fact about a configuration,
and the sentence a reader carries away from it is usually broader than the
experiment that produced it. The scope creeps in the retelling, not in the
data.

## What to trust, and how far

From all of this, a reader's hierarchy of confidence:

Most trustworthy are comparisons of the crate against itself — an A/B of
one change, on one box, in one session — because both arms share the same
weather and roughly the same placement. Next come directions that
reproduced across machines: the SPSC lead over crossbeam-channel appears
everywhere measured, even though its magnitude ranges from 13× to 41×.
Below that sit single-session competitor ratios, honest only with their
conditions attached. And absolutes — Melem/s figures — are the least
portable numbers in the directory: they describe one box on one day, and
are best read as the *scale* of the phenomenon rather than a promise.

None of this is special to `ultima_rings`. Any microbenchmark of
handoff-bound code — channels, queues, locks — inherits the same three
variables, and a published table that names none of them has simply not
looked. The results directory (`docs/bench-results/`, indexed by its
README) is this project's answer: dated records, conditions stated,
retractions kept. Read it like a lab notebook, not a leaderboard.
