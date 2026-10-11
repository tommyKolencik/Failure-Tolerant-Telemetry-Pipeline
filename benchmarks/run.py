"""Scaffold: future repeatable load, batching, and recovery benchmark runner.

M5: offer 100 events/sec initially, increase rates, and run each rate for
60 seconds three times. Compare batch sizes 1, 20, and 100 on the same workload.
Save seed, configuration, versions, hardware, commit ID, raw observations, and
exact accepted/rejected/quarantined/stored/missing/conflicting event sets under
benchmarks/results/. Correlate same-host monotonic publish and durable-ACK
timestamps by event key; measure batch HTTP latency separately. Use plotnine
for plots. No benchmark runner or measured results exist yet.
"""
