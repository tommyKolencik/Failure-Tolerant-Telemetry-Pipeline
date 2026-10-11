"""Scaffold: future agent command-line entry point and process lifecycle.

M1: configure paths, start the synthetic producer and uploader, and shut down
cleanly. M3/M4: run acquisition independently of HTTP uploads so slow requests
cannot block host metrics or serial reception. Give each worker its own SQLite
connection. No executable agent is implemented yet.
"""
