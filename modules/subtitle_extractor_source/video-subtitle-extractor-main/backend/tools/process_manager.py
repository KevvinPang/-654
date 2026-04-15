# -*- coding: utf-8 -*-
"""
Lightweight process registry used by the subtitle extractor.
"""

import atexit
import concurrent.futures
import platform
import subprocess


class ProcessManager:
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = ProcessManager()
        return cls._instance

    def __init__(self):
        self.processes = {}
        atexit.register(self.terminate_all)

    def add_process(self, process, name=None):
        if process is None:
            return None
        process_id = name or f"Process:{id(process)}"
        self.processes[process_id] = process
        print(f"Added process: {process_id}, PID: {getattr(process, 'pid', 'unknown')}")
        return process_id

    def add_pid(self, pid, name=None):
        if pid is None:
            return None
        process_id = name or f"Pid:{pid}"
        self.processes[process_id] = int(pid)
        print(f"Added process: {process_id}, PID: {pid}")
        return process_id

    def remove_process(self, process_id):
        if process_id in self.processes:
            del self.processes[process_id]
            print(f"Removed process: {process_id}")
            return True
        return False

    def terminate_all(self):
        if not self.processes:
            return
        try:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for process in list(self.processes.values()):
                    if isinstance(process, int):
                        futures.append(executor.submit(self.terminate_by_pid, process))
                    else:
                        futures.append(executor.submit(self.terminate_by_process, process))
                concurrent.futures.wait(futures)
        except RuntimeError:
            for process in list(self.processes.values()):
                try:
                    if isinstance(process, int):
                        self.terminate_by_pid(process)
                    else:
                        self.terminate_by_process(process)
                except Exception:
                    pass
        finally:
            self.processes.clear()

    def terminate_by_process(self, process):
        if process is None:
            return
        pid = getattr(process, "pid", None)
        try:
            print(f"Terminating process: pid: {pid}")
            if hasattr(process, "poll") and process.poll() is not None:
                return
            if hasattr(process, "terminate"):
                process.terminate()
            if hasattr(process, "join"):
                try:
                    process.join(timeout=3)
                except Exception:
                    pass
            if hasattr(process, "wait"):
                try:
                    process.wait(timeout=3)
                except Exception:
                    pass
            if hasattr(process, "poll") and process.poll() is None and hasattr(process, "kill"):
                process.kill()
        except Exception:
            pass
        if pid is not None:
            self.terminate_by_pid(pid)

    def terminate_by_pid(self, pid):
        if pid is None:
            return
        try:
            if platform.system() == "Windows":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(pid)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=3,
                )
            else:
                subprocess.run(
                    ["pkill", "-9", "-P", str(pid)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=2,
                )
                subprocess.run(
                    ["kill", "-9", str(pid)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=3,
                )
        except Exception as exc:
            print(f"Error forcibly terminating process with PID {pid}: {exc}")
