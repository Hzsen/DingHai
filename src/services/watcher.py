import os
import time
from threading import Lock, Timer
from typing import Optional

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from src.core.screener_logic import load_config, run_etl


class DebouncedETLHandler(FileSystemEventHandler):
    def __init__(self, config_path: str, debounce_seconds: float = 2.0) -> None:
        super().__init__()
        self.config_path = config_path
        self.debounce_seconds = debounce_seconds
        self._lock = Lock()
        self._timer: Optional[Timer] = None

    def on_created(self, event) -> None:
        if event.is_directory:
            return
        self._schedule()

    def on_moved(self, event) -> None:
        if event.is_directory:
            return
        self._schedule()

    def _schedule(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
            self._timer = Timer(self.debounce_seconds, self._run_etl)
            self._timer.start()

    def _run_etl(self) -> None:
        try:
            output_path = run_etl(config_path=self.config_path)
            print(f"ETL completed: {output_path}")
        except Exception as exc:  # noqa: BLE001 - log and continue watching
            print(f"ETL failed: {exc}")


def start_watcher(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)
    os.makedirs(config.data_dir, exist_ok=True)
    handler = DebouncedETLHandler(config_path=config_path)
    observer = Observer()
    observer.schedule(handler, config.data_dir, recursive=False)
    observer.start()
    print(f"Watching {config.data_dir} for new files...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    start_watcher()
