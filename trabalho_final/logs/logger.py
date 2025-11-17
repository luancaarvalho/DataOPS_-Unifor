import logging
from pathlib import Path
import time
from functools import wraps
from typing import Callable, Any


class Logger:
    def __init__(self, name: str = "logger", level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(level)

            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            ch.setFormatter(formatter)

            self.logger.addHandler(ch)

            log_file = Path(__file__).parent / "etl.log"
            file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            func_name = func.__qualname__
            self.logger.info(f"Started: {func_name}")
            start_time = time.perf_counter()

            try:
                result = func(*args, **kwargs)
                elapsed = time.perf_counter() - start_time
                self.logger.info(f"Finished: {func_name} (Duration: {elapsed:.3f}s)")
                return result
            except Exception as e:
                self.logger.exception(f"Exception in {func_name}: {e}")
                raise

        return wrapper
