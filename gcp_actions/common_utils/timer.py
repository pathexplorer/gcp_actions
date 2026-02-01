import time
from typing import Dict, Union

from contextlib import contextmanager
from unittest import result
import logging
logger = logging.getLogger(__name__)

@contextmanager
def time_stage(stage_name: str, total_times_dict: dict):
    """
    A context manager to time a pipeline stage and log the duration.
    """
    start_time = time.perf_counter()
    logger.debug(f"--- Starting {stage_name}...")
    try:
        yield  # Execution happens here
    except Exception as e:
        logger.error(f"!!! Stage {stage_name} FAILED: {e}", exc_info=True)

        raise  # Re-raise the exception to stop the pipeline
    finally:
        end_time = time.perf_counter()
        duration = end_time - start_time
        total_times_dict[stage_name] = duration
        logger.debug(f"--- Finished {stage_name}: {duration:.3f} seconds.")


def log_duration_table(all_stage_times, pipeline_type: str) -> None:
    """
    Calculates the percentage of total duration for each item and logs the
    results in a formatted table using logging.info.
    :param pipeline_type: public or private
    :param all_stage_times:
    """
    total_duration = sum(all_stage_times.values())

    name_duration_dict = {}
    for stage, duration in all_stage_times.items():
        name_duration_dict[stage] = duration

    if not name_duration_dict:
        logger.warning("The duration dictionary is empty.")
        return

    if total_duration == 0:
        # Handle the edge case of zero total duration gracefully
        logger.error("Total duration is zero. Check your input data.")
        return

    # --- 1. Define Table Structure and Header ---

    # Use f-string formatting for alignment: < for left, > for right
    header = f"| {'Name':<25} | {'Duration (s)':>15} | {'% of Total':>12} |"
    separator = f"+{'-' * 27}+{'-' * 17}+{'-' * 14}+"

    # Start the logged output with the header and separator
    log_output = [separator, header, separator]

    # --- 2. Iterate and Format Data Rows ---
    for name, duration in name_duration_dict.items():
        try:
            # Calculate percentage
            percentage = (duration / total_duration) * 100

            # Create the data row
            row = f"| {name:<25} | {duration:>15.3f} | {percentage:>11.2f}% |"
            log_output.append(row)

        except TypeError as e:
            logger.error(f"Skipping entry for '{name}'. Duration must be numeric. Error: {e}")
            continue

    # --- 3. Add Footer and Log Total ---
    log_output.append(separator)
    # Add a final row for the total duration
    total_row = f"| {'TOTAL':<25} | {total_duration:>15.3f} | {'100.00%':>12} |"
    log_output.append(total_row)
    log_output.append(separator)

    # --- 4. Combine and Log ---
    # Join all lines with a newline character and pass to logging.info
    final_table = "\n".join(log_output)
    logger.info(f"\n--- Duration of {pipeline_type} Pipeline Stages ---\n" + final_table)


def run_timer(func):
    """
    A decorator that standardizes the timing of a function.
    Ignore measures that are smaller than 0.01 seconds
    """
    def wrapper(*args, **kwargs):
        t_start = time.time()
        t_result = func(*args, **kwargs)
        t_total = time.time() - t_start
        if round(t_total, 5) > 0.01:
            logger.debug(f"{func.__name__} took {t_total:.2f}s")
        else:
            logger.debug(f"{func.__name__} shorter than 0.01s")
        return t_result
    return wrapper

if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    @run_timer
    def mult(a, b, c):
        time.sleep(0.01)
        return a * b * c
    print(mult(1,6, 8585))
