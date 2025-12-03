import time
from typing import Dict, Union
import logging
from contextlib import contextmanager

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

# def show_table(all_stage_times, pipeline_type: str):
#
#     total_time = sum(all_stage_times.values())
#
#     # Define Column Widths
#     # Stage Name column: 31 characters, left-aligned (<)
#     # Duration column: 10 characters, right-aligned (>)
#     WIDTH = 45  # Total approximate width for the line separator
#
#     logger.info(f"--- {pipeline_type} Pipeline Stage Durations Summary ---")
#
#     # 2. Log the Header Row
#     header_line = f"{'STAGE NAME':<31} {'DURATION (s)':>10}"
#     logger.info(header_line)
#     logger.info("-" * WIDTH)
#
#     # 3. Log each stage duration
#     percents = {}
#     for stage, duration in all_stage_times.items():
#         # Use f-string: <31 for left-aligning the stage name,
#         # >10.3f for right-aligning the duration with 3 decimal places
#         percents[stage] = duration
#         stage_line = f"{stage:<31} {duration:>10.3f}"
#         logger.info(stage_line)
#
#
#
#     # 4. Log the Footer Row (Total)
#     logger.info("-" * WIDTH)
#
#     # The text that actually needs padding/alignment
#     text_to_pad = f"TOTAL {pipeline_type.upper()} PIPELINE TIME"
#
#     # Apply padding to the text, then add the '**' on either side.
#     # This ensures the text inside the quotes is exactly 31 characters wide.
#     # Note: The entire final string will be longer than 31 characters.
#     total_line = f"**{text_to_pad:<31}** {total_time:>10.3f}"
#     logger.info(total_line)



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
        logging.warning("The duration dictionary is empty.")
        return

    if total_duration == 0:
        # Handle the edge case of zero total duration gracefully
        logging.error("Total duration is zero. Check your input data.")
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
            logging.error(f"Skipping entry for '{name}'. Duration must be numeric. Error: {e}")
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
    logging.info(f"\n--- Duration of {pipeline_type} Pipeline Stages ---\n" + final_table)