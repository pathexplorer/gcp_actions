import time
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

def show_table(all_stage_times, pipeline_type: str):

    total_time = sum(all_stage_times.values())

    # Define Column Widths
    # Stage Name column: 31 characters, left-aligned (<)
    # Duration column: 10 characters, right-aligned (>)
    WIDTH = 45  # Total approximate width for the line separator

    logger.info(f"--- {pipeline_type} Pipeline Stage Durations Summary ---")

    # 2. Log the Header Row
    header_line = f"{'STAGE NAME':<31} {'DURATION (s)':>10}"
    logger.info(header_line)
    logger.info("-" * WIDTH)

    # 3. Log each stage duration
    for stage, duration in all_stage_times.items():
        # Use f-string: <31 for left-aligning the stage name,
        # >10.3f for right-aligning the duration with 3 decimal places
        stage_line = f"{stage:<31} {duration:>10.3f}"
        logger.info(stage_line)

    # 4. Log the Footer Row (Total)
    logger.info("-" * WIDTH)

    # The text that actually needs padding/alignment
    text_to_pad = f"TOTAL {pipeline_type.upper()} PIPELINE TIME"

    # Apply padding to the text, then add the '**' on either side.
    # This ensures the text inside the quotes is exactly 31 characters wide.
    # Note: The entire final string will be longer than 31 characters.
    total_line = f"**{text_to_pad:<31}** {total_time:>10.3f}"
    logger.info(total_line)