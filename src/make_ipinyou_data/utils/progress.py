"""Logging and progress tracking utilities.

Rich-based progress display and logging setup for pipeline execution.
Progress bar is fixed at the bottom, logs scroll above in a panel.
"""

from __future__ import annotations

import sys
import time

from loguru import logger
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)


class ProgressTracker:
    """Pipeline progress tracker using Rich Progress.

    Manages a progress bar at the bottom while allowing logs to scroll above it.
    Uses loguru for logging, redirected to the Rich console.

    Parameters
    ----------
    total_steps : int
        Total number of steps in the pipeline.
    enabled : bool
        Whether to enable the progress display.
    verbose : bool, optional
        Whether to output INFO level logs. Defaults to True.
    description : str, optional
        Description shown next to the progress bar. Defaults to "Pipeline Progress".
    """

    def __init__(
        self,
        total_steps: int,
        enabled: bool,
        verbose: bool = True,
        description: str = "Pipeline Progress",
    ):
        self.enabled = enabled
        self.verbose = verbose
        self.total_steps = total_steps
        self.current_step = 0
        self.description = description

        self.console = Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(complete_style="green", finished_style="bold green"),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=self.console,
            transient=False,
        )
        self.task_id: TaskID | None = None

    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format seconds into HH:MM:SS."""
        total = int(seconds)
        hours, rem = divmod(total, 3600)
        minutes, secs = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def __enter__(self) -> ProgressTracker:
        """Start the progress display and configure logging."""
        if self.enabled:
            self.console.print(
                "\n[bold cyan]🚀 Starting iPinYou Data Pipeline[/bold cyan]\n"
            )
            self.progress.start()
            self.task_id = self.progress.add_task(
                self.description, total=self.total_steps
            )

        # Configure loguru to use Rich console
        logger.remove()
        level = "INFO" if self.verbose else "WARNING"

        # Loguru will now only be used for unexpected errors or non-progress logs
        # We use a simpler format as Rich console.log will handle the rest
        log_format = "<dim>{time:HH:mm:ss}</dim> | <level>{level: <8}</level> | <level>{message}</level>"

        if self.enabled:
            logger.add(
                lambda msg: self.console.print(msg.rstrip()),
                level=level,
                format=log_format,
                colorize=True,
            )
        else:
            logger.add(sys.stderr, level=level, format=log_format)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> bool:
        """Stop the progress display and print summary."""
        if self.enabled:
            duration = "00:00:00"
            if self.task_id is not None:
                task = self.progress.tasks[self.task_id]
                duration = self._format_duration(task.elapsed or 0.0)

            self.progress.stop()

            if not exc_type:
                self.console.print(
                    "\n[bold green]✔ Pipeline completed successfully![/bold green]"
                )
                self.console.print(
                    f"[dim]Total Steps: {self.total_steps} | Elapsed Time: {duration}[/dim]\n"
                )
        return False

    def log(self, message: str, style: str = "") -> None:
        """Log a message to the Rich console."""
        if self.enabled:
            self.console.log(f"[{style}]{message}[/{style}]" if style else message)
        else:
            logger.info(message)

    def info(self, message: str) -> None:
        """Log an info message."""
        self.log(message, style="blue")

    def success(self, message: str) -> None:
        """Log a success message."""
        self.log(message, style="bold green")

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.log(message, style="bold yellow")

    def error(self, message: str) -> None:
        """Log an error message."""
        self.log(message, style="bold red")

    def step(self, message: str, advance: int = 1) -> None:
        """Advance progress and log the step."""
        self.current_step += advance

        if self.enabled and self.task_id is not None:
            self.progress.update(self.task_id, advance=advance, description=message)

        # Use Rich console.log for the step message
        step_prefix = f"[bold cyan][{self.current_step}/{self.total_steps}][/bold cyan]"
        self.log(f"{step_prefix} {message}")

    def add_task(self, description: str, total: int) -> TaskID:
        """Add a new task to the progress bar."""
        if self.enabled:
            return self.progress.add_task(description, total=total)
        return TaskID(0)

    def remove_task(self, task_id: TaskID) -> None:
        """Remove a task from the progress bar."""
        if self.enabled:
            self.progress.remove_task(task_id)

    def finish(self) -> None:
        """Log final completion (optional, __exit__ handles summary)."""
        self.success("All pipeline steps completed.")


if __name__ == "__main__":
    """Demo: Progress tracker with simulated pipeline steps."""

    print("=== ProgressTracker Demo ===\n")

    with ProgressTracker(
        total_steps=6, enabled=True, description="Demo Pipeline"
    ) as tracker:
        tracker.step("Initializing environment")
        time.sleep(0.5)

        tracker.step("Loading configuration")
        time.sleep(0.5)

        tracker.step("Processing data batch 1/3")
        time.sleep(0.8)

        tracker.step("Processing data batch 2/3")
        time.sleep(0.8)

        tracker.step("Processing data batch 3/3")
        time.sleep(0.8)

        tracker.step("Finalizing results")
        time.sleep(0.5)
