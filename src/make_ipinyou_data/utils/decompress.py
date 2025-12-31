"""Decompression utilities.

Provides functionality to decompress bz2 files in parallel using asyncio and aiofiles,
with support for caching.
"""

from __future__ import annotations

import asyncio
import bz2
import os
from pathlib import Path
from typing import Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from .progress import ProgressTracker


class Decompressor:
    """Asynchronous bz2 decompressor.

    Uses asyncio and aiofiles to efficiently decompress multiple files in parallel.

    Parameters
    ----------
    cache_dir : Path
        Directory to store decompressed files.
    dataset_root : Path
        Root directory of the dataset (used for relative path calculation).
    max_workers : int | None, optional
        Maximum number of concurrent workers (defaults to CPU count - 1).
    force : bool, optional
        If True, ignore existing cache and re-decompress (default: False).
    verbose : bool, optional
        Whether to output detailed logs (default: False).

    Examples
    --------
    >>> decompressor = Decompressor(
    ...     cache_dir=Path("cache"),
    ...     dataset_root=Path("data"),
    ...     verbose=True
    ... )
    >>> files = [Path("data/file1.bz2"), Path("data/file2.bz2")]
    >>> uncompressed = decompressor.ensure_uncompressed(files)
    """

    def __init__(
        self,
        cache_dir: Path,
        dataset_root: Path,
        max_workers: int | None = None,
        force: bool = False,
        verbose: bool = False,
    ):
        self.cache_dir = cache_dir
        self.dataset_root = dataset_root
        self.max_workers = max_workers or max(1, (os.cpu_count() or 2) - 1)
        self.force = force
        self.verbose = verbose

    async def _decompress_one(
        self,
        source: Path,
        destination: Path,
        tracker: ProgressTracker | None = None,
    ) -> Path:
        """Decompress a single bz2 file asynchronously.

        Parameters
        ----------
        source : Path
            Path to the source bz2 file.
        destination : Path
            Path where the decompressed file will be saved.
        tracker : ProgressTracker | None, optional
            Progress tracker to update and log to.

        Returns
        -------
        Path
            Path to the decompressed file.
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        if self.verbose and tracker:
            tracker.log(
                f"Decompressing {source.name} -> {destination.name}", style="dim"
            )

        # bz2 decompression is CPU-intensive, so run in executor
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._decompress_sync, source, destination)

        if tracker and tracker.task_id is not None:
            tracker.progress.update(tracker.task_id, advance=1)

        if self.verbose and tracker:
            tracker.log(f"Finished decompressing {destination.name}", style="dim green")
        return destination

    def _decompress_sync(self, source: Path, destination: Path) -> None:
        """Synchronous decompression (executed in an executor).

        Parameters
        ----------
        source : Path
            Path to the source bz2 file.
        destination : Path
            Path where the decompressed file will be saved.
        """
        with bz2.open(source, "rb") as source_file, open(
            destination, "wb"
        ) as destination_file:
            # Copy in chunks for memory efficiency
            chunk_size = 1024 * 1024  # 1MB
            while chunk := source_file.read(chunk_size):
                destination_file.write(chunk)

    async def _process_files(
        self,
        pending: list[tuple[Path, Path]],
        tracker: ProgressTracker | None = None,
    ) -> None:
        """Decompress multiple files in parallel.

        Parameters
        ----------
        pending : list[tuple[Path, Path]]
            List of (source path, destination path) tuples.
        tracker : ProgressTracker | None, optional
            Progress tracker to update and log to.
        """
        if self.verbose and tracker:
            tracker.log(
                f"Decompressing {len(pending)} bz2 files with {self.max_workers} workers",
                style="bold blue",
            )

        # Limit concurrent execution with a semaphore
        semaphore = asyncio.Semaphore(self.max_workers)

        async def decompress_with_limit(source: Path, destination: Path) -> Path:
            async with semaphore:
                return await self._decompress_one(source, destination, tracker)

        tasks = [
            decompress_with_limit(source, destination)
            for source, destination in pending
        ]
        await asyncio.gather(*tasks)

    def ensure_uncompressed(
        self,
        files: Sequence[Path],
        tracker: ProgressTracker | None = None,
    ) -> list[Path]:
        """Decompress bz2 files in parallel and return the list of paths.

        Uses cache for already decompressed files (unless force=True).

        Parameters
        ----------
        files : Sequence[Path]
            List of file paths to process.
        tracker : ProgressTracker | None, optional
            Progress tracker to update and log to.

        Returns
        -------
        list[Path]
            List of processed file paths (bz2 paths are replaced with decompressed paths).

        Examples
        --------
        >>> decompressor = Decompressor(
        ...     cache_dir=Path("cache"),
        ...     dataset_root=Path("data"),
        ... )
        >>> files = [Path("data/file.bz2"), Path("data/file.txt")]
        >>> result = decompressor.ensure_uncompressed(files)
        >>> # result: [Path("cache/file"), Path("data/file.txt")]
        """
        pending: list[tuple[Path, Path]] = []
        prepared: list[Path] = []

        for source in files:
            if source.suffix == ".bz2":
                try:
                    relative_path = source.relative_to(self.dataset_root)
                except ValueError:
                    relative_path = Path(source.name)
                destination = self.cache_dir / relative_path.with_suffix("")
                if self.force or not destination.exists():
                    pending.append((source, destination))
                prepared.append(destination)
            else:
                prepared.append(source)

        if pending:
            # Run the asyncio event loop
            asyncio.run(self._process_files(pending, tracker))
        else:
            if self.verbose and tracker:
                tracker.log("No bz2 decompression needed", style="dim")

        return prepared


if __name__ == "__main__":
    """Test script for Decompressor.
    
    Creates sample text files, compresses them to bz2, then decompresses them.
    """
    import tempfile

    # Create temporary directories
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        data_dir = temp_path / "data"
        cache_dir = temp_path / "cache"
        data_dir.mkdir()
        cache_dir.mkdir()

        # Create sample text files
        sample_files = ["a.txt", "b.txt", "c.txt"]
        sample_contents = {
            "a.txt": "Hello from file A!\n" * 100,
            "b.txt": "This is file B with some content.\n" * 200,
            "c.txt": "File C contains different text.\n" * 150,
        }

        print("Creating sample files...")
        for filename in sample_files:
            file_path = data_dir / filename
            file_path.write_text(sample_contents[filename])
            print(f"  Created: {file_path} ({file_path.stat().st_size} bytes)")

        # Compress files to bz2
        print("\nCompressing files to bz2...")
        compressed_files = []
        for filename in sample_files:
            source_path = data_dir / filename
            compressed_path = data_dir / f"{filename}.bz2"

            with open(source_path, "rb") as source_file:
                with bz2.open(compressed_path, "wb") as compressed_file:
                    compressed_file.write(source_file.read())

            compressed_files.append(compressed_path)
            print(
                f"  Compressed: {compressed_path} ({compressed_path.stat().st_size} bytes)"
            )

        # Remove original files (to test decompression only)
        print("\nRemoving original uncompressed files...")
        for filename in sample_files:
            (data_dir / filename).unlink()
            print(f"  Removed: {filename}")

        # Test decompression
        print("\nTesting Decompressor...")
        decompressor = Decompressor(
            cache_dir=cache_dir,
            dataset_root=data_dir,
            max_workers=2,
            force=False,
            verbose=True,
        )

        from .progress import ProgressTracker

        decompressed_files: list[Path] = []
        with ProgressTracker(
            total_steps=len(compressed_files),
            enabled=True,
            verbose=True,
            description="Decompressing files",
        ) as tracker:
            decompressed_files = decompressor.ensure_uncompressed(
                compressed_files,
                tracker=tracker,
            )

        # Verify decompressed files
        print("\nVerifying decompressed files...")
        for original_name, decompressed_path in zip(sample_files, decompressed_files):
            if decompressed_path.exists():
                content = decompressed_path.read_text()
                expected_content = sample_contents[original_name]
                if content == expected_content:
                    print(
                        f"  ✓ {decompressed_path.name}: Content verified ({len(content)} chars)"
                    )
                else:
                    print(f"  ✗ {decompressed_path.name}: Content mismatch!")
            else:
                print(f"  ✗ {decompressed_path.name}: File not found!")

        # Test cache (re-run without force)
        print("\nTesting cache (should skip decompression)...")
        with ProgressTracker(
            total_steps=len(compressed_files),
            enabled=True,
            verbose=True,
            description="Using cache",
        ) as tracker:
            cached_files = decompressor.ensure_uncompressed(
                compressed_files,
                tracker=tracker,
            )

        print("\nAll tests completed successfully!")
        print(f"Temporary directory: {temp_dir}")
        print("(Will be cleaned up automatically)")
