"""Utility script to download and extract the iPinYou dataset via kagglehub."""

from pathlib import Path

from dotenv import load_dotenv
from kagglehub import dataset_download

# Dataset identifier on Kaggle.
DATASET_SLUG = "lastsummer/ipinyou"
# Name of the zip artifact produced by Kaggle; kept for reference/logging.
ARCHIVE_NAME = Path("ipinyou.contest.dataset.zip")

load_dotenv()


def load_dataset() -> Path:
    """Download the iPinYou dataset archive via KaggleHub.

    Returns
    -------
    Path
        Filesystem path pointing to the downloaded archive within the
        KaggleHub cache directory.
    """
    # kagglehub downloads into its cache and returns the zip file path.
    downloaded_path = Path(dataset_download(DATASET_SLUG))
    return downloaded_path


if __name__ == "__main__":
    dataset_path = load_dataset()
    print(f"Dataset loaded to {dataset_path}")
