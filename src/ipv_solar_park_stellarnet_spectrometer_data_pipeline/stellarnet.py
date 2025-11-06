from pathlib import Path
import numpy as np

SSM_COMMENT_CHAR = '"'


def load_spectrawiz_spectrum(path: str | Path) -> np.ndarray:
    """Read an stellarnet spectrawiz (`.SSM`) spectrum

    Args:
        path (str): Path to file. Typically an `.SSM` file.

    Raises:
        RuntimeError: Incorrect file format.

    Returns:
        np.ndarray: Array of `(wavelength, counts)`.
    """
    x = []
    y = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith(SSM_COMMENT_CHAR):
                continue

            parts = line.split()
            if len(parts) != 2:
                raise RuntimeError("invalid SSM format")

            wavelength = float(parts[0])
            counts = float(parts[1])
            x.append(wavelength)
            y.append(counts)

    return np.stack((x, y))
