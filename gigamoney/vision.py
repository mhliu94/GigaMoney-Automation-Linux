from __future__ import annotations

import csv
import io
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from PIL import Image

from .config import Settings
from .errors import ConfigurationError, UiError
from .ui import Rect


@dataclass(frozen=True)
class OcrWord:
    text: str
    box: Rect
    confidence: float = -1

    @property
    def center(self) -> tuple[int, int]:
        return self.box.center


@dataclass(frozen=True)
class OcrResult:
    text: str
    words: tuple[OcrWord, ...]


@dataclass(frozen=True)
class RegionStats:
    mean: float
    dark_fraction: float
    bright_fraction: float


@dataclass(frozen=True)
class ScreenProfile:
    center: RegionStats
    outer: RegionStats
    lower: RegionStats
    footer: RegionStats


def parse_tsv(tsv: str, *, x_offset: int = 0, y_offset: int = 0, scale: float = 1) -> OcrResult:
    words: list[OcrWord] = []
    for row in csv.DictReader(io.StringIO(tsv), delimiter="\t"):
        text = (row.get("text") or "").strip()
        if not text:
            continue
        try:
            confidence = float(row.get("conf", "-1"))
            left = round(float(row["left"]) / scale) + x_offset
            top = round(float(row["top"]) / scale) + y_offset
            width = round(float(row["width"]) / scale)
            height = round(float(row["height"]) / scale)
        except (KeyError, TypeError, ValueError):
            continue
        words.append(OcrWord(text, Rect(left, top, left + width, top + height), confidence))
    return OcrResult(" ".join(word.text for word in words), tuple(words))


class OcrEngine:
    def __init__(self, settings: Settings):
        command = str(settings.get("ocr.command", "tesseract"))
        self.command = shutil.which(command) if "/" not in command else str(settings.resolve_path(command))
        if not self.command or not Path(self.command).is_file():
            raise ConfigurationError("Tesseract is required. Install tesseract-ocr or set ocr.command.")
        self.language = str(settings.get("ocr.language", "eng"))

    def image(self, image: Image.Image, *, x_offset: int = 0, y_offset: int = 0, scale: float = 1) -> OcrResult:
        with tempfile.NamedTemporaryFile(suffix=".png") as handle:
            image.save(handle.name)
            try:
                result = subprocess.run(
                    [self.command, handle.name, "stdout", "-l", self.language, "tsv"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise UiError(f"OCR failed: {exc}") from exc
        if result.returncode != 0:
            raise UiError(f"OCR failed ({result.returncode}): {result.stderr.strip()}")
        return parse_tsv(result.stdout, x_offset=x_offset, y_offset=y_offset, scale=scale)

    def png(self, data: bytes) -> OcrResult:
        return self.image(Image.open(io.BytesIO(data)).convert("RGB"))

def screen_profile(image: Image.Image) -> ScreenProfile:
    source = image.convert("RGB")
    step = max(6, min(source.width, source.height) // 100)
    counts = {name: [0, 0, 0, 0] for name in ("center", "lower", "footer", "outer")}
    for y in range(step // 2, source.height, step):
        ny = y / source.height
        for x in range(step // 2, source.width, step):
            nx = x / source.width
            if 0.22 <= nx <= 0.78 and 0.22 <= ny <= 0.70:
                region = "center"
            elif 0.04 <= nx <= 0.96 and 0.76 <= ny <= 0.90:
                region = "lower"
            elif 0.04 <= nx <= 0.96 and 0.90 < ny <= 0.965:
                region = "footer"
            elif (
                0.04 <= nx <= 0.96 and 0.06 <= ny < 0.18
            ) or (
                (0.04 <= nx < 0.18 or 0.82 < nx <= 0.96) and 0.18 <= ny < 0.76
            ):
                region = "outer"
            else:
                continue
            red, green, blue = source.getpixel((x, y))
            luma = (54 * red + 183 * green + 19 * blue) // 256
            maximum = max(red, green, blue)
            dark = luma <= 90 and maximum <= 125
            bright = luma >= 140 or maximum >= 180
            counts[region][0] += 1
            counts[region][1] += int(dark)
            counts[region][2] += int(bright)
            counts[region][3] += luma

    def stats(region: str) -> RegionStats:
        count, dark, bright, total = counts[region]
        return RegionStats(total / max(1, count), dark / max(1, count), bright / max(1, count))

    return ScreenProfile(
        stats("center"), stats("outer"), stats("lower"), stats("footer")
    )


def classify_profile(profile: ScreenProfile) -> str:
    contrast = profile.center.mean - profile.outer.mean
    central = (
        contrast >= 14
        and profile.center.bright_fraction >= 0.08
        and profile.center.bright_fraction >= profile.outer.bright_fraction + 0.04
    )
    strong_center = (
        profile.outer.mean <= 120
        and profile.outer.bright_fraction <= 0.14
        and contrast >= 22
        and profile.center.bright_fraction >= 0.14
        and profile.center.bright_fraction >= profile.outer.bright_fraction + 0.06
    )
    bottom_lit = (
        profile.lower.mean >= 108
        or profile.footer.mean >= 108
        or profile.lower.bright_fraction >= 0.16
        or profile.footer.bright_fraction >= 0.16
    )
    bottom_dark = (
        profile.lower.mean <= 92
        and profile.footer.mean <= 92
        and profile.lower.dark_fraction >= 0.58
        and profile.footer.dark_fraction >= 0.58
        and profile.lower.bright_fraction <= 0.10
        and profile.footer.bright_fraction <= 0.10
        and profile.lower.mean <= profile.outer.mean + 25
        and profile.footer.mean <= profile.outer.mean + 25
        and profile.center.mean >= max(profile.lower.mean, profile.footer.mean) + 20
    )
    if central and bottom_lit:
        return "ConfirmationLike"
    if strong_center and bottom_dark:
        return "Ad"
    if central:
        return "Ambiguous"
    return "None"


def classify_png(data: bytes) -> str:
    return classify_profile(screen_profile(Image.open(io.BytesIO(data))))


def normalized_text(text: str) -> str:
    return " ".join(text.replace("’", "'").replace("‘", "'").split()).casefold()


def contains_all(text: str, values: Iterable[str]) -> bool:
    normalized = normalized_text(text)
    return all(value.casefold() in normalized for value in values)
