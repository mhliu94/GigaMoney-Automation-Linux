from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Callable, Iterable


@dataclass(frozen=True)
class Rect:
    left: int
    top: int
    right: int
    bottom: int

    @property
    def center(self) -> tuple[int, int]:
        return ((self.left + self.right) // 2, (self.top + self.bottom) // 2)

    @property
    def width(self) -> int:
        return self.right - self.left

    @property
    def height(self) -> int:
        return self.bottom - self.top


_BOUNDS = re.compile(r"\[(\d+),(\d+)\]\[(\d+),(\d+)\]")


def parse_bounds(value: str) -> Rect:
    match = _BOUNDS.fullmatch(value or "")
    if not match:
        raise ValueError(f"Invalid UI bounds: {value!r}")
    return Rect(*(int(part) for part in match.groups()))


def parse_xml(data: str | bytes) -> ET.Element:
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="replace")
    return ET.fromstring(data)


def nodes(root: ET.Element) -> Iterable[ET.Element]:
    return root.iter("node")


def resource_name(node: ET.Element) -> str:
    value = node.attrib.get("resource-id", "")
    return value.rsplit("/", 1)[-1]


def node_text(node: ET.Element) -> str:
    return (node.attrib.get("text") or node.attrib.get("content-desc") or "").strip()


def bounds(node: ET.Element) -> Rect:
    return parse_bounds(node.attrib.get("bounds", ""))


def find_by_resource(root: ET.Element, name: str) -> ET.Element | None:
    return next((node for node in nodes(root) if resource_name(node) == name), None)


def find_all_by_resource(root: ET.Element, name: str) -> list[ET.Element]:
    return [node for node in nodes(root) if resource_name(node) == name]


def find_by_text(
    root: ET.Element,
    text: str,
    *,
    exact: bool = True,
    prefer_bottom: bool = False,
    predicate: Callable[[ET.Element], bool] | None = None,
) -> ET.Element | None:
    target = text.casefold()
    matches: list[ET.Element] = []
    for node in nodes(root):
        value = node_text(node).casefold()
        okay = value == target if exact else target in value
        if okay and (predicate is None or predicate(node)):
            matches.append(node)
    if not matches:
        return None
    if prefer_bottom:
        return max(matches, key=lambda candidate: bounds(candidate).center[1])
    return matches[0]


def descendants_by_resource(node: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in node.iter("node") if resource_name(child) == name]


def child_text(node: ET.Element, name: str, default: str = "") -> str:
    children = descendants_by_resource(node, name)
    return node_text(children[0]) if children else default


def text_signature(root: ET.Element) -> str:
    values = sorted(value for value in (node_text(node) for node in nodes(root)) if value)
    return "|".join(values)
