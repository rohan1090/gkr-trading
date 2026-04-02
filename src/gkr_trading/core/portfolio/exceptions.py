from __future__ import annotations


class StrictReplayError(Exception):
    """Strict replay or apply mode refused to process a semantically invalid event."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
