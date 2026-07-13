from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    project_root: Path | None = None

    def __post_init__(self) -> None:
        if self.project_root is None:
            env_root = os.getenv("QUANT_AGENT_PROJECT_ROOT")
            root = Path(env_root).expanduser().resolve() if env_root else Path(__file__).resolve().parents[2]
            object.__setattr__(self, "project_root", root)

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_data_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def docs_dir(self) -> Path:
        return self.data_dir / "docs"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def db_path(self) -> Path:
        return self.processed_dir / "quant_agent.db"

    @property
    def bm25_index_path(self) -> Path:
        return self.processed_dir / "bm25_index.pkl"

    @property
    def vector_index_dir(self) -> Path:
        return self.processed_dir / "vector_index"

    @property
    def vector_index_path(self) -> Path:
        return self.vector_index_dir / "vector_index.pkl"

    def ensure_processed_dirs(self) -> None:
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.vector_index_dir.mkdir(parents=True, exist_ok=True)
