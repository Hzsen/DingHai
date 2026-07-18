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
            if env_root:
                root = Path(env_root).expanduser().resolve()
            else:
                root = next(
                    (
                        candidate
                        for candidate in (Path.cwd().resolve(), *Path.cwd().resolve().parents)
                        if (candidate / "pyproject.toml").is_file()
                        and (candidate / "src" / "quant_agent").is_dir()
                    ),
                    Path(__file__).resolve().parents[2],
                )
            object.__setattr__(self, "project_root", root)

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_data_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def labels_dir(self) -> Path:
        return self.data_dir / "labels"

    @property
    def narratives_dir(self) -> Path:
        return self.data_dir / "narratives"

    @property
    def features_dir(self) -> Path:
        return self.data_dir / "features"

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
    def knowledge_db_path(self) -> Path:
        value = os.getenv("KNOWLEDGE_DB_PATH")
        path = Path(value).expanduser() if value else self.processed_dir / "phase1_research.db"
        return path.resolve() if path.is_absolute() else (self.project_root / path).resolve()

    @property
    def bm25_index_path(self) -> Path:
        return self.processed_dir / "bm25_index.pkl"

    @property
    def vector_index_dir(self) -> Path:
        return self.processed_dir / "vector_index"

    @property
    def vector_index_path(self) -> Path:
        return self.vector_index_dir / "vector_index.pkl"

    @property
    def baselines_dir(self) -> Path:
        return self.project_root / "baselines"

    @property
    def leader_cases_path(self) -> Path:
        return self.labels_dir / "leader_cases.csv"

    @property
    def theme_events_path(self) -> Path:
        return self.narratives_dir / "theme_events.csv"

    @property
    def phase0_market_prices_path(self) -> Path:
        return self.raw_data_dir / "phase0_market_prices.parquet"

    @property
    def phase0_benchmark_path(self) -> Path:
        return self.raw_data_dir / "phase0_csi300.parquet"

    @property
    def phase0_features_path(self) -> Path:
        return self.features_dir / "daily_features.parquet"

    @property
    def phase0_baseline_path(self) -> Path:
        return self.baselines_dir / "phase0_leader_score.csv"

    def ensure_processed_dirs(self) -> None:
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.vector_index_dir.mkdir(parents=True, exist_ok=True)

    def ensure_phase0_dirs(self) -> None:
        for path in (
            self.raw_data_dir,
            self.labels_dir,
            self.narratives_dir,
            self.features_dir,
            self.baselines_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
