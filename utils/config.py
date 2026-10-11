"""Configuração do pipeline: carrega utils/config.yaml e resolve os caminhos das camadas."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "utils" / "config.yaml"


@dataclass(frozen=True)
class PipelineConfig:
    """Caminhos resolvidos e opções de leitura de cada camada."""

    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    raw_files: dict[str, str]
    csv_options: dict[str, Any]

    def raw_file(self, name: str) -> str:
        return str(self.raw_dir / self.raw_files[name])


def _resolve(path: str | Path) -> Path:
    path = Path(path)
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH, base_path: str | None = None) -> PipelineConfig:
    """Carrega o YAML; `base_path` sobrescreve o valor do arquivo."""
    with open(_resolve(config_path), encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    base = _resolve(base_path or raw["base_path"])
    layers = raw["layers"]
    return PipelineConfig(
        raw_dir=base / layers["raw"],
        bronze_dir=base / layers["bronze"],
        silver_dir=base / layers["silver"],
        gold_dir=base / layers["gold"],
        raw_files=dict(raw["raw_files"]),
        csv_options=dict(raw["csv_options"]),
    )


def parse_args(description: str) -> PipelineConfig:
    """CLI padrão dos scripts do pipeline: --config e --base-path."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Path to the YAML config.")
    parser.add_argument("--base-path", default=None, help="Override the base data directory from the config.")
    args = parser.parse_args()
    return load_config(args.config, args.base_path)
