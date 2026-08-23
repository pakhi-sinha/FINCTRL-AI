import random
import uuid
import json
import os
from typing import Dict, List
from datetime import datetime, timezone
from schemas.models import FinctrlDataset, GroundTruthDataset, DatasetMetadata, ERPRecord, RazorpayRecord, BankRecord, GroundTruthGroup
from synthetic_data.scenarios import ScenarioGenerator
from collections import defaultdict

class Config:
    def __init__(self, seed: int = 42, dataset_sizes: Dict[str, int] = None, scenario_distribution: Dict[str, float] = None):
        self.seed = seed
        self.dataset_sizes = dataset_sizes or {
            "DEV": 200,
            "VALIDATION": 100,
            "HELD_OUT": 100
        }
        self.scenario_distribution = scenario_distribution or {
            "CLEAN_1_TO_1": 0.75,
            "FEE_DISCREPANCY": 0.05,
            "MISSING_RECORD": 0.04,
            "CONSOLIDATED_1_TO_N": 0.05,
            "TIMING_SKEW": 0.04,
            "TRUNCATED_REFERENCE": 0.04,
            "CONSOLIDATED_REFUNDS": 0.03
        }

class SyntheticDataEngine:
    def __init__(self, config: Config):
        self.config = config
        self.rng = random.Random(config.seed)
        self.scenario_generator = ScenarioGenerator(self.rng, tzinfo=timezone.utc)

        # Deterministic generation_id based on seed and config properties
        namespace = uuid.NAMESPACE_OID
        config_str = f"1.0.0-{config.seed}-{json.dumps(config.scenario_distribution, sort_keys=True)}"
        self.generation_id = str(uuid.uuid5(namespace, config_str))

    def _generate_dataset(self, name: str, size: int) -> tuple[FinctrlDataset, GroundTruthDataset]:
        # Refined generation_id strictly per dataset to ensure uniqueness across splits but determinism within
        namespace = uuid.NAMESPACE_OID
        dataset_config_str = f"1.0.0-{name}-{size}-{self.config.seed}-{json.dumps(self.config.scenario_distribution, sort_keys=True)}"
        dataset_generation_id = str(uuid.uuid5(namespace, dataset_config_str))
        erp_records = []
        rzp_records = []
        bank_records = []
        gt_groups = []

        scenarios = list(self.config.scenario_distribution.keys())
        weights = list(self.config.scenario_distribution.values())

        scenario_counts = defaultdict(int)

        for _ in range(size):
            scenario = self.rng.choices(scenarios, weights=weights)[0]
            scenario_counts[scenario] += 1

            erps, rzps, banks, gt = self.scenario_generator.generate(scenario)

            erp_records.extend(erps)
            rzp_records.extend(rzps)
            bank_records.extend(banks)
            gt_groups.append(gt)

        metadata = DatasetMetadata(
            dataset_name=name,
            generator_version="1.0.0",
            random_seed=self.config.seed,
            generation_id=dataset_generation_id,
            record_counts={
                "erp": len(erp_records),
                "rzp": len(rzp_records),
                "bank": len(bank_records),
                "groups": len(gt_groups)
            },
            scenario_counts=dict(scenario_counts)
        )

        dataset = FinctrlDataset(
            metadata=metadata,
            erp_records=erp_records,
            rzp_records=rzp_records,
            bank_records=bank_records
        )

        gt_dataset = GroundTruthDataset(
            metadata=metadata,
            groups=gt_groups
        )

        return dataset, gt_dataset

    def generate_all(self) -> Dict[str, tuple[FinctrlDataset, GroundTruthDataset]]:
        datasets = {}
        for name, size in self.config.dataset_sizes.items():
            datasets[name] = self._generate_dataset(name, size)
        return datasets

def save_datasets(datasets: Dict[str, tuple[FinctrlDataset, GroundTruthDataset]], output_dir: str):
    def custom_encoder(obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    for name, (dataset, gt_dataset) in datasets.items():
        base_dir = os.path.join(output_dir, name.lower())
        os.makedirs(base_dir, exist_ok=True)

        with open(os.path.join(base_dir, "dataset.json"), "w") as f:
            json.dump(dataset.model_dump(mode="json"), f, indent=2)

        with open(os.path.join(base_dir, "ground_truth.json"), "w") as f:
            json.dump(gt_dataset.model_dump(mode="json"), f, indent=2)

if __name__ == "__main__":
    config = Config(seed=42)
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    save_datasets(datasets, os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
