import pytest
import os
import json
from decimal import Decimal
from synthetic_data.generator import SyntheticDataEngine, Config
from schemas.models import FinctrlDataset, GroundTruthDataset

def test_generator_reproducibility():
    config1 = Config(seed=42, dataset_sizes={"TEST": 10})
    engine1 = SyntheticDataEngine(config1)
    datasets1 = engine1.generate_all()

    config2 = Config(seed=42, dataset_sizes={"TEST": 10})
    engine2 = SyntheticDataEngine(config2)
    datasets2 = engine2.generate_all()

    ds1, gt1 = datasets1["TEST"]
    ds2, gt2 = datasets2["TEST"]

    assert len(ds1.erp_records) == len(ds2.erp_records)
    assert ds1.erp_records[0].id == ds2.erp_records[0].id
    assert gt1.groups[0].scenario == gt2.groups[0].scenario

def test_required_fields_exist():
    config = Config(seed=42, dataset_sizes={"TEST": 5})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    assert len(ds.erp_records) > 0
    assert hasattr(ds.erp_records[0], "amount")
    assert hasattr(ds.rzp_records[0], "gross_amount")
    assert hasattr(ds.bank_records[0], "amount")
    assert hasattr(gt.groups[0], "scenario")
    assert hasattr(gt.groups[0], "expected_outcome")

def test_monetary_relationships():
    config = Config(seed=1, dataset_sizes={"TEST": 50})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    # Check that fee + tax + net = gross
    for rzp in ds.rzp_records:
        if rzp.type == "payment":
            assert rzp.fee + rzp.tax + rzp.net_amount == rzp.gross_amount

def test_scenario_distribution():
    config = Config(seed=42, dataset_sizes={"TEST": 1000})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    # Check that at least some of each are generated
    counts = ds.metadata.scenario_counts
    assert counts.get("CLEAN_1_TO_1", 0) > 600
    assert counts.get("FEE_DISCREPANCY", 0) > 10
    assert counts.get("MISSING_RECORD", 0) > 10
    assert counts.get("CONSOLIDATED_1_TO_N", 0) > 10
    assert counts.get("TIMING_SKEW", 0) > 10
    assert counts.get("TRUNCATED_REFERENCE", 0) > 10
    assert counts.get("CONSOLIDATED_REFUNDS", 0) > 10

def test_ground_truth_references():
    config = Config(seed=42, dataset_sizes={"TEST": 50})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    erp_ids = {r.id for r in ds.erp_records}
    rzp_ids = {r.id for r in ds.rzp_records}
    bank_ids = {r.id for r in ds.bank_records}

    for group in gt.groups:
        for eid in group.erp_record_ids:
            assert eid in erp_ids
        for rid in group.rzp_record_ids:
            assert rid in rzp_ids
        for bid in group.bank_record_ids:
            assert bid in bank_ids

def test_1_to_1_relationships():
    config = Config(seed=42, dataset_sizes={"TEST": 50})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    clean_groups = [g for g in gt.groups if g.scenario == "CLEAN_1_TO_1"]
    assert len(clean_groups) > 0

    for group in clean_groups:
        assert len(group.erp_record_ids) == 1
        assert len(group.rzp_record_ids) == 1
        assert len(group.bank_record_ids) == 1
        assert group.expected_outcome == "MATCH"

def test_1_to_n_relationships():
    config = Config(seed=42, dataset_sizes={"TEST": 50})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    con_groups = [g for g in gt.groups if g.scenario == "CONSOLIDATED_1_TO_N"]
    assert len(con_groups) > 0

    for group in con_groups:
        assert len(group.erp_record_ids) > 1
        assert len(group.rzp_record_ids) > 1
        assert len(group.bank_record_ids) == 1
        assert group.expected_outcome == "MATCH_CONSOLIDATED"

def test_missing_records():
    config = Config(seed=42, dataset_sizes={"TEST": 100})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()
    ds, gt = datasets["TEST"]

    missing_groups = [g for g in gt.groups if g.scenario == "MISSING_RECORD"]
    assert len(missing_groups) > 0

    for group in missing_groups:
        missing_source = group.metadata.get("missing_source")
        assert missing_source in ["ERP", "RZP", "BANK"]
        if missing_source == "ERP":
            assert len(group.erp_record_ids) == 0
        elif missing_source == "RZP":
            assert len(group.rzp_record_ids) == 0
        elif missing_source == "BANK":
            assert len(group.bank_record_ids) == 0

def test_held_out_independence():
    config = Config(seed=42, dataset_sizes={"DEV": 50, "HELD_OUT": 50})
    engine = SyntheticDataEngine(config)
    datasets = engine.generate_all()

    dev_ds, dev_gt = datasets["DEV"]
    ho_ds, ho_gt = datasets["HELD_OUT"]

    dev_erp_ids = {r.id for r in dev_ds.erp_records}
    ho_erp_ids = {r.id for r in ho_ds.erp_records}

    assert dev_erp_ids.isdisjoint(ho_erp_ids)

def test_regenerating_same_seed_same_result():
    config = Config(seed=99, dataset_sizes={"DEV": 50})
    engine1 = SyntheticDataEngine(config)
    datasets1 = engine1.generate_all()

    engine2 = SyntheticDataEngine(config)
    datasets2 = engine2.generate_all()

    ds1 = datasets1["DEV"][0]
    ds2 = datasets2["DEV"][0]

    # Metadata like generation_id will differ, but records should be exactly the same
    assert ds1.erp_records == ds2.erp_records
    assert ds1.rzp_records == ds2.rzp_records
    assert ds1.bank_records == ds2.bank_records
