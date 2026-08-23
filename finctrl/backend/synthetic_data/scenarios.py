from datetime import datetime, timedelta
import uuid
import random
from decimal import Decimal
from typing import Dict, Any, Tuple, List, Optional
from schemas.models import ERPRecord, RazorpayRecord, BankRecord, GroundTruthGroup

class ScenarioGenerator:
    def __init__(self, rng: random.Random, tzinfo=None):
        self.rng = rng
        self.tzinfo = tzinfo
        self.current_time = datetime(2023, 1, 1, 10, 0, 0, tzinfo=self.tzinfo)

    def _next_uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self.rng.getrandbits(128))

    def _next_timestamp(self) -> datetime:
        # Progress time randomly between 1 minute and 2 hours
        self.current_time += timedelta(minutes=self.rng.randint(1, 120))
        return self.current_time

    def _calculate_fee_tax(self, amount: int) -> Tuple[int, int, int]:
        # amount is in paise. E.g., 2% fee, 18% tax on fee
        fee_decimal = Decimal(amount) * Decimal('0.02')
        fee = int(fee_decimal)
        tax = int(Decimal(fee) * Decimal('0.18'))
        net_amount = amount - fee - tax
        return fee, tax, net_amount

    def _generate_clean_1_to_1(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        group_id = self._next_uuid()
        amount = self.rng.randint(10000, 100000) # 100 to 1000 INR

        erp_id = self._next_uuid()
        rzp_id = self._next_uuid()
        bank_id = self._next_uuid()

        receipt_no = f"RCPT_{self.rng.randint(10000, 99999)}"
        rzp_payment_id = f"pay_{self.rng.randint(100000, 999999)}"
        rzp_settlement_id = f"setl_{self.rng.randint(100000, 999999)}"

        timestamp = self._next_timestamp()

        fee, tax, net_amount = self._calculate_fee_tax(amount)

        erp = ERPRecord(
            id=erp_id,
            reference_id=receipt_no,
            amount=amount,
            timestamp=timestamp,
            type="sale",
            status="completed"
        )

        rzp = RazorpayRecord(
            id=rzp_id,
            rzp_payment_id=rzp_payment_id,
            rzp_settlement_id=rzp_settlement_id,
            order_receipt=receipt_no,
            gross_amount=amount,
            fee=fee,
            tax=tax,
            net_amount=net_amount,
            type="payment",
            timestamp=timestamp + timedelta(seconds=self.rng.randint(1, 60)),
            status="captured"
        )

        bank = BankRecord(
            id=bank_id,
            transaction_ref=rzp_settlement_id,
            description=f"Razorpay Settlement {rzp_settlement_id}",
            amount=net_amount,
            type="credit",
            timestamp=timestamp + timedelta(days=2), # Settlement T+2
            status="processed"
        )

        gt = GroundTruthGroup(
            group_id=group_id,
            scenario="CLEAN_1_TO_1",
            erp_record_ids=[erp_id],
            rzp_record_ids=[rzp_id],
            bank_record_ids=[bank_id],
            expected_outcome="MATCH"
        )

        return [erp], [rzp], [bank], gt

    def _generate_fee_discrepancy(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        group_id = self._next_uuid()
        amount = self.rng.randint(10000, 100000)

        erp_id = self._next_uuid()
        rzp_id = self._next_uuid()
        bank_id = self._next_uuid()

        receipt_no = f"RCPT_{self.rng.randint(10000, 99999)}"
        rzp_payment_id = f"pay_{self.rng.randint(100000, 999999)}"
        rzp_settlement_id = f"setl_{self.rng.randint(100000, 999999)}"

        timestamp = self._next_timestamp()

        fee, tax, net_amount = self._calculate_fee_tax(amount)

        # Discrepancy: bank receives slightly less or more than expected net_amount
        discrepancy = self.rng.randint(100, 500) # 1 to 5 INR
        bank_amount = net_amount - discrepancy if self.rng.choice([True, False]) else net_amount + discrepancy

        erp = ERPRecord(
            id=erp_id,
            reference_id=receipt_no,
            amount=amount,
            timestamp=timestamp,
            type="sale",
            status="completed"
        )

        rzp = RazorpayRecord(
            id=rzp_id,
            rzp_payment_id=rzp_payment_id,
            rzp_settlement_id=rzp_settlement_id,
            order_receipt=receipt_no,
            gross_amount=amount,
            fee=fee,
            tax=tax,
            net_amount=net_amount, # RZP expects this
            type="payment",
            timestamp=timestamp + timedelta(seconds=self.rng.randint(1, 60)),
            status="captured"
        )

        bank = BankRecord(
            id=bank_id,
            transaction_ref=rzp_settlement_id,
            description=f"Razorpay Settlement {rzp_settlement_id}",
            amount=bank_amount, # Actual banked is different
            type="credit",
            timestamp=timestamp + timedelta(days=2),
            status="processed"
        )

        gt = GroundTruthGroup(
            group_id=group_id,
            scenario="FEE_DISCREPANCY",
            erp_record_ids=[erp_id],
            rzp_record_ids=[rzp_id],
            bank_record_ids=[bank_id],
            expected_outcome="MISMATCH_FEE",
            metadata={"discrepancy": discrepancy}
        )

        return [erp], [rzp], [bank], gt

    def _generate_missing_record(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        group_id = self._next_uuid()
        amount = self.rng.randint(10000, 100000)

        receipt_no = f"RCPT_{self.rng.randint(10000, 99999)}"
        rzp_payment_id = f"pay_{self.rng.randint(100000, 999999)}"
        rzp_settlement_id = f"setl_{self.rng.randint(100000, 999999)}"

        timestamp = self._next_timestamp()

        fee, tax, net_amount = self._calculate_fee_tax(amount)

        missing_type = self.rng.choice(["ERP", "RZP", "BANK"])

        erps = []
        rzps = []
        banks = []

        erp_id = self._next_uuid()
        rzp_id = self._next_uuid()
        bank_id = self._next_uuid()

        if missing_type != "ERP":
            erps.append(ERPRecord(
                id=erp_id,
                reference_id=receipt_no,
                amount=amount,
                timestamp=timestamp,
                type="sale",
                status="completed"
            ))

        if missing_type != "RZP":
            rzps.append(RazorpayRecord(
                id=rzp_id,
                rzp_payment_id=rzp_payment_id,
                rzp_settlement_id=rzp_settlement_id,
                order_receipt=receipt_no,
                gross_amount=amount,
                fee=fee,
                tax=tax,
                net_amount=net_amount,
                type="payment",
                timestamp=timestamp + timedelta(seconds=self.rng.randint(1, 60)),
                status="captured"
            ))

        if missing_type != "BANK":
            banks.append(BankRecord(
                id=bank_id,
                transaction_ref=rzp_settlement_id,
                description=f"Razorpay Settlement {rzp_settlement_id}",
                amount=net_amount,
                type="credit",
                timestamp=timestamp + timedelta(days=2),
                status="processed"
            ))

        gt = GroundTruthGroup(
            group_id=group_id,
            scenario="MISSING_RECORD",
            erp_record_ids=[r.id for r in erps],
            rzp_record_ids=[r.id for r in rzps],
            bank_record_ids=[r.id for r in banks],
            expected_outcome="MISSING_DATA",
            metadata={"missing_source": missing_type}
        )

        return erps, rzps, banks, gt

    def _generate_consolidated_1_to_n(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        group_id = self._next_uuid()
        num_records = self.rng.randint(2, 5)

        erps = []
        rzps = []

        total_net_amount = 0
        rzp_settlement_id = f"setl_{self.rng.randint(100000, 999999)}"

        for _ in range(num_records):
            amount = self.rng.randint(5000, 50000)
            receipt_no = f"RCPT_{self.rng.randint(10000, 99999)}"
            rzp_payment_id = f"pay_{self.rng.randint(100000, 999999)}"

            timestamp = self._next_timestamp()
            fee, tax, net_amount = self._calculate_fee_tax(amount)
            total_net_amount += net_amount

            erp_id = self._next_uuid()
            rzp_id = self._next_uuid()

            erps.append(ERPRecord(
                id=erp_id,
                reference_id=receipt_no,
                amount=amount,
                timestamp=timestamp,
                type="sale",
                status="completed"
            ))

            rzps.append(RazorpayRecord(
                id=rzp_id,
                rzp_payment_id=rzp_payment_id,
                rzp_settlement_id=rzp_settlement_id,
                order_receipt=receipt_no,
                gross_amount=amount,
                fee=fee,
                tax=tax,
                net_amount=net_amount,
                type="payment",
                timestamp=timestamp + timedelta(seconds=self.rng.randint(1, 60)),
                status="captured"
            ))

        bank_id = self._next_uuid()
        bank = BankRecord(
            id=bank_id,
            transaction_ref=rzp_settlement_id,
            description=f"Razorpay Settlement {rzp_settlement_id}",
            amount=total_net_amount,
            type="credit",
            timestamp=erps[-1].timestamp + timedelta(days=2),
            status="processed"
        )

        gt = GroundTruthGroup(
            group_id=group_id,
            scenario="CONSOLIDATED_1_TO_N",
            erp_record_ids=[r.id for r in erps],
            rzp_record_ids=[r.id for r in rzps],
            bank_record_ids=[bank_id],
            expected_outcome="MATCH_CONSOLIDATED"
        )

        return erps, rzps, [bank], gt

    def _generate_timing_skew(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        erps, rzps, banks, gt = self._generate_clean_1_to_1()
        gt.scenario = "TIMING_SKEW"
        gt.expected_outcome = "MATCH_DELAYED"

        delay_days = self.rng.randint(5, 14)
        banks[0].timestamp += timedelta(days=delay_days)
        gt.metadata={"delay_days": delay_days}
        return erps, rzps, banks, gt

    def _generate_truncated_reference(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        erps, rzps, banks, gt = self._generate_clean_1_to_1()
        gt.scenario = "TRUNCATED_REFERENCE"
        gt.expected_outcome = "MATCH_PARTIAL_REF"

        original_ref = banks[0].transaction_ref
        truncated = original_ref[:-3] if len(original_ref) > 3 else original_ref[:3]
        banks[0].transaction_ref = truncated

        return erps, rzps, banks, gt

    def _generate_consolidated_refunds(self) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        group_id = self._next_uuid()
        num_refunds = self.rng.randint(2, 4)

        erps = []
        rzps = []

        total_refund_amount = 0
        rzp_refund_settlement_id = f"setl_rfnd_{self.rng.randint(100000, 999999)}"

        for _ in range(num_refunds):
            amount = self.rng.randint(5000, 20000)
            receipt_no = f"RFND_RCPT_{self.rng.randint(10000, 99999)}"
            rzp_refund_id = f"rfnd_{self.rng.randint(100000, 999999)}"

            timestamp = self._next_timestamp()

            total_refund_amount += amount

            erp_id = self._next_uuid()
            rzp_id = self._next_uuid()

            erps.append(ERPRecord(
                id=erp_id,
                reference_id=receipt_no,
                amount=amount, # Amount is positive but type is refund
                timestamp=timestamp,
                type="refund",
                status="completed"
            ))

            rzps.append(RazorpayRecord(
                id=rzp_id,
                rzp_payment_id=rzp_refund_id,
                rzp_settlement_id=rzp_refund_settlement_id,
                order_receipt=receipt_no,
                gross_amount=amount,
                fee=0,
                tax=0,
                net_amount=amount, # usually refunds don't have fee back or if they do it's complicated, let's keep it simple
                type="refund",
                timestamp=timestamp + timedelta(seconds=self.rng.randint(1, 60)),
                status="processed"
            ))

        bank_id = self._next_uuid()
        bank = BankRecord(
            id=bank_id,
            transaction_ref=rzp_refund_settlement_id,
            description=f"Razorpay Refund {rzp_refund_settlement_id}",
            amount=total_refund_amount,
            type="debit", # Bank sees a debit
            timestamp=erps[-1].timestamp + timedelta(days=1),
            status="processed"
        )

        gt = GroundTruthGroup(
            group_id=group_id,
            scenario="CONSOLIDATED_REFUNDS",
            erp_record_ids=[r.id for r in erps],
            rzp_record_ids=[r.id for r in rzps],
            bank_record_ids=[bank_id],
            expected_outcome="MATCH_REFUND"
        )

        return erps, rzps, [bank], gt

    def generate(self, scenario: str) -> Tuple[List[ERPRecord], List[RazorpayRecord], List[BankRecord], GroundTruthGroup]:
        mapping = {
            "CLEAN_1_TO_1": self._generate_clean_1_to_1,
            "FEE_DISCREPANCY": self._generate_fee_discrepancy,
            "MISSING_RECORD": self._generate_missing_record,
            "CONSOLIDATED_1_TO_N": self._generate_consolidated_1_to_n,
            "TIMING_SKEW": self._generate_timing_skew,
            "TRUNCATED_REFERENCE": self._generate_truncated_reference,
            "CONSOLIDATED_REFUNDS": self._generate_consolidated_refunds,
        }

        if scenario not in mapping:
            raise ValueError(f"Unknown scenario: {scenario}")

        return mapping[scenario]()
