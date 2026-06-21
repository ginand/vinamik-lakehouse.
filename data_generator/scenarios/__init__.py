"""Scenario modules for VinaMilk ERP data generation."""
from scenarios.scenario_revenue import RevenueScenario
from scenarios.scenario_collections import CollectionScenario, PaymentScenario
from scenarios.scenario_procurement import ProcurementScenario
from scenarios.scenario_payroll_depreciation import PayrollDepreciationScenario

__all__ = [
    "RevenueScenario",
    "CollectionScenario",
    "PaymentScenario",
    "ProcurementScenario",
    "PayrollDepreciationScenario",
]
