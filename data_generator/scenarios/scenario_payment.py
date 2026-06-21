"""
scenario_payment.py — Re-exports PaymentScenario from scenario_collections.
PaymentScenario is defined alongside CollectionScenario since they share logic.
"""
from scenarios.scenario_collections import PaymentScenario

__all__ = ["PaymentScenario"]
