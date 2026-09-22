"""
Unit tests for fixtures data counts and validation.
Verifies compliance with assignment thresholds:
- >= 20 restaurants
- >= 30 customers
- >= 40 menu items
"""

import sys
import os

# Add parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fixtures import RESTAURANTS, CUSTOMERS, MENU_ITEMS, get_fixture_counts, validate_fixtures


def test_fixture_counts():
    """Asserts that fixtures meet and exceed all minimum count requirements."""
    counts = get_fixture_counts()
    
    assert counts["restaurants"] >= 20, f"Expected >= 20 restaurants, got {counts['restaurants']}"
    assert counts["customers"] >= 30, f"Expected >= 30 customers, got {counts['customers']}"
    assert counts["menu_items"] >= 40, f"Expected >= 40 menu items, got {counts['menu_items']}"
    assert validate_fixtures() is True


def test_fixture_uniqueness():
    """Asserts that fixture items are unique strings without duplicates."""
    assert len(RESTAURANTS) == len(set(RESTAURANTS)), "Duplicate restaurant names found"
    assert len(CUSTOMERS) == len(set(CUSTOMERS)), "Duplicate customer names found"
    assert len(MENU_ITEMS) == len(set(MENU_ITEMS)), "Duplicate menu items found"
