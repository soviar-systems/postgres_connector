"""
Unit tests for postgres_connector module-level helpers.

Scope
─────
Pure logic only — no database connection required. These tests run anywhere
without any PostgreSQL instance available.

What belongs here
    _is_constraint()          — keyword detection from schemata tuple first tokens
    create_attributes_dict()  — schemata → {table: [col, ...]} transformation

What does NOT belong here
    Any test that calls .connect(), .create_tables(), .insert_into_table(), etc.
    Those require a live PostgreSQL instance and belong in test_integration.py
    (marked with @pytest.mark.integration).

Naming convention
    One test class per public function/method, named Test<FunctionName>.
    Each method tests one behaviour or edge case; the name describes the scenario.
"""

import pytest
from postgres_connector import PostgresConnector, _is_constraint


# ── _is_constraint ────────────────────────────────────────────────────────────

class TestIsConstraint:
    """
    _is_constraint(attr) returns True when the first token of `attr` (uppercased)
    is in _CONSTRAINT_KEYWORDS, False otherwise.

    It is used by create_attributes_dict() and create_tables() to separate column
    entries from constraint entries in a schemata tuple list.
    """
    def test_primary_key(self):
        assert _is_constraint("PRIMARY KEY") is True

    def test_unique(self):
        assert _is_constraint("UNIQUE") is True

    def test_foreign_key(self):
        assert _is_constraint("FOREIGN KEY") is True

    def test_check(self):
        assert _is_constraint("CHECK") is True

    def test_exclude(self):
        assert _is_constraint("EXCLUDE") is True

    def test_column_name_id(self):
        assert _is_constraint("id") is False

    def test_column_name_with_underscores(self):
        assert _is_constraint("resolution_id") is False

    def test_column_name_that_starts_like_keyword(self):
        # "primary_contact" starts with "primary" but is not a constraint keyword
        assert _is_constraint("primary_contact") is False

    def test_case_insensitive_lower(self):
        assert _is_constraint("primary key") is True

    def test_case_insensitive_mixed(self):
        assert _is_constraint("Primary Key") is True

    def test_unique_with_definition(self):
        # As it appears in schemata: first token is the keyword
        assert _is_constraint("UNIQUE") is True

    def test_foreign_key_multiword(self):
        assert _is_constraint("FOREIGN KEY") is True


# ── create_attributes_dict ────────────────────────────────────────────────────

class TestCreateAttributesDict:
    def test_excludes_primary_key_constraint(self):
        relations = {
            "title": [
                ("id",   "SERIAL"),
                ("name", "TEXT UNIQUE"),
                ("PRIMARY KEY", "(id)"),
            ],
        }
        result = PostgresConnector.create_attributes_dict(relations)
        assert result == {"title": ["id", "name"]}

    def test_excludes_unique_constraint(self):
        relations = {
            "vote": [
                ("resolution_id", "INTEGER"),
                ("country_id",    "INTEGER"),
                ("UNIQUE", "(resolution_id, country_id)"),
            ],
        }
        result = PostgresConnector.create_attributes_dict(relations)
        assert result == {"vote": ["resolution_id", "country_id"]}

    def test_multiple_tables(self):
        relations = {
            "a": [("id", "SERIAL"), ("PRIMARY KEY", "(id)")],
            "b": [("x", "TEXT"), ("y", "TEXT"), ("UNIQUE", "(x, y)")],
        }
        result = PostgresConnector.create_attributes_dict(relations)
        assert result == {"a": ["id"], "b": ["x", "y"]}

    def test_all_keys_present(self):
        relations = {
            "t1": [("col", "TEXT"), ("PRIMARY KEY", "(col)")],
            "t2": [("a", "INT"), ("b", "INT"), ("UNIQUE", "(a, b)")],
        }
        result = PostgresConnector.create_attributes_dict(relations)
        assert set(result.keys()) == {"t1", "t2"}

    def test_does_not_modify_original(self):
        relations = {
            "t": [("id", "SERIAL"), ("name", "TEXT"), ("PRIMARY KEY", "(id)")],
        }
        PostgresConnector.create_attributes_dict(relations)
        # Original must be unchanged
        assert len(relations["t"]) == 3

    def test_single_column_table(self):
        relations = {
            "flag": [("code", "CHAR(2) UNIQUE"), ("PRIMARY KEY", "(code)")],
        }
        result = PostgresConnector.create_attributes_dict(relations)
        assert result == {"flag": ["code"]}
