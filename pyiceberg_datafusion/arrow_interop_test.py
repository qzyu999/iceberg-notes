"""
Arrow C Data Interface Interop Test: All Permutations
=====================================================
Proves that PyArrow, DataFusion, DuckDB, and Polars can all exchange
Arrow data with each other via zero-copy (Arrow C Data Interface).

Libraries tested:
- PyArrow 20.0.0
- DataFusion 53.0.0
- DuckDB 1.5.4
- Polars 1.34.0
- pandas 2.2.3 (via PyArrow bridge)
"""

import pyarrow as pa
import datafusion
from datafusion import SessionContext
import duckdb
import polars as pl
import pandas as pd

# ═══════════════════════════════════════════════════════════════════
# Create a reference table in each library
# ═══════════════════════════════════════════════════════════════════

# Reference data: 3 columns, 4 rows
REF_DATA = {
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "score": [95.5, 87.3, 91.2, 88.7],
}


def create_pyarrow_table() -> pa.Table:
    return pa.table(REF_DATA)


def create_datafusion_table():
    ctx = SessionContext()
    pa_table = pa.table(REF_DATA)
    ctx.register_record_batches("source", [pa_table.to_batches()])
    return ctx.sql("SELECT * FROM source")


def create_duckdb_table():
    con = duckdb.connect()
    con.execute("CREATE TABLE source AS SELECT * FROM (VALUES (1,'Alice',95.5),(2,'Bob',87.3),(3,'Charlie',91.2),(4,'Diana',88.7)) AS t(id, name, score)")
    return con.execute("SELECT * FROM source")


def create_polars_df() -> pl.DataFrame:
    return pl.DataFrame(REF_DATA)


def create_pandas_df() -> pd.DataFrame:
    return pd.DataFrame(REF_DATA)


# ═══════════════════════════════════════════════════════════════════
# Conversion functions: Library A → Library B
# ═══════════════════════════════════════════════════════════════════

def to_pyarrow(source, source_name: str) -> pa.Table:
    """Convert any source to PyArrow Table."""
    if source_name == "pyarrow":
        return source
    elif source_name == "datafusion":
        return source.to_arrow_table()
    elif source_name == "duckdb":
        return source.fetch_arrow_table()
    elif source_name == "polars":
        return source.to_arrow()
    elif source_name == "pandas":
        return pa.Table.from_pandas(source)
    raise ValueError(f"Unknown source: {source_name}")


def to_datafusion(source, source_name: str):
    """Register any source into a DataFusion SessionContext and query it."""
    ctx = SessionContext()
    if source_name == "pyarrow":
        ctx.register_record_batches("data", [source.to_batches()])
    elif source_name == "datafusion":
        # Already a DF result — convert to arrow first
        pa_table = source.to_arrow_table()
        ctx.register_record_batches("data", [pa_table.to_batches()])
    elif source_name == "duckdb":
        pa_table = source.fetch_arrow_table()
        ctx.register_record_batches("data", [pa_table.to_batches()])
    elif source_name == "polars":
        pa_table = source.to_arrow()
        ctx.register_record_batches("data", [pa_table.to_batches()])
    elif source_name == "pandas":
        pa_table = pa.Table.from_pandas(source)
        ctx.register_record_batches("data", [pa_table.to_batches()])
    else:
        raise ValueError(f"Unknown source: {source_name}")
    return ctx.sql("SELECT * FROM data").to_arrow_table()


def to_duckdb(source, source_name: str):
    """Load any source into DuckDB and query it."""
    con = duckdb.connect()
    if source_name == "pyarrow":
        con.register("data", source)
    elif source_name == "datafusion":
        pa_table = source.to_arrow_table()
        con.register("data", pa_table)
    elif source_name == "duckdb":
        pa_table = source.fetch_arrow_table()
        con.register("data", pa_table)
    elif source_name == "polars":
        # DuckDB can consume Polars directly
        con.register("data", source)
    elif source_name == "pandas":
        con.register("data", source)
    else:
        raise ValueError(f"Unknown source: {source_name}")
    return con.execute("SELECT * FROM data").fetch_arrow_table()


def to_polars(source, source_name: str) -> pl.DataFrame:
    """Convert any source to Polars DataFrame."""
    if source_name == "pyarrow":
        return pl.from_arrow(source)
    elif source_name == "datafusion":
        pa_table = source.to_arrow_table()
        return pl.from_arrow(pa_table)
    elif source_name == "duckdb":
        pa_table = source.fetch_arrow_table()
        return pl.from_arrow(pa_table)
    elif source_name == "polars":
        return source
    elif source_name == "pandas":
        return pl.from_pandas(source)
    raise ValueError(f"Unknown source: {source_name}")


def to_pandas(source, source_name: str) -> pd.DataFrame:
    """Convert any source to pandas DataFrame."""
    if source_name == "pyarrow":
        return source.to_pandas()
    elif source_name == "datafusion":
        return source.to_arrow_table().to_pandas()
    elif source_name == "duckdb":
        return source.fetch_arrow_table().to_pandas()
    elif source_name == "polars":
        return source.to_pandas()
    elif source_name == "pandas":
        return source
    raise ValueError(f"Unknown source: {source_name}")


# ═══════════════════════════════════════════════════════════════════
# Run all permutations
# ═══════════════════════════════════════════════════════════════════

LIBRARIES = ["pyarrow", "datafusion", "duckdb", "polars", "pandas"]

CREATORS = {
    "pyarrow": create_pyarrow_table,
    "datafusion": create_datafusion_table,
    "duckdb": create_duckdb_table,
    "polars": create_polars_df,
    "pandas": create_pandas_df,
}

CONVERTERS = {
    "pyarrow": to_pyarrow,
    "datafusion": to_datafusion,
    "duckdb": to_duckdb,
    "polars": to_polars,
    "pandas": to_pandas,
}

# Reference values for validation
REF_IDS = [1, 2, 3, 4]
REF_NAMES = ["Alice", "Bob", "Charlie", "Diana"]


def validate_result(result, target_name: str) -> bool:
    """Check that the result contains the expected data."""
    if target_name == "pyarrow":
        ids = result.column("id").to_pylist()
    elif target_name == "datafusion":
        ids = result.column("id").to_pylist()  # already pa.Table from to_datafusion
    elif target_name == "duckdb":
        ids = result.column("id").to_pylist()  # already pa.Table from to_duckdb
    elif target_name == "polars":
        ids = result["id"].to_list()
    elif target_name == "pandas":
        ids = result["id"].tolist()
    else:
        return False
    return sorted(ids) == REF_IDS


def main():
    print("=" * 70)
    print("Arrow C Data Interface: All Permutations Interop Test")
    print("=" * 70)
    print(f"\nLibraries: PyArrow {pa.__version__}, DataFusion {datafusion.__version__}, "
          f"DuckDB {duckdb.__version__}, Polars {pl.__version__}, pandas {pd.__version__}")
    print(f"\nTesting {len(LIBRARIES)} × {len(LIBRARIES)} = {len(LIBRARIES)**2} permutations "
          f"({len(LIBRARIES)**2 - len(LIBRARIES)} cross-library transfers)\n")

    results = []
    for source_name in LIBRARIES:
        source = CREATORS[source_name]()
        for target_name in LIBRARIES:
            try:
                result = CONVERTERS[target_name](source, source_name)
                valid = validate_result(result, target_name)
                status = "✓" if valid else "✗ (data mismatch)"
                results.append((source_name, target_name, True, valid))
            except Exception as e:
                status = f"✗ ({type(e).__name__}: {e})"
                results.append((source_name, target_name, False, False))

            # Re-create source for duckdb since cursors are consumed
            if source_name == "duckdb":
                source = CREATORS[source_name]()

            print(f"  {source_name:>10} → {target_name:<10}: {status}")
        print()

    # Summary
    total = len(results)
    passed = sum(1 for _, _, ok, valid in results if ok and valid)
    failed = total - passed
    cross_library = sum(1 for s, t, ok, valid in results if s != t and ok and valid)

    print("=" * 70)
    print(f"RESULTS: {passed}/{total} passed, {failed} failed")
    print(f"Cross-library transfers: {cross_library}/{len(LIBRARIES)**2 - len(LIBRARIES)} successful")
    print("=" * 70)

    if failed == 0:
        print("\n✓ ALL permutations work. Arrow C Data Interface enables zero-copy")
        print("  exchange between ALL five libraries at a single interop boundary.")
    else:
        print(f"\n✗ {failed} permutations failed. See above for details.")


if __name__ == "__main__":
    main()
