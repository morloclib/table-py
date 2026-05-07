"""Python implementations for the table stdlib module.

These wrap pyarrow.RecordBatch operations directly. The wire-format layer
is responsible for getting RecordBatch values across the cross-pool SHM
boundary via the Arrow C Data Interface (zero-copy).
"""

import pyarrow as pa
import pyarrow.compute as pc


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def morloc_asCol(name, vec):
    """Build a single-column RecordBatch carrying `vec` under column `name`."""
    return pa.RecordBatch.from_arrays([pa.array(list(vec))], names=[name])


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------

def morloc_nrow(t):
    return t.num_rows

def morloc_ncol(t):
    return t.num_columns

def morloc_names(t):
    return list(t.column_names)


# ---------------------------------------------------------------------------
# Row operations
# ---------------------------------------------------------------------------

def morloc_sliceRows(start, end, t):
    """Slice rows in the half-open range [start, end). Matches morloc's
    root Indexed.slice semantics: bounds clamped non-negative; end <= start
    returns empty; end > total clamps to total; start >= total returns
    empty."""
    if start < 0:
        start = 0
    if end < 0:
        end = 0
    total = t.num_rows
    if start >= total or end <= start:
        return t.slice(0, 0)
    end_clamped = min(end, total)
    return t.slice(start, end_clamped - start)


def morloc_filterRows(mask, t):
    """Boolean-mask row selection. The filter result is a Table; we
    collapse back to a single batch since morloc Tables are
    RecordBatches at the wire level."""
    pa_mask = pa.array(list(mask), type=pa.bool_())
    filtered = pc.filter(t, pa_mask)
    if filtered.num_rows == 0:
        # filter() returns a RecordBatch directly when input is one;
        # the empty-row case is fine.
        return filtered
    return filtered


def morloc_pickRows(indices, t):
    """Gather rows by integer indices. pyarrow Table.take supports
    arbitrary index orders and duplicates; we round-trip via Table to
    get a take(), then collapse back to a single batch."""
    idx = pa.array(list(indices), type=pa.int64())
    tbl = pa.Table.from_batches([t]).take(idx)
    if tbl.num_rows == 0:
        # Preserve the original schema on the empty result.
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=t.schema.field(i).type)
             for i in range(t.num_columns)],
            names=list(t.column_names),
        )
    return tbl.combine_chunks().to_batches()[0]


def morloc_distinctRows(t):
    """Deduplicate rows. pyarrow doesn't have a direct
    drop_duplicates on RecordBatch, but we can build a row hash and
    keep the first occurrence indices."""
    n = t.num_rows
    if n <= 1:
        return t
    # Build per-row tuples for hashing.
    cols = [t.column(i).to_pylist() for i in range(t.num_columns)]
    seen = set()
    keep = []
    for r in range(n):
        key = tuple(col[r] for col in cols)
        if key not in seen:
            seen.add(key)
            keep.append(r)
    return morloc_pickRows(keep, t)


def morloc_sortRows(spec, t):
    """Multi-key stable sort. `spec` is a list of (col_name, ascending)
    tuples where ascending=True means ascending, False descending.
    Empty spec is a no-op."""
    if not spec:
        return t
    keys = [(name, "ascending" if asc else "descending") for (name, asc) in spec]
    tbl = pa.Table.from_batches([t]).sort_by(keys)
    if tbl.num_rows == 0:
        return t
    return tbl.combine_chunks().to_batches()[0]


# ---------------------------------------------------------------------------
# Column operations
# ---------------------------------------------------------------------------

def morloc_getCol(name, t):
    """Extract a column as a list. The wire form is morloc Vector
    (= List); zero-copy fast path applies when the underlying numpy
    buffer is contiguous."""
    return t.column(name).to_pylist()


def morloc_setCol(name, vec, t):
    """Set or replace a column. If `name` exists in `t`, replace in
    place; otherwise append to the end."""
    array = pa.array(list(vec))
    cols = list(t.column_names)
    if name in cols:
        idx = cols.index(name)
        arrays = [array if i == idx else t.column(i) for i in range(t.num_columns)]
        return pa.RecordBatch.from_arrays(arrays, names=cols)
    arrays = [t.column(i) for i in range(t.num_columns)] + [array]
    return pa.RecordBatch.from_arrays(arrays, names=cols + [name])


def morloc_dropCols(name_list, t):
    """Drop several columns by name. Absent keys are silently ignored,
    matching the type-level (r - l) semantics."""
    drop_set = set(name_list)
    keep = [c for c in t.column_names if c not in drop_set]
    return t.select(keep)


def morloc_selectCols(name_list, t):
    """Project the table to the named columns, preserving the requested
    order. Trusts the typechecker: every name must exist in t."""
    return t.select(list(name_list))


def morloc_selectColsDyn(name_list, t):
    """Runtime [Str] projection escape hatch. Same kernel as selectCols
    but no type-level guarantee that names exist."""
    return t.select(list(name_list))


def morloc_renameCol(old_name, new_name, t):
    """Rename a single column. Rebuild the schema with one name
    swapped; the underlying arrays are reused (zero-copy)."""
    cols = list(t.column_names)
    new_cols = [new_name if c == old_name else c for c in cols]
    arrays = [t.column(i) for i in range(t.num_columns)]
    return pa.RecordBatch.from_arrays(arrays, names=new_cols)


# ---------------------------------------------------------------------------
# Concatenation
# ---------------------------------------------------------------------------

def morloc_rbind(t1, t2):
    """Concatenate two RecordBatches row-wise. Both must share a
    schema; column data is concatenated per column. The result has
    n1+n2 rows."""
    return pa.RecordBatch.from_arrays(
        [pa.concat_arrays([t1.column(i), t2.column(i)]) for i in range(t1.num_columns)],
        names=t1.column_names,
    )


def morloc_cbind(t1, t2):
    """Concatenate two RecordBatches column-wise. Both must share a
    row count; columns are appended. The result has the union of
    columns."""
    arrays = [t1.column(i) for i in range(t1.num_columns)] + \
             [t2.column(i) for i in range(t2.num_columns)]
    names = list(t1.column_names) + list(t2.column_names)
    return pa.RecordBatch.from_arrays(arrays, names=names)
