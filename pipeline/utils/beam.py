"""General utilities for testing Apache Beam pipelines.

TODO: consume directly from gfw-common when merged and released.
"""
import sys

from difflib import ndiff
from io import StringIO
from itertools import zip_longest
from typing import Any, Callable, Tuple, List, Iterable

from apache_beam.testing.util import BeamAssertException
from rich.columns import Columns
from rich.console import Console, Group
from rich.panel import Panel
from rich.pretty import pretty_repr


def _diff_lines(a: str, b: str) -> Tuple[str, str, bool]:
    a_lines, b_lines = a.splitlines(), b.splitlines()
    a_out, b_out = [], []
    changed = False
    for line in ndiff(a_lines, b_lines):
        tag, content = line[0], line[2:]
        if tag == " ":
            a_out.append(f"  {content}")
            b_out.append(f"  {content}")
        elif tag == "-":
            changed = True
            a_out.append(f"[red]- {content}[/red]")
            b_out.append("")  # line not in b
        elif tag == "+":
            changed = True
            a_out.append("")  # line not in a
            b_out.append(f"[green]+ {content}[/green]")

    return "\n".join(a_out), "\n".join(b_out), changed


def _compare_items(a: Any, b: Any) -> Tuple[str, str, bool]:
    return _diff_lines(
        pretty_repr(a, indent_size=4, max_width=20),
        pretty_repr(b, indent_size=4, max_width=20),
    )


def _render_diff_panel(left: str, right: str, idx: int) -> Columns:
    return Columns(
        [
            Panel(left, title=f"Actual #{idx}", expand=True),
            Panel(right, title=f"Expected #{idx}", expand=True),
        ],
        expand=True,
        equal=True,
    )


def _default_equals_fn(e: Any, a: Any) -> bool:
    return e == a


def normalize(obj: Any) -> Any:
    """Recursively sorts dict keys and lists to get consistent ordering for comparison.

    If elements are not sortable, preserves their order.
    """
    if isinstance(obj, dict):
        return {k: normalize(obj[k]) for k in sorted(obj)}
    elif isinstance(obj, (list, tuple)):
        normalized_elements = [normalize(e) for e in obj]
        try:
            return sorted(normalized_elements)
        except TypeError:
            # Fallback to plain list or tuple, not the original type
            if isinstance(obj, tuple):
                return tuple(normalized_elements)
            else:
                return normalized_elements
    else:
        return obj


def equal_to(
    expected: List[Any], equals_fn: Callable[[Any, Any], bool] = _default_equals_fn
) -> Callable[[List[Any]], None]:
    """Drop-in replacement for `apache_beam.testing.util.equal_to` with rich diff output.

    This matcher performs unordered comparison of top-level elements in actual and expected
    PCollection outputs, just like Apache Beam's `equal_to`. However, it adds a rich diff
    visualization to help debug mismatches by rendering side-by-side differences.

    Use in tests with `assert_that(pcoll, equal_to(expected))`.

    Note:
        - Only top-level permutations are considered equal:
          `[1, 2]` and `[2, 1]` are equal, but `[[1, 2]]` and `[[2, 1]]` are not.
        - If elements are not directly comparable fallback comparison with a
          custom equality function or deep diff is used to handle:
          1) Collections with types that don't have a deterministic sort order
             (e.g. pyarrow Tables as of 0.14.1)
          2) Collections that have different types.

    Args:
        expected: Iterable of expected PCollection elements.
        equals_fn: Optional function `(expected_item, actual_item) -> bool` to customize equality.

    Returns:
        A matcher function for use with `apache_beam.testing.util.assert_that`.
    """

    def _matcher(actual: Iterable[Any]) -> None:
        expected_list = [normalize(e) for e in expected]
        actual_list = [normalize(e) for e in actual]

        try:
            if actual_list == expected_list:
                return
        except TypeError:
            pass

        # Slower method, fallback comparison.
        unmatched_expected = expected_list[:]
        unmatched_actual = []
        for a in actual_list:
            for i, e in enumerate(unmatched_expected):
                if equals_fn(e, a):
                    unmatched_expected.pop(i)
                    break
            else:
                unmatched_actual.append(a)

        if not unmatched_actual and not unmatched_expected:
            return

        diffs = []
        for i, (a, b) in enumerate(
            zip_longest(unmatched_actual, unmatched_expected, fillvalue={}), 1
        ):
            left, right, changed = _compare_items(a, b)
            if changed:
                diffs.append(_render_diff_panel(left, right, i))

        if diffs:  # Diffs found. Raise exception with colorized output.
            sys.tracebacklimit = 0  # Suppress traceback
            buf = StringIO()
            console = Console(file=buf, force_terminal=True, width=130)
            console.print(Group(*diffs))
            raise BeamAssertException(f"PCollection contents differ:\n{buf.getvalue()}")

    return _matcher
