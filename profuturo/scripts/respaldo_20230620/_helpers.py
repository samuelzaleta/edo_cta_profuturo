from typing import Callable, Dict, Any


def chunk(iterable: iter, size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def group_by(
    iterable: iter,
    key_selector: Callable[[Any], Any],
    element_selector: Callable[[Any], Any],
) -> Dict[Any, Any]:
    result = {}

    for item in iterable:
        key = key_selector(item)
        element = element_selector(item)

        if not result.get(key):
            result[key] = []

        result[key].append(element)

    return result
