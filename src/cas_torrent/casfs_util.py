# https://github.com/google/casfs
# casfs/util.py

from typing import Any, List, Optional, Union

def compact(items: List[Optional[Any]]) -> List[Any]:
  """Return only truthy elements of `items`."""
  return [item for item in items if item]

def shard(digest: str, depth: int, width: int) -> str:
  """This creates a list of `depth` number of tokens with width `width` from the
  first part of the id plus the remainder.

  TODO examine Clojure's Blocks to see if there's some nicer style here.

  """

  first = [digest[i * width:width * (i + 1)] for i in range(depth)]
  remaining = [digest[depth * width:]]
  return compact(first + remaining)
