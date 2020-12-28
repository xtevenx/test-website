import enum
import json
import os
import typing

from .queue_management import SimpleQueue, Status

_DB_DIRECTORY: str = "db/"


@enum.unique
class Result(enum.Enum):
    DATABASE_CORRUPT = enum.auto()
    DATABASE_ERROR = enum.auto()
    KEY_COLLISION = enum.auto()
    KEY_NOT_FOUND = enum.auto()
    SUCCESS = enum.auto()


# define private components ===========================================

_DB_QUEUE: SimpleQueue = SimpleQueue()


def _decorate_queue(func: typing.Callable) -> typing.Callable:
    # Decorator to ensure only one instance of the decorated function is
    # running at one time.

    def foo(*args, **kwargs) -> typing.Any:
        job = _DB_QUEUE.add_job(lambda: func(*args, **kwargs))
        while job.status != Status.COMPLETE:
            ...
        return job.output

    return foo


def _ensure_exists(fn: str) -> Result:
    """Ensure that a file exists.

    Ensures that a file exists and is in valid JSON format.  If it does not
    exist, a file is created containing an empty JSON object.

    Args:
      - fn: A string describing the location of the file.

    Returns:
      - Result.DATABASE_CORRUPT if the file exists but cannot be parsed by the
          JSON decoder.
      - Result.DATABASE_ERROR if the location of the file cannot be created.
      - Result.DATABASE_SUCCESS if the file exists and complies with the JSON
          format.
    """

    dirname = os.path.dirname(fn)

    try:
        os.makedirs(dirname, exist_ok=True)
    except FileExistsError:
        return Result.DATABASE_ERROR

    try:
        # This is quite inefficient if we read the file again after this
        # function is run. Perhaps we can implement this in a way that the file
        # will only be read (and parsed) once.
        with open(fn, "r") as fp:
            json.load(fp)

    except FileNotFoundError:
        with open(fn, "w") as fp:
            json.dump(dict(), fp, separators=(",", ":"))

    except json.JSONDecodeError:
        return Result.DATABASE_CORRUPT

    return Result.SUCCESS


# define public components ============================================

@_decorate_queue
def get_value(fn: str, key: typing.Any) -> (typing.Any, Result):
    """Get a value from a JSON file.

    Take a JSON formatted file containing a single object.  Decodes the JSON
    formatting and returns the value corresponding to the given key.

    Args:
      - fn: A string describing the location of the file.
      - key: A variable containing the key of the value to retrieve.

    Returns:
        A tuple containing the value and result of the operation.  If the
        operation is successful, the value will be whatever was contained in
        the JSON object, and the result will be Result.SUCCESS.  If the
        operation failed, the value will be None and the result will be one of
        the following:
          - Result.DATABASE_CORRUPT if the file exists but cannot be parsed by
              the JSON decoder.
          - Result.DATABASE_ERROR if the location of the file cannot be
              created.
          - Result.KEY_NOT_FOUND if the object does not contain the key.
          - Result.DATABASE_SUCCESS if the value was successfully retrieved.
    """

    fn = os.path.join(_DB_DIRECTORY, fn)
    if (result := _ensure_exists(fn)) != Result.SUCCESS:
        return None, result

    with open(fn, "r") as fp:
        data = json.load(fp)

    try:
        return data[key], Result.SUCCESS
    except KeyError:
        return None, Result.KEY_NOT_FOUND


@_decorate_queue
def set_value(fn: str, key: typing.Any, value: typing.Any, duplicate_ok: bool = False) -> Result:
    """Set a value in a JSON file.

    Take a JSON formatted file containing a single object.  Decodes the JSON
    formatting, sets a new attribute in the object, and writes it back into the
    file.

    Args:
      - fn: A string describing the location of the file.
      - key: A variable containing the name of the attribute.
      - value: A variable containing the value of the attribute.
      - duplicate_ok: Optional; A bool.

    Returns:
      - Result.DATABASE_CORRUPT if the file exists but cannot be parsed by the
          JSON decoder.
      - Result.DATABASE_ERROR if the location of the file cannot be created.
      - Result.KEY_COLLISION if the object already contains the key and
          duplicate_ok is True.
      - Result.DATABASE_SUCCESS if the attribute was successfully set.
    """

    fn = os.path.join(_DB_DIRECTORY, fn)
    if (result := _ensure_exists(fn)) != Result.SUCCESS:
        return result

    with open(fn, "r") as fp:
        data = json.load(fp)

    if not duplicate_ok and key in data:
        return Result.KEY_COLLISION

    data[key] = value
    with open(fn, "w") as fp:
        json.dump(data, fp, separators=(",", ":"))

    return Result.SUCCESS
