
import struct
import socket
import datetime
from typing import Any, Optional, List, Union, Callable


def format_datetime(
        value: str,
        formats: Union[List[str], tuple],
        extractor: Optional[Callable] = None
) -> Any:
    extractor = extractor or (lambda x: x)
    for fmt in formats:
        try:
            return extractor(datetime.datetime.strptime(value, fmt))
        except ValueError:
            pass
    return value


def simple_datetime(value: str) -> Union[datetime.datetime, str]:
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return value


def dt_strftime(value: Any, formats: Union[List[str], tuple]) -> str:
    if hasattr(value, 'strftime'):
        for fmt in formats:
            try:
                return value.strftime(fmt)
            except (TypeError, ValueError):
                pass
    return value


def iptoint(value: str) -> int:
    try:
        return struct.unpack('!I', socket.inet_aton(value))[0]
    except OSError:
        raise ValueError(f"Illegal IP address({value}) for IP Field")


def iptostr(value: int) -> str:
    try:
        return socket.inet_ntoa(struct.pack('!I', value))
    except struct.error:
        raise ValueError("IP value must be range [0, 4294967295]"
                         f"got {value}")
