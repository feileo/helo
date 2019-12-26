import re
from typing import Any


EMAIL_REGEX = re.compile(
    r'^ ('
    r'( ( [%(atext)s]+ (\.[%(atext)s]+)* ) | ("( [%(qtext)s\s] | \\[%(vchar)s\s] )*") )'
    r'@((?!-)[A-Z0-9-]{1,63}(?<!-)\.)+[A-Z]{2,63})$'
    % {
        'atext': '-A-Z0-9!#$%&\'*+/=?^_`{|}~',
        'qtext': '\x21\x23-\x5B\\\x5D-\x7E',
        'vchar': '\x21-\x7E'
    },
    re.I + re.X
)

URL_REGEXP = re.compile(
    r'^(?:http|ftp)s?://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)$',
    re.I
)

IPV4_REGEXP = re.compile(
    r'^( ((%(oct)s\.){3} %(oct)s) )'
    % {
        'oct': '( 25[0-5] | 2[0-4][0-9] | [0-1]?[0-9]{1,2} )'
    },
    re.I + re.X
)


def is_email(value: Any) -> bool:
    if not value:
        return False

    return EMAIL_REGEX.match(value) is not None


def is_url(value: Any) -> bool:
    if not value:
        return False

    return URL_REGEXP.match(value) is not None


def is_ipv4(value: Any) -> bool:
    if not value:
        return False

    return IPV4_REGEXP.match(value) is not None
