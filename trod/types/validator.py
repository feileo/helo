import re
from typing import Any


FWS = r'(?:(?:[ \t]*(?:\r\n))?[ \t]+)'
COMMENT = r'\((?:' + FWS + r'?' + \
    r'(?:[\x01-\x08\x0b\x0c\x0f-\x1f\x7f\x21-\x27\x2a-\x5b\x5d-\x7e]|(?:\\.))' + \
    r')*' + FWS + r'?\)'
CFWS = r'(?:' + FWS + r'?' + COMMENT + ')*(?:' + \
       FWS + '?' + COMMENT + '|' + FWS + ')'
ATEXT = r'[\w!#$%&\'\*\+\-/=\?\^`\{\|\}~]'
ATOM = CFWS + r'?' + ATEXT + r'+' + CFWS + r'?'
DOT_ATOM_TEXT = ATEXT + r'+(?:\.' + ATEXT + r'+)*'
DOT_ATOM = CFWS + r'?' + DOT_ATOM_TEXT + CFWS + r'?'
QTEXT = r'[' + r'\x01-\x08\x0b\x0c\x0f-\x1f\x7f' + r'\x21\x23-\x5b\x5d-\x7e]'
QCONTENT = r'(?:' + QTEXT + r'|' + r'(?:\\.)' + r')'
QUOTED_STRING = CFWS + r'?' + r'"(?:' + FWS + \
    r'?' + QCONTENT + r')*' + FWS + r'?' + r'"' + CFWS + r'?'
LOCAL_PART = r'(?:' + DOT_ATOM + r'|' + QUOTED_STRING + r')'
DTEXT = r'[' + r'\x01-\x08\x0b\x0c\x0f-\x1f\x7f' + r'\x21-\x5a\x5e-\x7e]'
DCONTENT = r'(?:' + DTEXT + r'|' + r'(?:\\.)' + r')'
DOMAIN_LITERAL = CFWS + r'?' + r'\[' + \
    r'(?:' + FWS + r'?' + DCONTENT + \
    r')*' + FWS + r'?\]' + CFWS + r'?'
DOMAIN = r'(?:' + DOT_ATOM + r'|' + DOMAIN_LITERAL + r')'
ADDR_SPEC = LOCAL_PART + r'@' + DOMAIN

VALID_EMAIL_REGEXP = '^' + ADDR_SPEC + '$'


def is_email(value: Any) -> bool:
    if not value:
        return False

    return re.match(VALID_EMAIL_REGEXP, value) is not None


VALID_URL_REGEXP = (
    r'^(?:http|ftp)s?://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)$'
)


def is_url(value: Any) -> bool:
    if not value:
        return False

    regex = re.compile(VALID_URL_REGEXP, re.IGNORECASE)
    return regex.match(value) is not None
