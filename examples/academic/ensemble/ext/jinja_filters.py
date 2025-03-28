"""
Jinja filters
"""

import string


def filter_member2letter(member):
    """Convert a int to an uppercase letter"""
    return string.ascii_uppercase[member.id - 1]


JINJA_FILTERS = {"member2letter": filter_member2letter}
