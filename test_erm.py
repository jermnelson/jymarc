"""
 :mod:`test_erm` Tests for the ERM update module
"""
__author__ = "Jeremy Nelson"

import erm_update

def test_format_holding_stmt():
    for value in [('1.1 2002- 01- 01-','(Jan. 01, 2002)-'),
                  ("1.1 2002-2006 01-12 01-31",'(Jan. 01, 2002)-(Dec. 31, 2006)'),
                  ("1.1 58- 1- 2004- 01- 01-",'v.58:no.1 (Jan. 01, 2004)-')]:
        yield check_format_holding_stmt, value[0], value[1]

def check_format_holding_stmt(raw_value, formatted_value):
    func_value = erm_update.format_holding_stmt(raw_value)
    assert  func_value == formatted_value
