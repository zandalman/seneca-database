#!/usr/bin/env python
# -*- coding:utf-8 -*-

# import classes and functions
from SQLAlchemy_functions import *

# check SQLAlchemy version
if StrictVersion(sqlalchemy.__version__) < '1.3':
    print("Warning: SQLAlchemy is out of date. Update to at least version 1.3 to guarantee compatibility.")

Session = init_db()

# begin session
with session_scope(Session) as session:
    # do whatever
    pass
