"""
Persistent state on all servers:
(Updated on stable storage before responding to RPCs)
"""

import json
import sqlite3


create_log_table = """
CREATE TABLE IF NOT EXISTS log (
    log_index INTEGER PRIMARY KEY,
    log_term INTEGER NOT NULL,
    log_entry VARCHAR
)
"""

insert_log = """
INSERT OR IGNORE
    INTO log (log_index, log_term)
    VALUES (0, 0)
"""


class SQLiteLog:

    def __init__(self, connection):
        self.con = connection
        self.init()

    def init(self):
        self.con.execute(create_log_table)
        self.con.execute(insert_log)

    def last(self):
        sel = """
            SELECT log_index, log_term
                FROM log
                ORDER BY log_index DESC
                LIMIT 1
        """
        return self.con.execute(sel).fetchone()


create_server_table = """
CREATE TABLE IF NOT EXISTS server (
    id INTEGER PRIMARY KEY,
    current_term INTEGER NOT NULL,
    voted_for VARCHAR,
    CHECK( id = 1)
) ;
"""

insert_server = """
INSERT OR IGNORE
    INTO server (id, current_term)
    VALUES (1, 0)
"""

update_current_term = """
UPDATE server
    SET current_term = :current_term,
        voted_for = NULL
    WHERE current_term < :current_term
"""
select_current_term = "SELECT current_term FROM server"

update_voted_for = "UPDATE server SET voted_for = :voted_for"
select_voted_for = "SELECT voted_for FROM server"


class SQLitePersistentState:

    def __init__(self, connection=None):
        if connection is None:
            connection = sqlite3.connect(':memory:')
        self.con = connection
        self.init()
        self.log = SQLiteLog(connection)

    def init(self):
        self.con.execute(create_server_table)
        self.con.execute(insert_server)

    def set_current_term(self, term):
        rc = self.con.execute("""
            UPDATE server
                SET current_term = :term,
                    voted_for = NULL
                WHERE current_term < :term
        """, dict(term=term)).rowcount
        if rc != 1:
            raise ValueError("update current_term modified {} rows".format(rc))

    def get_current_term(self):
        (current_term,) = self.con.execute(select_current_term).fetchone()
        return current_term

    @property
    def voted_for(self):
        (voted_for,) = self.con.execute(select_voted_for).fetchone()
        return voted_for and json.loads(voted_for)

    @voted_for.setter
    def voted_for(self, candidate):
        params = dict(voted_for=json.dumps(candidate))
        rowcount = self.con.execute(update_voted_for, params).rowcount
        assert rowcount > 0

    @classmethod
    def connect(cls, *args, **kwargs):
        connection = sqlite3.connect(*args, **kwargs)
        return cls(connection)
