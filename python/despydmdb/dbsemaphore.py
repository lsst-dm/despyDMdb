#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Using the database, provide semaphore capability.
"""

__version__ = "$Rev$"

import time

import despymisc.miscutils as miscutils


class DBSemaphore ():
    """
    Using the database, provide semaphore capability.
    Currently requires Oracle
    """

    def __init__(self, semname, task_id, desfile=None, section=None):
        """
        Create the DB connection and do the semaphore wait.
        """
        self.desfile = desfile
        self.section = section
        self.semname = semname
        self.task_id = task_id
        self.slot = None

        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - INFO - semname %s" % self.semname)
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - db-specific imports")
        import despydmdb.desdmdbi as desdmdbi
        import cx_Oracle
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - db-specific imports")

        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - db connection")
        self.dbh = desdmdbi.DesDmDbi(desfile, section)
        miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - db connection")

        curs = self.dbh.cursor()

        sql = 'select count(*) from semlock where name=%s' % self.dbh.get_named_bind_string('name')
        curs.execute(sql, {'name': semname})
        num_slots = curs.fetchone()[0]
        if num_slots == 0:
            miscutils.fwdebug(0, "SEMAPHORE_DEBUG", "SEM - ERROR - no locks with name %s" % semname)
            raise ValueError('No locks with name %s' % semname)

        self.id = self.dbh.get_seq_next_value('seminfo_seq')
        self.dbh.basic_insert_row('seminfo', {'id': self.id, 'name': self.semname,
                                              'request_time': self.dbh.get_current_timestamp_str(),
                                              'task_id': task_id, 'num_slots': num_slots})
        self.dbh.commit()

        self.slot = curs.var(cx_Oracle.NUMBER)
        done = False
        trycnt = 1
        MAXTRIES = 5
        TRYINTERVAL = 10
        while not done and trycnt <= MAXTRIES:
            try:
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - wait")
                curs.callproc("SEM_WAIT", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - wait")
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - INFO - slot %s" % self.slot)
                done = True
            except Exception as e:
                miscutils.fwdebug(0, "SEMAPHORE_DEBUG", "SEM - ERROR - %s" % str(e))

                time.sleep(TRYINTERVAL)

                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - remake db connection")
                self.dbh = desdmdbi.DesDmDbi(desfile, section)
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - remake db connection")

                curs = self.dbh.cursor()
                self.slot = curs.var(cx_Oracle.NUMBER)

                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - dequeue")
                curs.callproc("SEM_DEQUEUE", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - dequeue")

                trycnt += 1

        if done:
            # need different connection to do the commit of the grant info as commit will release lock
            dbh2 = desdmdbi.DesDmDbi(desfile, section)
            dbh2.basic_update_row('SEMINFO',
                                  {'grant_time': dbh2.get_current_timestamp_str(),
                                   'num_requests': trycnt,
                                   'slot': self.slot},
                                  {'id': self.id})
            dbh2.commit()

    def __del__(self):
        """ 
        Do the semaphore signal and close DB connection
        """
        if self.slot != None:
            try:
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - BEG - signal")
                curs = self.dbh.cursor()
                curs.callproc("SEM_SIGNAL", [self.semname, self.slot])
                miscutils.fwdebug(3, "SEMAPHORE_DEBUG", "SEM - END - signal")
                self.dbh.basic_update_row('SEMINFO',
                                          {'release_time': self.dbh.get_current_timestamp_str()},
                                          {'id': self.id})
                self.dbh.commit()
            except Exception as e:
                miscutils.fwdebug(0, "SEMAPHORE_DEBUG", "SEM - ERROR - %s" % str(e))

        self.slot = None
        self.dbh.close()

    def __str__(self):
        """
        x.__str__() <==> str(x)
        """
        return str({'name': self.semname, 'slot': self.slot})


if __name__ == '__main__':
    pass
