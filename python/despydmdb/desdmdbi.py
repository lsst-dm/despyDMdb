#!/usr/bin/env python

"""Higher-level DB functions used across multiple svn projects of DESDM.

Modified more often than the lower-level despydb
"""


import re
import sys
import copy
import time
import socket
from collections import OrderedDict

import despydb.desdbi as desdbi
import despydmdb.dmdb_defs as dmdbdefs
import despymisc.miscutils as miscutils


class DesDmDbi (desdbi.DesDbi):
    """Adds DB functions used across various DM projects.
    """

    def __init__(self, desfile=None, section=None):
        desdbi.DesDbi.__init__(self, desfile, section, retry=True)

    def exec_sql_expression(self, expression):
        """Execute an SQL expression or expressions.

        Construct and execute an SQL statement from a string containing an SQL
        expression or a list of such strings.  Return a sequence containing a
        result for each column.
        """
        if isinstance(expression, list):
            s = ','.join(expression)
        else:
            s = expression

        stmt = self.get_expr_exec_format() % s
        cursor = self.cursor()
        cursor.execute(stmt)
        res = cursor.fetchone()
        cursor.close()
        return res

    def get_expr_exec_format(self):
        """Return a format string for a statement to execute SQL expressions.

        The returned format string contains a single unnamed python subsitution
        string that expects a string containing the expressions to be executed.
        Once the expressions have been substituted into the string, the
        resulting SQL statement may be executed.

        Examples:
            expression:      con.get_expr_exec_format()
            oracle result:   SELECT %s FROM DUAL
            postgres result: SELECT %s

            expression:      con.get_expr_exec_format() % 'func1(), func2()'
            oracle result:   SELECT func1(), func2() FROM DUAL
            postgres result: SELECT func1(), func2()
        """
        return self.con.get_expr_exec_format()

    def get_metadata(self):
        sql = "select * from ops_metadata"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(list(zip(desc, line)))
            headername = d['file_header_name'].lower()
            columnname = d['column_name'].lower()
            if headername not in result:
                result[headername] = OrderedDict()
            if columnname not in result[headername]:
                result[headername][columnname] = d
            else:
                raise Exception("Found duplicate row in metadata (%s, %s)" % (headername, columnname))

        curs.close()
        return result

    def get_all_filetype_metadata(self):
        """Provide a complete set of filetype metadata required during a run.

        Gets a dictionary of dictionaries or string=value pairs representing
        data from the OPS_METADATA, OPS_FILETYPE, and OPS_FILETYPE_METADATA tables.
        """
        sql = """select f.filetype, f.metadata_table, f.filetype_mgmt,
                    nvl(fm.file_hdu, 'primary') file_hdu,
                    fm.status, fm.derived,
                    fm.file_header_name, m.column_name
                from OPS_METADATA m, OPS_FILETYPE f, OPS_FILETYPE_METADATA fm
                where m.file_header_name=fm.file_header_name
                    and f.filetype=fm.filetype
                    and fm.status != 'I'
                """
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for row in curs:
            info = dict(list(zip(desc, row)))
            ptr = result
            ftype = info['filetype'].lower()
            if ftype not in result:
                result[ftype] = OrderedDict({'hdus': OrderedDict()})
                if info['metadata_table'] is not None:
                    result[ftype]['metadata_table'] = info['metadata_table'].lower()
                if info['filetype_mgmt'] is not None:
                    result[ftype]['filetype_mgmt'] = info['filetype_mgmt']

            if info['file_hdu'].lower() not in result[ftype]['hdus']:
                result[ftype]['hdus'][info['file_hdu'].lower()] = OrderedDict()

            ptr = result[ftype]['hdus'][info['file_hdu'].lower()]
            if info['status'].lower() not in ptr:
                ptr[info['status'].lower()] = OrderedDict()

            ptr = ptr[info['status'].lower()]
            if info['derived'].lower() not in ptr:
                ptr[info['derived'].lower()] = OrderedDict()

            ptr[info['derived'].lower()][info['file_header_name'].lower()] = info['column_name'].lower()

        curs.close()

        return result

    def get_site_info(self):
        """Return contents of ops_site and ops_site_val tables.
        """
        # assumes foreign key constraints so cannot have site in ops_site_val that isn't in ops_site

        site_info = self.query_results_dict('select * from ops_site', 'name')

        sql = "select name,key,val from ops_site_val"
        curs = self.cursor()
        curs.execute(sql)
        for (name, key, val) in curs:
            site_info[name][key] = val
        return site_info

    def get_archive_info(self):
        """Return contents of ops_archive and ops_archive_val tables.
        """
        # assumes foreign key constraints so cannot have archive in ops_archive_val that isn't in ops_archive

        archive_info = self.query_results_dict('select * from ops_archive', 'name')

        sql = "select name,key,val from ops_archive_val"
        curs = self.cursor()
        curs.execute(sql)
        for (name, key, val) in curs:
            archive_info[name][key] = val
        return archive_info

    def get_archive_transfer_info(self):
        """Return contents of ops_archive_transfer and ops_archive_transfer_val tables.
        """
        archive_transfer = OrderedDict()
        sql = "select src,dst,transfer from ops_archive_transfer"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                archive_transfer[row[0]] = OrderedDict()
            archive_transfer[row[0]][row[1]] = OrderedDict({'transfer': row[2]})

        sql = "select src,dst,key,val from ops_archive_transfer_val"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                miscutils.fwdebug(
                    0, 'DESDBI_DEBUG', "WARNING: found info in ops_archive_transfer_val for src archive %s which is not in ops_archive_transfer" % row[0])
                archive_transfer[row[0]] = OrderedDict()
            if row[1] not in archive_transfer[row[0]]:
                miscutils.fwdebug(
                    0, 'DESDBI_DEBUG', "WARNING: found info in ops_archive_transfer_val for dst archive %s which is not in ops_archive_transfer" % row[1])
                archive_transfer[row[0]][row[1]] = OrderedDict()
            archive_transfer[row[0]][row[1]][row[2]] = row[3]
        return archive_transfer

    def get_job_file_mvmt_info(self):
        """Return contents of ops_job_file_mvmt and ops_job_file_mvmt_val tables.
        """
        # [site][home][target][key] = [val]  where req key is mvmtclass

        sql = "select site,home_archive,target_archive,mvmtclass from ops_job_file_mvmt"
        curs = self.cursor()
        curs.execute(sql)
        info = OrderedDict()
        for (site, home, target, mvmt) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if site not in info:
                info[site] = OrderedDict()
            if home not in info[site]:
                info[site][home] = OrderedDict()
            info[site][home][target] = OrderedDict({'mvmtclass': mvmt})

        sql = "select site,home_archive,target_archive,key,val from ops_job_file_mvmt_val"
        curs = self.cursor()
        curs.execute(sql)
        for (site, home, target, key, val) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if (site not in info or
                home not in info[site] or
                    target not in info[site][home]):
                miscutils.fwdie("Error: found info in ops_job_file_mvmt_val (%s, %s, %s, %s, %s) which is not in ops_job_file_mvmt" % (
                    site, home, target, key, val), 1)
            info[site][home][target][key] = val
        return info

    def load_artifact_gtt(self, filelist):
        """Insert file artifact information into global temp table.
        """
        # filelist is list of file dictionaries
        # returns artifact GTT table name

        parsemask = miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION

        # make sure table is empty before loading it
        self.empty_gtt(dmdbdefs.DB_GTT_ARTIFACT)

        colmap = [dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION,
                  dmdbdefs.DB_COL_MD5SUM, dmdbdefs.DB_COL_FILESIZE]
        rows = []
        for file in filelist:
            miscutils.fwdebug(3, 'DESDBI_DEBUG', "file = %s" % file)
            fname = None
            comp = None
            md5sum = None
            filesize = None
            if (dmdbdefs.DB_COL_FILENAME in file or dmdbdefs.DB_COL_FILENAME.lower() in file):
                if dmdbdefs.DB_COL_COMPRESSION in file:
                    fname = file[dmdbdefs.DB_COL_FILENAME]
                    comp = file[dmdbdefs.DB_COL_COMPRESSION]
                elif dmdbdefs.DB_COL_COMPRESSION.lower() in file:
                    fname = file[dmdbdefs.DB_COL_FILENAME.lower()]
                    comp = file[dmdbdefs.DB_COL_COMPRESSION.lower()]
                elif dmdbdefs.DB_COL_FILENAME in file:
                    (fname, comp) = miscutils.parse_fullname(file[dmdbdefs.DB_COL_FILENAME], parsemask)
                else:
                    (fname, comp) = miscutils.parse_fullname(
                        file[dmdbdefs.DB_COL_FILENAME.lower()], parsemask)
                miscutils.fwdebug(3, 'DESDBI_DEBUG', "fname=%s, comp=%s" % (fname, comp))
            elif 'fullname' in file:
                (fname, comp) = miscutils.parse_fullname(file['fullname'], parsemask)
                miscutils.fwdebug(3, 'DESDBI_DEBUG', "parse_fullname: fname=%s, comp=%s" % (fname, comp))
            else:
                miscutils.fwdebug(3, 'DESDBI_DEBUG', "file=%s" % (file))
                raise ValueError("Invalid entry filelist (%s)" % file)

            if dmdbdefs.DB_COL_FILESIZE in file:
                filesize = file[dmdbdefs.DB_COL_FILESIZE]
            elif dmdbdefs.DB_COL_FILESIZE.lower() in file:
                filesize = file[dmdbdefs.DB_COL_FILESIZE.lower()]

            if dmdbdefs.DB_COL_MD5SUM in file:
                md5sum = file[dmdbdefs.DB_COL_MD5SUM]
            elif dmdbdefs.DB_COL_MD5SUM.lower() in file:
                md5sum = file[dmdbdefs.DB_COL_MD5SUM.lower()]

            miscutils.fwdebug(3, 'DESDBI_DEBUG', "row: fname=%s, comp=%s, filesize=%s, md5sum=%s" %
                              (fname, comp, filesize, md5sum))
            rows.append({dmdbdefs.DB_COL_FILENAME: fname, dmdbdefs.DB_COL_COMPRESSION: comp,
                         dmdbdefs.DB_COL_FILESIZE: filesize, dmdbdefs.DB_COL_MD5SUM: md5sum})

        self.insert_many(dmdbdefs.DB_GTT_ARTIFACT, colmap, rows)
        return dmdbdefs.DB_GTT_ARTIFACT

    def load_filename_gtt(self, filelist):
        """Insert filenames into filename global temp table.

        To use in join for later query.
        """
        # returns filename GTT table name

        # make sure table is empty before loading it
        self.empty_gtt(dmdbdefs.DB_GTT_FILENAME)

        colmap = [dmdbdefs.DB_COL_FILENAME, dmdbdefs.DB_COL_COMPRESSION]
        rows = []
        for file in filelist:
            fname = None
            comp = None
            if isinstance(file, str):
                (fname, comp) = miscutils.parse_fullname(
                    file, miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
            elif isinstance(file, dict) and (dmdbdefs.DB_COL_FILENAME in file or dmdbdefs.DB_COL_FILENAME.lower() in file):
                if dmdbdefs.DB_COL_COMPRESSION in file:
                    fname = file[dmdbdefs.DB_COL_FILENAME]
                    comp = file[dmdbdefs.DB_COL_COMPRESSION]
                elif dmdbdefs.DB_COL_COMPRESSION.lower() in file:
                    fname = file[dmdbdefs.DB_COL_FILENAME.lower()]
                    comp = file[dmdbdefs.DB_COL_COMPRESSION.lower()]
                elif dmdbdefs.DB_COL_FILENAME in file:
                    (fname, comp) = miscutils.parse_fullname(
                        file[dmdbdefs.DB_COL_FILENAME], miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
                else:
                    (fname, comp) = miscutils.parse_fullname(
                        file[dmdbdefs.DB_COL_FILENAME.lower()], miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
            else:
                raise ValueError("Invalid entry filelist (%s)" % file)
            rows.append({dmdbdefs.DB_COL_FILENAME: fname, dmdbdefs.DB_COL_COMPRESSION: comp})
        self.insert_many(dmdbdefs.DB_GTT_FILENAME, colmap, rows)
        return dmdbdefs.DB_GTT_FILENAME

    def load_id_gtt(self, idlist):
        self.empty_gtt(dmdbdefs.DB_GTT_ID)
        colmap = [dmdbdefs.DB_COL_ID]
        rows = []
        for desfid in idlist:
            if isinstance(desfid, int):
                rows.append({dmdbdefs.DB_COL_ID: desfid})
            else:
                raise ValueError("invalid entry idlist (%s)" % str(desfid))
        self.insert_many(dmdbdefs.DB_GTT_ID, colmap, rows)
        return dmdbdefs.DB_GTT_ID

    def empty_gtt(self, tablename):
        """Clean out temp table if one wants separate commit/rollback control.
        """
        # could be changed to generic empty table function, for now wanted safety check
        if 'gtt' not in tablename.lower():
            raise ValueError("Invalid table name for a global temp table (missing GTT)")

        sql = "delete from %s" % tablename
        curs = self.cursor()
        curs.execute(sql)
        curs.close()

    def create_task(self, name, info_table,
                    parent_task_id=None, root_task_id=None, i_am_root=False,
                    label=None, do_begin=False, do_commit=False):
        """Insert a row into the task table and return task id.
        """
        row = {'name': name, 'info_table': info_table}

        row['id'] = self.get_seq_next_value('task_seq') # get task id

        if parent_task_id is not None:
            row['parent_task_id'] = int(parent_task_id)

        if i_am_root:
            row['root_task_id'] = row['id']
        elif root_task_id is not None:
            row['root_task_id'] = int(root_task_id)

        if label is not None:
            row['label'] = label

        self.basic_insert_row('task', row)

        if do_begin:
            self.begin_task(row['id'])

        if do_commit:
            self.commit()

        return row['id']

    def begin_task(self, task_id, do_commit=False):
        """Update a row in the task table with beginning of task info.
        """
        updatevals = {'start_time': self.get_current_timestamp_str(),
                      'exec_host': socket.gethostname()}
        wherevals = {'id': task_id} # get task id

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()

    def end_task(self, task_id, status, do_commit=False):
        """Update a row in the task table with end of task info.
        """
        wherevals = {}
        wherevals['id'] = task_id

        updatevals = {}
        updatevals['end_time'] = self.get_current_timestamp_str()
        updatevals['status'] = status

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()

    def get_datafile_metadata(self, filetype):
        """Gets a dictionary of all datafile (such as XML or fits table data files) metadata for the given filetype.

        Returns a list: [target_table_name,metadata]
        """
        TABLE = 0
        HDU = 1
        ATTRIBUTE = 2
        POSITION = 3
        COLUMN = 4
        DATATYPE = 5
        FORMAT = 6

        bindstr = self.get_named_bind_string("afiletype")
        sql = """select table_name, hdu, lower(attribute_name), position, lower(column_name), datafile_datatype, data_format
                from OPS_DATAFILE_TABLE df, OPS_DATAFILE_METADATA md
                where df.filetype = md.filetype and current_flag=1 and lower(df.filetype) = lower(""" + bindstr + """)
                order by md.attribute_name, md.POSITION"""
        result = OrderedDict()
        curs = self.cursor()
        curs.execute(sql, {"afiletype": filetype})

        tablename = None
        for row in curs:
            if tablename == None:
                tablename = row[TABLE]
            if row[HDU] not in list(result.keys()):
                result[row[HDU]] = {}
            if row[ATTRIBUTE] not in list(result[row[HDU]].keys()):
                result[row[HDU]][row[ATTRIBUTE]] = {}
                result[row[HDU]][row[ATTRIBUTE]]['datatype'] = row[DATATYPE]
                result[row[HDU]][row[ATTRIBUTE]]['format'] = row[FORMAT]
                result[row[HDU]][row[ATTRIBUTE]]['columns'] = []
            if len(result[row[HDU]][row[ATTRIBUTE]]['columns']) == row[POSITION]:
                result[row[HDU]][row[ATTRIBUTE]]['columns'].append(row[COLUMN])
            else:
                result[row[HDU]][row[ATTRIBUTE]]['columns'][row[POSITION]] = row[COLUMN]
        curs.close()
        if tablename is None:
            raise ValueError('Invalid filetype - missing entries in datafile tables')
        return [tablename, result]


#### Embedded simple test
if __name__ == '__main__':
    dbh = DesDmDbi()
    print('dbh = ', dbh)
    if dbh.is_postgres():
        print('Connected to postgres DB')
    elif dbh.is_oracle():
        print('Connected to oracle DB')
    print('which_services_file = ', dbh.which_services_file())
    print('which_services_section = ', dbh.which_services_section())

    print(dbh.get_column_names('exposure'))

    cursor = dbh.cursor()
    cursor.execute('SELECT count(*) from exposure')
    row = cursor.fetchone()
    print('Number exposures:', row[0])
    cursor.close()
    #dbh.commit()
    dbh.close()
