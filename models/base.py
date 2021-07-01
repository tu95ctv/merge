from db import Database
from config import options
import logging
from tools import split_list
logger = logging.getLogger('SIEUVIET_MIGRATE')


class Table(object):
    _name = ''
    # consider change this to list
    _update_key = ''
    cursed_columns = ('customerName', 'sellerCode')

    def __init__(self, name=''):
        if name:
            self._name = name
        self.mapping_name = self._name + "_mapping"
        self.crm = Database(options['CRM'])
        self.accounting = Database(options['ACCOUNTING'])
        self.columns = self.get_columns()
        self.columns_str = self.get_columns_str()
        self.noupdate_fields = self.get_noupdate_fields()
        self.logger = logger

    def get_noupdate_fields(self):
        return [
            'id',
            'create_uid',
            'write_uid',
            'company_id'
        ]

    def get_highest_id(self):
        self.accounting.cursor.execute("SELECT last_value FROM %s_id_seq" % self._name)
        res = self.accounting.cursor.fetchall()
        return res[0][0]

    def set_highest_id(self, highest_id):
        logger.info("Set %s as highest ID for %s" % (highest_id, self._name))
        res = self.accounting.cursor.execute("ALTER SEQUENCE %s_id_seq RESTART WITH %s;" % (self._name, highest_id))
        return res

    def get_columns(self):
        # TODO: Should store this in object
        self.crm.cursor.execute("""
        SELECT
            column_name 
        FROM
            information_schema.columns
        WHERE table_schema = 'public' AND table_name = '%s';
        """ % self._name)
        res = self.crm.cursor.fetchall()
        return [x[0] for x in res]

    def get_columns_str(self):
        return ', '.join([x if x not in self.cursed_columns else '"%s"' % x for x in self.columns])

    def init_mapping_table(self):
        logger.info("Init mapping table for %s" % self._name)
        init_mapping_tbl_query = """        
        CREATE TABLE IF NOT EXISTS %s (
            crm_id INT,
            accounting_id INT,
            ins_data VARCHAR,
            upt_data VARCHAR
        ); 
        """ % self.mapping_name
        self.accounting.cursor.execute(init_mapping_tbl_query)

    def store_mapping_table(self, data):
        logger.info("Store mapping table for %s" % self._name)
        ins_query = "INSERT INTO %s (crm_id, accounting_id, ins_data, upt_data) VALUES " % self.mapping_name
        ins_list = []
        for key in data:
            ins_str = self.accounting.cursor.mogrify('(%s, %s, %s, %s)', (key, data[key]['map_id'], data[key]['ins_data'], data[key]['upt_data'])).decode('utf8')
            ins_list.append(ins_str)
        chunks = split_list(ins_list)
        for chunk in chunks:
            query = ins_query + ", ".join(chunk)
            self.accounting.cursor.execute(query)

    def prepare_insert(self, data, keys):
        keys = [x if x not in self.cursed_columns else '"%s"' % x for x in keys]
        insert_query = '''INSERT INTO %s (%s) VALUES ''' % (self._name, ','.join(keys))
        args_str = ','.join(['%s'] * len(data))
        insert_query += args_str
        return insert_query

    def prepare_update(self, data):
        def _where():
            return '{key} = %s'.format(key=self._update_key)

        def _values(line):
            return ','.join(
                ["{k} = %s".format(k=k) if k not in self.cursed_columns else '"{k}" = %s'.format(k=k) for k, v in
                 line.items() if k not in self.noupdate_fields])

        query = "UPDATE %s SET %s WHERE %s" % (self._name, _values(data), _where())
        return query

    def get_crm_data(self):
        raise NotImplementedError()

    def migrate(self, clear_acc_data=False):
        raise NotImplementedError()
