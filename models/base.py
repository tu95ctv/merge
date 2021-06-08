class Table(object):
    _name = ''

    def __init__(self, db):
        self.mapping_name = self._name + "_mapping"
        self.db = db
        self.cr = db.cursor
        self.columns = self.get_columns()
        self.columns_str = self.get_columns_str()

    def get_highest_id(self):
        self.cr.execute("SELECT last_value FROM %s_id_seq" % self._name)
        res = self.cr.fetchall()
        return res[0][0]

    def get_columns(self):
        # TODO: Should store this in object
        self.cr.execute("""
        SELECT
            column_name 
        FROM
            information_schema.columns
        WHERE table_schema = 'public' AND table_name = '%s';
        """ % self._name)
        res = self.cr.fetchall()
        return [x[0] for x in res]

    def get_columns_str(self):
        return ', '.join([x if x != 'customerName' else '"%s"' % x for x in self.columns])

    def select_all(self):
        self.cr.execute("SELECT %s FROM %s" % (self.columns_str, self._name))
        return self.cr.dictfetchall()

    def init_mapping_table(self):
        init_mapping_tbl_query = """
        DROP TABLE IF EXISTS %s;
        CREATE TABLE %s (
            crm_id INT,
            accounting_id INT
        ); 
        """ % (self.mapping_name, self.mapping_name)
        self.cr.execute(init_mapping_tbl_query)

    def store_mapping_table(self, data):
        ins_query = "INSERT INTO %s (crm_id, accounting_id) VALUES " % self.mapping_name
        ins_list = []
        for key in data:
            ins_list.append(str((key, data[key])))
        ins_query += ", ".join(ins_list)
        self.cr.execute(ins_query)

    def prepare_insert(self, data):
        insert_query = '''INSERT INTO %s (%s) VALUES ''' % (self._name, self.columns_str)
        args_str = ','.join(['%s'] * len(data))
        insert_query += args_str
        return insert_query

    def migrate(self, crm_datas):
        raise Warning("NOT IMPLEMENT")
