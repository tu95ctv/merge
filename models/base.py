class Table(object):
    _name = ''
    # consider change this to list
    _update_key = ''
    cursed_columns = ('customerName', 'sellerCode')

    def __init__(self, db):
        self.mapping_name = self._name + "_mapping"
        self.db = db
        self.cr = db.cursor
        self.columns = self.get_columns()
        self.columns_str = self.get_columns_str()
        self.noupdate_fields = self.get_noupdate_fields()

    def get_noupdate_fields(self):
        return [
            'id',
            'create_uid',
            'write_uid',
            'company_id'
        ]

    def get_highest_id(self):
        self.cr.execute("SELECT last_value FROM %s_id_seq" % self._name)
        res = self.cr.fetchall()
        return res[0][0]

    def set_highest_id(self, highest_id):
        res = self.cr.execute("ALTER SEQUENCE %s_id_seq RESTART WITH %s;" % (self._name, highest_id))
        return res

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
        return ', '.join([x if x not in self.cursed_columns else '"%s"' % x for x in self.columns])

    def select_all(self):
        self.cr.execute("SELECT %s FROM %s" % (self.columns_str, self._name))
        return self.cr.dictfetchall()

    def init_mapping_table(self):
        init_mapping_tbl_query = """        
        CREATE TABLE IF NOT EXISTS %s (
            crm_id INT,
            accounting_id INT
        ); 
        """ % self.mapping_name
        self.cr.execute(init_mapping_tbl_query)

    def store_mapping_table(self, data):
        ins_query = "INSERT INTO %s (crm_id, accounting_id) VALUES " % self.mapping_name
        ins_list = []
        for key in data:
            ins_list.append(str((key, data[key])))
        ins_query += ", ".join(ins_list)
        self.cr.execute(ins_query)

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

    def migrate(self, crm_datas, crm):
        raise NotImplementedError()
