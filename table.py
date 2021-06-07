from datetime import datetime


class Table(object):
    def __init__(self, name, db):
        self.name = name
        self.mapping_name = self.name + "_mapping"
        self.db = db
        self.cr = db.cursor
        self.columns = self.get_columns()
        self.columns_str = self.get_columns_str()

    def get_highest_id(self):
        self.cr.execute("SELECT last_value FROM %s_id_seq" % self.name)
        res = self.cr.fetchall()
        return res[0][0]

    def get_columns(self):
        #TODO: Should store this in object
        self.cr.execute("""
        SELECT
            column_name 
        FROM
            information_schema.columns
        WHERE table_schema = 'public' AND table_name = '%s';
        """ % self.name)
        res = self.cr.fetchall()
        return [x[0] for x in res]

    def get_columns_str(self):
        columns = self.columns
        return ', '.join([x if x != 'customerName' else '"%s"' % x for x in columns])

    def select_all(self):
        self.cr.execute("SELECT %s FROM %s" % (self.columns_str, self.name))
        return self.cr.dictfetchall()

    def select(self):
        pass

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
        mapped_ids = {}
        columns = self.columns_str
        insert_query = "INSERT INTO %s (%s) VALUES " % (self.name, columns)
        next_id = self.get_highest_id() + 1
        lines = []
        for line in data:
            line = list(line)
            # Mapped source id & target id
            mapped_ids[line[0]] = next_id
            line[0] = next_id

            line = self.standardlize_ins_data(line)
            line = str(tuple(line))
            line = line.replace("'Null'", "Null")
            lines.append(line)

            next_id += 1
        insert_query += ', '.join(lines)
        return insert_query, mapped_ids

    def update(self):
        pass

    def standardlize_ins_data(self, line):
        for i, val in enumerate(line):
            if val == None:
                line[i] = 'Null'
            if isinstance(val, datetime):
                line[i] = str(val)
        return line
