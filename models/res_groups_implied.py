from .base_master_data import BaseMasterData


class ResGroupsImplied(BaseMasterData):
    _name = 'res_groups_implied_rel'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM res_groups_implied_rel")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=False, set_sequence=True):
        datas = self.get_crm_data()
        self.accounting.cursor.execute("SELECT * FROM res_groups_implied_rel")
        current_rel = {(x['gid'], x['hid']): 1 for x in self.accounting.cursor.dictfetchall()}
        self.accounting.cursor.execute("SELECT * FROM res_groups_mapping")
        groups_mapping = {x['crm_id']: x['accounting_id'] for x in self.accounting.cursor.dictfetchall()}
        to_insert = []
        for line in datas:
            if line['gid'] in groups_mapping:
                line['gid'] = groups_mapping[line['gid']]
            if line['hid'] in groups_mapping:
                line['hid'] = groups_mapping[line['hid']]
            if (line['gid'], line['hid']) not in current_rel:
                to_insert.append(tuple([line[k] for k in line]))
        if to_insert:
            ins_query = self.prepare_insert(to_insert, line.keys())
            query = self.accounting.cursor.mogrify(ins_query, to_insert).decode('utf8')
            self.accounting.cursor.execute(query)
            self.accounting.close()
