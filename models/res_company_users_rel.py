from .base_master_data import BaseMasterData


class ResCompanyUsersRel(BaseMasterData):
    _name = 'res_company_users_rel'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM %s" % self._name)
        crm_datas = self.crm.cursor.dictfetchall()
        return crm_datas

    def migrate(self, clear_acc_data=True, set_sequence=True):
        # Get User mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        users_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        # Get Groups mapping datas
        # self.accounting.cursor.execute("SELECT * FROM res_groups_mapping")
        # groups_mapping = self.accounting.cursor.dictfetchall()
        # groups_mapping_dict = {x['crm_id']: x['accounting_id'] for x in groups_mapping}

        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        current_rels = {x['user_id']: 1 for x in self.accounting.cursor.dictfetchall()}

        crm_datas = self.get_crm_data()
        toinsert = []

        for line in crm_datas:
            if line['user_id'] in users_mapping_dict:
                line['user_id'] = users_mapping_dict[line['user_id']]
            if line['user_id'] not in current_rels:
                toinsert.append(tuple(line[k] for k in line))

        ins_query = self.prepare_insert(toinsert, line.keys())
        query = self.accounting.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.accounting.cursor.execute(query)
        self.accounting.close()
