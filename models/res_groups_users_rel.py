from .base import Table


class ResGroupsUsersRel(Table):

    _name = 'res_groups_users_rel'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM %s" % self._name)
        crm_datas = self.crm.cursor.dictfetchall()
        return crm_datas

    def migrate(self, clear_acc_data=True):
        # Get User mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        users_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        # Get Groups mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_groups_mapping")
        groups_mapping = self.accounting.cursor.dictfetchall()
        groups_mapping_dict = {x['crm_id']: x['accounting_id'] for x in groups_mapping}

        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        current_rels = {(x['gid'], x['uid']): 1 for x in self.accounting.cursor.dictfetchall()}

        crm_datas = self.get_crm_data()
        toinsert = []

        for line in crm_datas:
            if line['uid'] in users_mapping_dict:
                line['uid'] = users_mapping_dict[line['uid']]
            if line['gid'] in groups_mapping_dict:
                line['gid'] = groups_mapping_dict[line['gid']]
            if (line['gid'], line['uid']) not in current_rels:
                toinsert.append(tuple(line[k] for k in line))

        ins_query = self.prepare_insert(toinsert, line.keys())
        query = self.accounting.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.accounting.cursor.execute(query)
        self.accounting.close()
