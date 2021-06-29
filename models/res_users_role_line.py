from .base import Table


class ResUsersRoleLine(Table):
    _name = 'res_users_role_line'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM res_users_role_line")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=True):
        datas = self.get_crm_data()
        # Get Users mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        users_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        current_rels = {(x['role_id'], x['user_id']): 1 for x in self.accounting.cursor.dictfetchall()}
        toinsert = []
        next_id = int(self.get_highest_id()) + 1
        for line in datas:
            for f in ['user_id', 'create_uid', 'write_uid']:
                if line[f] in users_mapping_dict:
                    line[f] = users_mapping_dict[line[f]]
            if (line['role_id'], line['user_id']) not in current_rels:
                line['id'] = next_id
                next_id += 1
                toinsert.append(tuple(line[k] for k in line))
        # Free some memory
        self.set_highest_id(next_id)
        ins_query = self.prepare_insert(toinsert, line.keys())
        query = self.accounting.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.accounting.cursor.execute(query)
        self.accounting.close()
