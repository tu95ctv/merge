from .base_master_data import BaseMasterData


class ResUsersRole(BaseMasterData):
    _name = 'res_users_role'
    _update_key = 'group_id'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM res_users_role")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=False):
        datas = self.get_crm_data()
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        users_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}
        self.accounting.cursor.execute("SELECT * FROM res_groups_mapping")
        groups_mapping = {x['crm_id']: x['accounting_id'] for x in self.accounting.cursor.dictfetchall()}
        self.accounting.cursor.execute("SELECT * FROM res_users_role")
        current_acc_data = self.accounting.cursor.dictfetchall()
        mapping = {x['group_id']: x['id'] for x in current_acc_data}
        to_inserts = []
        current_id = max_id = int(self.get_highest_id())
        for role in datas:
            if role['group_id'] not in mapping:
                for f in ['create_uid', 'write_uid']:
                    if role[f] in users_mapping_dict:
                        role[f] = users_mapping_dict[role[f]]
                if role['group_id'] in groups_mapping:
                    role['group_id'] = groups_mapping[role['group_id']]
                to_inserts.append(tuple([role[k] for k in role]))
                max_id = max(role['id'], max_id)
        if current_id != max_id:
            self.set_highest_id(max_id)
        chunks = [tuple(to_inserts[x:x + 10000]) for x in range(0, len(to_inserts), 10000)]
        for i, chunk in enumerate(chunks):
            ins_query = self.prepare_insert(chunk, datas[0].keys())
            query = self.accounting.cursor.mogrify(ins_query, chunk).decode('utf8')
            self.accounting.cursor.execute(query)
        self.accounting.close()
