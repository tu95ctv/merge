from .table import Table


class ResUser(Table):
    _name = 'res_users'

    def migrate(self, crm_datas):

        self.init_mapping_table()
        all_accounting_users = self.select_all()
        all_crm_users = crm_datas
        users_mapping = {}
        existing_users = {}
        crm_users_toinsert = []
        for acc_user in all_accounting_users:
            existing_users[acc_user['login']] = acc_user['id']

        next_id = int(self.get_highest_id()) + 1
        for crm_user in all_crm_users:
            if existing_users.get(crm_user['login'], False):
                users_mapping[crm_user['id']] = existing_users.get(crm_user['login'])
            else:
                # Ugly hack to map sale_team_id
                if crm_user['sale_team_id'] == 51:
                    crm_user['sale_team_id'] = 52
                users_mapping[crm_user['id']] = next_id
                crm_user['id'] = next_id
                next_id += 1
                crm_users_toinsert.append(tuple([crm_user[k] for k in crm_user]))
        ins_query = self.prepare_insert(crm_users_toinsert)
        self.store_mapping_table(users_mapping)
        query = self.db.cursor.mogrify(ins_query, crm_users_toinsert).decode('utf8')
        self.db.cursor.execute(query)
        self.db.close()
