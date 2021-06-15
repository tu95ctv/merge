from .base import Table


class ResUser(Table):
    _name = 'res_users'

    def drop_partner_id_not_null(self):
        self.db.cursor.execute("ALTER TABLE public.res_users ALTER COLUMN partner_id DROP NOT NULL;")

    def set_partner_id_not_null(self):
        self.db.cursor.execute("ALTER TABLE public.res_users ALTER COLUMN partner_id SET NOT NULL;")

    def migrate(self, crm_datas):
        self.init_mapping_table()
        # Partner doesn't exist at this point so remove not null constrain temporary
        self.drop_partner_id_not_null()
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
        self.set_highest_id(next_id)
        self.db.close()

    def update_partner_id(self, user_partner):
        queries = []
        for user_id, partner_id in user_partner.items():
            queries.append("UPDATE res_users set partner_id = %s WHERE id = %s" (partner_id, user_id))

        queries = ';'.join(queries)
        self.db.cursor.execute(queries)
        # Add not null constrain on partner_id
        self.set_partner_id_not_null()
        self.db.close()
