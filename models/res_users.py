from .base import Table


class ResUser(Table):
    _name = 'res_users'
    _update_key = 'login'

    def get_columns(self):
        res = super(ResUser, self).get_columns()
        res.remove('partner_id')
        res.remove('write_uid')
        res.remove('create_uid')
        return res

    def get_noupdate_fields(self):
        res = super(ResUser, self).get_noupdate_fields()
        res.extend([
            'login',
            # 'password',
            'company_id',
            'partner_id',
        ])
        return res

    def drop_partner_id_not_null(self):
        self.accounting.cursor.execute("ALTER TABLE public.res_users ALTER COLUMN partner_id DROP NOT NULL;")

    def set_partner_id_not_null(self):
        self.accounting.cursor.execute("ALTER TABLE public.res_users ALTER COLUMN partner_id SET NOT NULL;")

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM res_users")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=True):
        self.logger.info("Migrating res.users ....")
        self.init_mapping_table()
        # Partner doesn't exist at this point so remove not null constrain temporary
        self.drop_partner_id_not_null()
        self.accounting.cursor.execute("SELECT id, login FROM res_users")
        all_accounting_users = self.accounting.cursor.dictfetchall()
        all_crm_users = self.get_crm_data()
        users_mapping = {}
        existing_users = {}
        users_to_update = []
        crm_users_toinsert = []
        for acc_user in all_accounting_users:
            existing_users[acc_user['login']] = acc_user['id']

        next_id = int(self.get_highest_id()) + 1
        for crm_user in all_crm_users:
            # Ugly hack to map sale_team_id
            if crm_user['sale_team_id'] == 51:
                crm_user['sale_team_id'] = 52
            if existing_users.get(crm_user['login'], False):
                users_mapping[crm_user['id']] = existing_users.get(crm_user['login'])
                users_to_update.append(crm_user)
            else:
                users_mapping[crm_user['id']] = next_id
                crm_user['id'] = next_id
                # Set default create / write user is Administrator
                crm_user['create_uid'] = 2
                crm_user['write_uid'] = 2
                # Remove partner_id & map later
                del crm_user['partner_id']
                next_id += 1
                crm_users_toinsert.append(tuple([crm_user[k] for k in crm_user]))

        # Insert new users
        if crm_users_toinsert:
            keys = list(crm_user.keys())
            keys.remove('partner_id')
            ins_query = self.prepare_insert(crm_users_toinsert, keys)
            query = self.accounting.cursor.mogrify(ins_query, crm_users_toinsert).decode('utf8')
            self.accounting.cursor.execute(query)
            self.set_highest_id(next_id)
        # Update users
        update_queries = []
        for user in users_to_update:
            update_query = self.prepare_update(user)
            login = user['login']
            del user['login']
            vals = [v for k, v in user.items() if k not in self.noupdate_fields]
            vals.append(login)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))
        if update_queries:
            self.accounting.cursor.execute(';'.join(update_queries))

        self.store_mapping_table(users_mapping)
        self.accounting.close()

    def update_partner_id(self, user_partner):
        queries = []
        for login, partner_id in user_partner.items():
            queries.append("UPDATE res_users set partner_id = %s WHERE login = '%s'" % (partner_id, login))

        queries = ';'.join(queries)
        self.accounting.cursor.execute(queries)
        # Add not null constrain on partner_id
        self.set_partner_id_not_null()
        self.accounting.close()
