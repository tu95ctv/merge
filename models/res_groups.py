from .base import Table


class ResGroups(Table):
    _name = 'res_groups'

    def get_noupdate_fields(self):
        res = super(ResGroups, self).get_noupdate_fields()
        res.extend([
            'category_id',
            'name'
        ])
        return res

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM res_groups")
        all_crm_partner = self.crm.cursor.dictfetchall()
        return all_crm_partner

    def get_accounting_data(self):
        self.accounting.cursor.execute("SELECT * FROM res_groups")
        return self.accounting.cursor.dictfetchall()

    def migrate(self, clear_acc_data=True):
        self.logger.info("Migrating res.partner ....")
        groups_to_update = []
        partners_mapping = {}
        existing_partner = {}
        crm_group_toinsert = []
        all_crm_groups = self.get_crm_data()

        self.init_mapping_table()

        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.accounting.cursor.execute("SELECT * FROM ir_module_category_mapping")
        category_mapping = self.accounting.cursor.dictfetchall()
        category_mapping_dict = {x['crm_id']: x['accounting_id'] for x in category_mapping}

        all_accounting_groups = self.get_accounting_data()

        for acc_groups in all_accounting_groups:
            key = (acc_groups['name'], acc_groups['category_id'])
            existing_partner[key] = acc_groups['id']
        partner_user_fields = [
            'write_uid',
            'create_uid',
        ]
        next_id = int(self.get_highest_id()) + 1
        for group in all_crm_groups:
            if group['category_id'] in category_mapping_dict:
                group['category_id'] = category_mapping_dict[group['category_id']]
            key = (group['name'], group['category_id'])
            if existing_partner.get(key, False):
                partners_mapping[group['id']] = existing_partner.get(key)
                groups_to_update.append(group)
            else:
                for field in partner_user_fields:
                    if group[field] in user_mapping_dict:
                        group[field] = user_mapping_dict[group[field]]
                partners_mapping[group['id']] = next_id
                group['id'] = next_id
                crm_group_toinsert.append(tuple([group[k] for k in group]))
                next_id += 1

        # Insert partner
        if crm_group_toinsert:
            ins_query = self.prepare_insert(crm_group_toinsert, all_crm_groups[0].keys())
            query = self.accounting.cursor.mogrify(ins_query, crm_group_toinsert).decode('utf8')
            self.accounting.cursor.execute(query)
            self.set_highest_id(next_id)

        # Update group
        update_queries = []
        for partner_update in groups_to_update:
            update_query = self.prepare_update(partner_update)
            name = partner_update['name']
            del partner_update['name']
            category_id = partner_update['category_id']
            del partner_update['category_id']
            vals = [v for k, v in partner_update.items() if k not in self.noupdate_fields]
            vals.append(name)
            vals.append(category_id)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))

        if update_queries:
            chunks = [tuple(update_queries[x:x + 10000]) for x in range(0, len(update_queries), 10000)]
            for chunk in chunks:
                self.accounting.cursor.execute(';'.join(chunk))

        self.store_mapping_table(partners_mapping)
        self.accounting.close()

    def prepare_update(self, data):
        def _where():
            wheres = []
            for key in ['name', 'category_id']:
                wheres.append('{key} = %s'.format(key=key))
            return ' AND '.join(wheres)

        def _values(line):
            return ','.join(
                ["{k} = %s".format(k=k) if k not in self.cursed_columns else '"{k}" = %s'.format(k=k) for k, v in
                 line.items() if k not in self.noupdate_fields])

        query = "UPDATE %s SET %s WHERE %s" % (self._name, _values(data), _where())
        return query