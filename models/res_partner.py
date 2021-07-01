from .base import Table


class ResPartner(Table):
    _name = 'res_partner'

    _update_key = 'ref'

    def __init__(self, name=''):
        super(ResPartner, self).__init__(name)
        self.columns.remove('parent_id')
        self.columns.remove('commercial_partner_id')
        self.columns_str = self.get_columns_str()

    def get_noupdate_fields(self):
        res = super(ResPartner, self).get_noupdate_fields()
        res.extend([
            'ref',
            'user_id',
            'parent_id',
            'commercial_partner_id',
            'name',
            'display_name'
        ])
        return res

    def get_crm_data(self):
        # Note: 211 is ID of API user
        self.crm.cursor.execute(
            "SELECT * FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL AND create_uid = 211")
        all_crm_partner = self.crm.cursor.dictfetchall()

        # Get partner that link to user from CRM side
        self.accounting.cursor.execute("SELECT login FROM res_users WHERE partner_id IS NULL")
        logins = tuple(x[0] for x in self.accounting.cursor.fetchall())
        self.crm.cursor.execute("SELECT partner_id FROM res_users WHERE login in %s" % (logins,))
        user_partner = tuple(x[0] for x in self.crm.cursor.fetchall())
        self.crm.cursor.execute(
            "SELECT * FROM res_partner WHERE id IN %s" % (user_partner, ))
        all_crm_partner.extend(self.crm.cursor.dictfetchall())
        return all_crm_partner

    def migrate(self, clear_acc_data=True):
        self.logger.info("Migrating res.partner ....")
        partners_to_update = []
        partners_mapping = {}
        crm_partner_toinsert = []
        all_crm_partner = self.get_crm_data()

        self.init_mapping_table()

        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.accounting.cursor.execute("SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % self.columns_str)
        all_accounting_partner = self.accounting.cursor.dictfetchall()

        existing_partner = {x['ref']: x['id'] for x in all_accounting_partner if x['ref']}
        partner_user_fields = [
            'write_uid',
            'create_uid',
            'user_id'
        ]
        next_id = int(self.get_highest_id()) + 1
        for partner in all_crm_partner:
            if existing_partner.get(partner['ref'], False):
                partners_mapping[partner['id']] = {
                    'map_id': existing_partner.get(partner['ref']),
                    'ins_data': '',
                    'upt_data': str(partner)
                }
                partners_to_update.append(partner)
            else:
                for field in partner_user_fields:
                    if partner[field] in user_mapping_dict:
                        partner[field] = user_mapping_dict[partner[field]]
                partners_mapping[partner['id']] = {
                    'map_id': next_id,
                    'ins_data': str(partner),
                    'upt_data': '',
                }
                partner['id'] = next_id
                del partner['commercial_partner_id']
                del partner['parent_id']
                crm_partner_toinsert.append(tuple([partner[k] for k in partner]))
                next_id += 1

        # Insert partner
        if crm_partner_toinsert:
            ins_query = self.prepare_insert(crm_partner_toinsert, all_crm_partner[0].keys())
            query = self.accounting.cursor.mogrify(ins_query, crm_partner_toinsert).decode('utf8')
            self.accounting.cursor.execute(query)
            self.set_highest_id(next_id)

        # Update partner
        update_queries = []
        for partner_update in partners_to_update:
            update_query = self.prepare_update(partner_update)
            ref = partner_update['ref']
            del partner_update['ref']
            vals = [v for k, v in partner_update.items() if k not in self.noupdate_fields]
            vals.append(ref)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))

        if update_queries:
            chunks = [tuple(update_queries[x:x + 10000]) for x in range(0, len(update_queries), 10000)]
            for chunk in chunks:
                self.accounting.cursor.execute(';'.join(chunk))

        self.store_mapping_table(partners_mapping)
        self.accounting.close()
