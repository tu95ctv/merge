from .base import Table


class ResPartner(Table):
    _name = 'res_partner'

    _update_key = 'ref'

    def __init__(self, db):
        super(ResPartner, self).__init__(db)
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

    def migrate(self, crm_datas, crm):
        partners_to_update = []
        partners_mapping = {}
        existing_partner = {}
        crm_partner_toinsert = []
        all_crm_partner = crm_datas

        self.init_mapping_table()

        self.db.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.db.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.db.cursor.execute("SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % crm.columns_str)
        all_accounting_partner = self.db.cursor.dictfetchall()

        for acc_partner in all_accounting_partner:
            if acc_partner['ref']:
                existing_partner[acc_partner['ref']] = acc_partner['id']
        partner_user_fields = [
            'write_uid',
            'create_uid',
            'user_id'
        ]
        next_id = int(self.get_highest_id()) + 1
        for partner in all_crm_partner:
            if existing_partner.get(partner['ref'], False):
                partners_mapping[partner['id']] = existing_partner.get(partner['ref'])
                partners_to_update.append(partner)
            else:
                for field in partner_user_fields:
                    if partner[field] in user_mapping_dict:
                        partner[field] = user_mapping_dict[partner[field]]
                partners_mapping[partner['id']] = next_id
                partner['id'] = next_id
                crm_partner_toinsert.append(tuple([partner[k] for k in partner]))
                next_id += 1

        # Insert partner
        if crm_partner_toinsert:
            ins_query = crm.prepare_insert(crm_partner_toinsert, all_crm_partner[0].keys())
            query = self.db.cursor.mogrify(ins_query, crm_partner_toinsert).decode('utf8')
            self.db.cursor.execute(query)
            self.set_highest_id(next_id)

        # Update partner
        update_queries = []
        for partner_update in partners_to_update:
            update_query = crm.prepare_update(partner_update)
            ref = partner_update['ref']
            del partner_update['ref']
            vals = [v for k, v in partner_update.items() if k not in self.noupdate_fields]
            vals.append(ref)
            update_queries.append(self.db.cursor.mogrify(update_query, vals).decode('utf8'))

        if update_queries:
            chunks = [tuple(update_queries[x:x + 10000]) for x in range(0, len(update_queries), 10000)]
            for chunk in chunks:
                self.db.cursor.execute(';'.join(chunk))

        self.store_mapping_table(partners_mapping)
        self.db.close()
