from .base import Table


class ResPartner(Table):
    _name = 'res_partner'

    def __init__(self, db):
        super(ResPartner, self).__init__(db)
        self.columns.remove('parent_id')
        self.columns.remove('commercial_partner_id')
        self.columns_str = self.get_columns_str()

    def get_noupdate_fields(self):
        res = super(ResPartner, self).get_noupdate_fields()
        res.extend([
            'company_id'
        ])
        return res

    def migrate(self, crm_datas, crm):
        partners_to_update = []
        partners_mapping = {}
        existing_partner = {}
        crm_partner_toinsert = []

        self.init_mapping_table()

        self.db.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.db.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.db.cursor.execute("SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % self.columns_str)
        all_accounting_partner = self.db.cursor.dictfetchall()
        all_crm_partner = crm_datas
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
        if crm_partner_toinsert:
            ins_query = crm.prepare_insert(crm_partner_toinsert)
            self.store_mapping_table(partners_mapping)
            query = self.db.cursor.mogrify(ins_query, crm_partner_toinsert).decode('utf8')
            self.db.cursor.execute(query)
            self.set_highest_id(next_id)

        update_queries = []
        for partner in partners_to_update:
            update_query = self.prepare_update(partner)
            login = partner['ref']
            del partner['ref']
            vals = [v for k, v in partner.items() if k not in self.noupdate_fields]
            vals.append(login)
            update_queries.append(self.db.cursor.mogrify(update_query, vals).decode('utf8'))
        if update_queries:
            self.db.cursor.execute(';'.join(update_queries))

