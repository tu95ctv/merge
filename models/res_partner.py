from .base import Table


class ResPartner(Table):
    _name = 'res_partner'

    def __init__(self, db):
        super(ResPartner, self).__init__(db)
        self.columns.remove('parent_id')
        self.columns.remove('commercial_partner_id')
        self.columns_str = self.get_columns_str()

    def migrate(self, crm_datas):
        self.init_mapping_table()
        partners_mapping = {}

        self.db.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.db.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        existing_partner = {}
        crm_partner_toinsert = []
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
            else:
                for field in partner_user_fields:
                    if partner[field] in user_mapping_dict:
                        partner[field] = user_mapping_dict[partner[field]]
                partners_mapping[partner['id']] = next_id
                partner['id'] = next_id
                crm_partner_toinsert.append(tuple([partner[k] for k in partner]))
        ins_query = self.prepare_insert(crm_partner_toinsert)
        self.store_mapping_table(partners_mapping)
        query = self.db.cursor.mogrify(ins_query, crm_partner_toinsert).decode('utf8')
        self.db.cursor.execute(query)
        self.db.close()
