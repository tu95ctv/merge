from .base import Table


class CrmLead(Table):
    _name = 'crm_lead'

    def get_crm_data(self):
        # columns = ','.join(['crm_lead.%s' % x for x in accounting_lead.columns])
        # Note: 211 is ID of API user
        self.crm.cursor.execute("""
            SELECT 
                crm_lead.*
            FROM crm_lead 
            INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
            WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
            AND partner.create_uid = 211
        """)
        all_crm_leads = self.crm.cursor.dictfetchall()
        return all_crm_leads

    def migrate(self, clear_acc_data=True):
        all_crm_leads = self.get_crm_data()

        # Get user mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        # Get Partner mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_partner_mapping")
        partner_mapping = self.accounting.cursor.dictfetchall()
        partner_mapping_dict = {x['crm_id']: x['accounting_id'] for x in partner_mapping}

        leads_toinsert = []
        # next_id = int(self.get_highest_id()) + 1
        for lead in all_crm_leads:
            if lead['partner_id'] in partner_mapping_dict:
                lead['partner_id'] = partner_mapping_dict[lead['partner_id']]
            for f in ['user_id', 'create_uid', 'write_uid']:
                if lead[f] in user_mapping_dict:
                    lead[f] = user_mapping_dict[lead[f]]
            # Ugly hack to map team_id
            if lead['team_id'] == 51:
                lead['team_id'] = 52
            # lead['id'] = next_id
            # next_id += 1
            leads_toinsert.append(tuple(lead[k] for k in lead))
        # Free some memory
        partner_mapping_dict.clear()
        del partner_mapping
        # Split data to 10k each chunk
        # TODO: use multiprocess to speed up insert data
        chunks = [tuple(leads_toinsert[x:x + 10000]) for x in range(0, len(leads_toinsert), 10000)]
        for i, chunk in enumerate(chunks):
            ins_query = self.prepare_insert(chunk, lead.keys())
            query = self.accounting.cursor.mogrify(ins_query, chunk).decode('utf8')
            self.accounting.cursor.execute(query)
        # self.set_highest_id(next_id)
        self.accounting.close()
