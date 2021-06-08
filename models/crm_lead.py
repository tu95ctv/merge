from .base import Table


class CrmLead(Table):
    _name = 'crm_lead'

    def migrate(self, crm_datas):

        all_crm_leads = crm_datas
        self.db.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.db.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

        self.db.cursor.execute("SELECT * FROM res_partner_mapping")
        partner_mapping = self.db.cursor.dictfetchall()
        partner_mapping_dict = {x['crm_id']: x['accounting_id'] for x in partner_mapping}

        leads_toinsert = []
        for lead in all_crm_leads:
            if lead['partner_id'] in partner_mapping_dict:
                lead['partner_id'] = partner_mapping_dict[lead['partner_id']]
            if lead['user_id'] in user_mapping_dict:
                lead['user_id'] = user_mapping_dict[lead['user_id']]
            if lead['create_uid'] in user_mapping_dict:
                lead['create_uid'] = user_mapping_dict[lead['create_uid']]
            if lead['write_uid'] in user_mapping_dict:
                lead['write_uid'] = user_mapping_dict[lead['write_uid']]
            if lead['team_id'] == 51:
                lead['team_id'] = 52
            leads_toinsert.append([lead[k] for k in lead])
        partner_mapping_dict.clear()
        del partner_mapping
        chunks = [leads_toinsert[x:x + 100] for x in range(0, len(leads_toinsert), 100)]
        for chunk in chunks:
            ins_query = self.prepare_insert(chunk)
            query = self.db.cursor.mogrify(ins_query, chunk).decode('utf8')
            self.db.cursor.execute(query)
            self.db.conn.commit()
        self.db.close()
