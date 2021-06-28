from .base import Table


class ResPartnerResPartnerCategoryRel(Table):
    _name = 'res_partner_res_partner_category_rel'

    def get_crm_data(self):
        # Note: 211 is ID of API user
        self.crm.cursor.execute("""
                SELECT 
                    *
                FROM res_partner_res_partner_category_rel
                WHERE partner_id IN (
                    SELECT 
                    id 
                    FROM res_partner 
                    WHERE company_type ='employer' AND ref IS NOT NULL AND create_uid = 211
                ) 
            """)
        datas = self.crm.cursor.dictfetchall()
        return datas

    def migrate(self, clear_acc_data=True):
        datas = self.get_crm_data()
        # Get Partner mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_partner_mapping")
        partner_mapping = self.accounting.cursor.dictfetchall()
        partner_mapping_dict = {x['crm_id']: x['accounting_id'] for x in partner_mapping}
        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        current_rels = {(x['partner_id'], x['category_id']): 1 for x in self.accounting.cursor.dictfetchall()}
        toinsert = []
        # next_id = int(self.get_highest_id()) + 1
        for line in datas:
            if line['partner_id'] in partner_mapping_dict:
                line['partner_id'] = partner_mapping_dict[line['partner_id']]
            if (line['partner_id'], line['category_id']) not in current_rels:
                toinsert.append(tuple(line[k] for k in line))
        # Free some memory
        partner_mapping_dict.clear()
        del partner_mapping
        ins_query = self.prepare_insert(toinsert, line.keys())
        query = self.accounting.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.accounting.cursor.execute(query)
        self.accounting.close()
