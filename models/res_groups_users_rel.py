from .base import Table


class ResGroupsUsersRel(Table):
    _name = 'res_groups_users_rel'

    def migrate(self, datas, crm, clear_acc_data=True):
        # Get Partner mapping datas
        self.db.cursor.execute("SELECT * FROM res_partner_mapping")
        partner_mapping = self.db.cursor.dictfetchall()
        partner_mapping_dict = {x['crm_id']: x['accounting_id'] for x in partner_mapping}
        self.db.cursor.execute("SELECT * FROM %s" % self._name)
        current_rels = {(x['partner_id'], x['category_id']): 1 for x in self.db.cursor.dictfetchall()}
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
        ins_query = crm.prepare_insert(toinsert, line.keys())
        query = self.db.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.db.cursor.execute(query)
        self.db.close()
