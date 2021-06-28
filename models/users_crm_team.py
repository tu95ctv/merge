from .base_master_data import BaseMasterData


class UsersCrmTeam(BaseMasterData):
    _name = 'users_crm_team'

    def migrate(self, clear_acc_data=False):
        datas = self.get_crm_data()
        # Get Users mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        user_mapping = self.accounting.cursor.dictfetchall()
        user_mapping = {x['crm_id']: x['accounting_id'] for x in user_mapping}
        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        for line in datas:
            if line['user_id'] in user_mapping:
                line['user_id'] = user_mapping[line['user_id']]
        current_rels = {(x['user_id'], x['team_id']): 1 for x in self.accounting.cursor.dictfetchall()}
        toinsert = []
        for line in datas:
            if line['user_id'] in user_mapping:
                line['user_id'] = user_mapping[line['user_id']]
            if (line['user_id'], line['team_id']) not in current_rels:
                toinsert.append(tuple(line[k] for k in line))
        ins_query = self.prepare_insert(toinsert, line.keys())
        query = self.accounting.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.accounting.cursor.execute(query)
        self.accounting.close()
