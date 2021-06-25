from .base import Table


class UsersCrmTeam(Table):
    _name = 'users_crm_team'

    def migrate(self, datas, crm, clear_acc_data=True):
        # Get Users mapping datas
        self.db.cursor.execute("SELECT * FROM res_users_mapping")
        user_mapping = self.db.cursor.dictfetchall()
        user_mapping = {x['crm_id']: x['accounting_id'] for x in user_mapping}
        self.db.cursor.execute("SELECT * FROM %s" % self._name)
        for line in datas:
            if line['user_id'] in user_mapping:
                line['user_id'] = user_mapping[line['user_id']]
        current_rels = {(x['user_id'], x['team_id']): 1 for x in self.db.cursor.dictfetchall()}
        toinsert = []
        for line in datas:
            if line['user_id'] in user_mapping:
                line['user_id'] = user_mapping[line['user_id']]
            if (line['user_id'], line['team_id']) not in current_rels:
                toinsert.append(tuple(line[k] for k in line))
        ins_query = crm.prepare_insert(toinsert, line.keys())
        query = self.db.cursor.mogrify(ins_query, toinsert).decode('utf8')
        self.db.cursor.execute(query)
        self.db.close()
