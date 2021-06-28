from .base_master_data import BaseMasterData


class CrmTeam(BaseMasterData):
    _name = 'crm_team'

    _update_key = 'id'

    def get_noupdate_fields(self):
        res = super(CrmTeam, self).get_noupdate_fields()
        res.extend([
            'alias_id',
        ])
        return res

    def migrate(self, clear_acc_data=False):
        datas = self.get_crm_data()
        # Get Users mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        user_mapping = self.accounting.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in user_mapping}
        self.accounting.cursor.execute("SELECT * FROM %s" % self._name)
        for line in datas:
            for f in ['user_id', 'create_uid', 'write_uid']:
                if f in line and line[f] in user_mapping_dict:
                    line[f] = user_mapping_dict[line[f]]
        update_queries = []
        for team in datas:
            update_query = self.prepare_update(team)
            id = team['id']
            del team['id']
            vals = [v for k, v in team.items() if k not in self.noupdate_fields]
            if id == 51:
                id = 52
            vals.append(id)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))
        if update_queries:
            self.accounting.cursor.execute(';'.join(update_queries))
        self.accounting.close()
