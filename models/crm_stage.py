from .base_master_data import BaseMasterData


class CrmStage(BaseMasterData):
    _name = 'crm_stage'

    _update_key = 'id'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM crm_stage")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=False):
        datas = self.get_crm_data()
        update_queries = []
        for stage in datas:
            update_query = self.prepare_update(stage)
            login = stage['id']
            del stage['id']
            vals = [v for k, v in stage.items() if k not in self.noupdate_fields]
            vals.append(login)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))
        if update_queries:
            self.accounting.cursor.execute(';'.join(update_queries))
        self.accounting.close()
