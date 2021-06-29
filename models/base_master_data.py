from .base import Table


class BaseMasterData(Table):

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM %s" % self._name)
        crm_datas = self.crm.cursor.dictfetchall()
        return crm_datas

    def migrate(self, clear_acc_data=False, set_sequence=True):
        self.logger.info("Migrating %s ...." % self._name)
        if clear_acc_data:
            self.accounting.cursor.execute("DELETE FROM %s" % self._name)
        to_inserts = self.get_crm_data()
        # Get user mapping datas
        self.accounting.cursor.execute("SELECT * FROM res_users_mapping")
        users_mapping = self.accounting.cursor.dictfetchall()
        user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}
        data = []

        current_id = max_id = int(self.get_highest_id()) if set_sequence else 0
        for cll in to_inserts:
            for f in ['user_id', 'create_uid', 'write_uid']:
                if f in cll and cll[f] in user_mapping_dict:
                    cll[f] = user_mapping_dict[cll[f]]
            data.append(tuple(cll[k] for k in cll))
            if set_sequence:
                max_id = max(cll['id'], max_id)
        if current_id != max_id:
            self.set_highest_id(current_id)
        chunks = [tuple(data[x:x + 10000]) for x in range(0, len(data), 10000)]
        for i, chunk in enumerate(chunks):
            ins_query = self.prepare_insert(chunk, to_inserts[0].keys())
            query = self.accounting.cursor.mogrify(ins_query, chunk).decode('utf8')
            self.accounting.cursor.execute(query)
        self.accounting.close()
