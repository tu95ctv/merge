from .base import Table


class IrModuleCategory(Table):
    _name = 'ir_module_category'
    _update_key = 'name'

    def get_crm_data(self):
        self.crm.cursor.execute("SELECT * FROM ir_module_category")
        return self.crm.cursor.dictfetchall()

    def get_accounting_data(self):
        self.accounting.cursor.execute("SELECT * FROM ir_module_category")
        return self.crm.cursor.dictfetchall()

    def migrate(self, clear_acc_data=False):
        next_id = int(self.get_highest_id()) + 1
        self.init_mapping_table()
        crm_datas = self.get_crm_data()
        accounting_datas = self.get_accounting_data()
        crm_cat_dict = {x['name']: x['id'] for x in accounting_datas}
        to_insert = []
        to_update = []
        mapping_datas = []

        for cat in crm_datas:
            if crm_cat_dict.get(cat['name'], False):
                mapping_datas[cat['id']] = crm_cat_dict[cat['name']]
                to_update.append(cat)
            else:
                mapping_datas[cat['id']] = next_id
                cat['id'] = next_id
                to_insert.append(tuple([cat[k] for k in cat]))
                next_id += 1
        # Insert partner
        if to_insert:
            ins_query = self.prepare_insert(to_insert, crm_datas[0].keys())
            query = self.accounting.cursor.mogrify(ins_query, to_insert).decode('utf8')
            self.accounting.cursor.execute(query)
            self.set_highest_id(next_id)

        # Update partner
        update_queries = []
        for cat_to_update in to_update:
            update_query = self.prepare_update(cat_to_update)
            ref = cat_to_update['name']
            del cat_to_update['name']
            vals = [v for k, v in cat_to_update.items() if k not in self.noupdate_fields]
            vals.append(ref)
            update_queries.append(self.accounting.cursor.mogrify(update_query, vals).decode('utf8'))

        if update_queries:
            chunks = [tuple(update_queries[x:x + 10000]) for x in range(0, len(update_queries), 10000)]
            for chunk in chunks:
                self.accounting.cursor.execute(';'.join(chunk))

        self.store_mapping_table(mapping_datas)
        self.accounting.close()
