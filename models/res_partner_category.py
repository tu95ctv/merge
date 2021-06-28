from .base_master_data import BaseMasterData


class ResPartnerCategory(BaseMasterData):
    _name = 'res_partner_category'

    def get_crm_data(self):
        self.crm.cursor.execute("""SELECT * FROM res_partner_category WHERE id > 20""")
        data = self.crm.cursor.dictfetchall()
        return data
