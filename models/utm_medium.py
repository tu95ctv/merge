from .base_master_data import BaseMasterData


class UtmMedium(BaseMasterData):
    _name = 'utm_medium'

    def get_crm_data(self):
        self.crm.cursor.execute("""SELECT * FROM utm_medium WHERE id > 14""")
        data = self.crm.cursor.dictfetchall()
        return data
