from ..base_master_data import BaseMasterData


class CrmTagRelAfterMigrate(BaseMasterData):
    _name = 'crm_tag_rel'

    def get_crm_data(self):
        self.crm.cursor.execute("""
        SELECT * FROM crm_tag_rel
        EXCEPT
        SELECT *
         FROM crm_tag_rel
         WHERE lead_id IN (
            SELECT
                crm_lead.id
            FROM crm_lead
            INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
            WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
            AND partner.create_uid = 211
         )""")
        data = self.crm.cursor.dictfetchall()
        return data
