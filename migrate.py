from db import Database
from models import ResUser, ResPartner, CrmLead
from tools import config


def migrate_user():
    crm_users = ResUser(Database(config.options['CRM']))
    accounting_users = ResUser(Database(config.options['ACCOUNTING']))
    all_crm_users = crm_users.select_all()
    accounting_users.migrate(all_crm_users)


def migrate_partner():
    crm_partner = ResPartner(Database(config.options['CRM']))
    accounting_partner = ResPartner(Database(config.options['ACCOUNTING']))
    crm_partner.db.cursor.execute(
        "SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % crm_partner.columns_str)
    all_crm_partner = crm_partner.db.cursor.dictfetchall()
    accounting_partner.migrate(all_crm_partner)


def migrate_leads():
    crm_lead = CrmLead(Database(config.options['CRM']))
    accounting_lead = CrmLead(Database(config.options['ACCOUNTING']))
    crm_lead.db.cursor.execute("""
        SELECT 
            crm_lead.*
        FROM crm_lead 
        INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
        WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
    """)
    all_crm_leads = crm_lead.db.cursor.dictfetchall()
    accounting_lead.migrate(all_crm_leads)


if __name__ == '__main__':
    # migrate_user()
    # migrate_partner()
    migrate_leads()
