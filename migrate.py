from db import Database
from models import ResUser, ResPartner, CrmLead
from tools import config


def migrate_user():
    crm_users = ResUser(Database(config.options['CRM']))
    accounting_users = ResUser(Database(config.options['ACCOUNTING']))
    all_crm_users = crm_users.select_all()
    accounting_users.migrate(all_crm_users)


def migrate_partner():
    def map_user_partner_id():
        user_partners_dict = {}
        crm_users = ResUser(Database(config.options['CRM']))
        accounting_users = ResUser(Database(config.options['ACCOUNTING']))
        # Get all users from accounting which doesn't have partner_id
        accounting_users.db.cursor.execute("SELECT login FROM res_users WHERE partner_id IS NULL")
        user_logins = accounting_users.db.cursor.fetchall()
        # Get all users from CRM with corresponding logins
        crm_users.db.cursor.execute("SELECT login, partner_id FROM res_users WHERE login in (%s)" % user_logins)
        user_partners = crm_users.db.cursor.dictfetchall()
        # Get partner mapping data
        accounting_users.db.cursor.execute("SELECT crm_id, accounting_id FROM res_partner_mapping")
        res_partner_mapping = {x['crm_id']: x['accounting_id'] for x in accounting_users.db.cursor.dictfetchall()}

        # Map login with partner
        for line in user_partners:
            user_partners_dict[line['login']] = res_partner_mapping[line['partner_id']]
        accounting_users.update_partner_id(user_partners_dict)


    crm_partner = ResPartner(Database(config.options['CRM']))
    accounting_partner = ResPartner(Database(config.options['ACCOUNTING']))
    crm_partner.db.cursor.execute(
        "SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % crm_partner.columns_str)
    all_crm_partner = crm_partner.db.cursor.dictfetchall()
    accounting_partner.migrate(all_crm_partner)
    map_user_partner_id()
    accounting_partner.db.close()


def migrate_leads():
    crm_lead = CrmLead(Database(config.options['CRM']))
    accounting_lead = CrmLead(Database(config.options['ACCOUNTING']))
    columns = ','.join(['crm_lead.%s' % x for x in accounting_lead.columns])
    crm_lead.db.cursor.execute("""
        SELECT 
            %s
        FROM crm_lead 
        INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
        WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
    """ % columns)
    all_crm_leads = crm_lead.db.cursor.dictfetchall()
    accounting_lead.migrate(all_crm_leads)


if __name__ == '__main__':
    # migrate_user()
    # migrate_partner()
    migrate_leads()
