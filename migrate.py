import logging

from db import Database
from models import *
from tools import config

logger = logging.getLogger('SIEUVIET_MIGRATE')


def migrate_user():
    logger.info("Migrating res.users ....")
    crm_users = ResUser(Database(config.options['CRM']))
    accounting_users = ResUser(Database(config.options['ACCOUNTING']))
    all_crm_users = crm_users.select_all()
    accounting_users.migrate(all_crm_users, crm_users)


def migrate_partner():
    logger.info("Migrating res.partner ....")
    crm_partner = ResPartner(Database(config.options['CRM']))
    accounting_partner = ResPartner(Database(config.options['ACCOUNTING']))
    # Note: 211 is ID of API user
    crm_partner.db.cursor.execute(
        "SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL AND create_uid = 211" % crm_partner.columns_str)
    all_crm_partner = crm_partner.db.cursor.dictfetchall()

    # Get partner that link to user from CRM side
    accounting_partner.db.cursor.execute("SELECT login FROM res_users WHERE partner_id IS NULL")
    logins = tuple(x[0] for x in accounting_partner.db.cursor.fetchall())
    crm_partner.db.cursor.execute("SELECT partner_id FROM res_users WHERE login in %s" % (logins,))
    user_partner = tuple(x[0] for x in crm_partner.db.cursor.fetchall())
    crm_partner.db.cursor.execute(
        "SELECT %s FROM res_partner WHERE id IN %s" % (crm_partner.columns_str, user_partner))
    all_crm_partner.extend(crm_partner.db.cursor.dictfetchall())
    accounting_partner.migrate(all_crm_partner, crm_partner)


def map_user_partner_id():
    logger.info("Mapping partner_id for users ....")
    user_partners_dict = {}
    crm_users = ResUser(Database(config.options['CRM']))
    accounting_users = ResUser(Database(config.options['ACCOUNTING']))
    # Get all users from accounting which doesn't have partner_id
    accounting_users.db.cursor.execute("SELECT login FROM res_users WHERE partner_id IS NULL")
    user_logins = tuple(x[0] for x in accounting_users.db.cursor.fetchall())
    # Get all users from CRM with corresponding logins
    crm_users.db.cursor.execute("SELECT login, partner_id FROM res_users WHERE login in %s" % (user_logins,))
    user_partners = crm_users.db.cursor.dictfetchall()
    # Get partner mapping data
    accounting_users.db.cursor.execute("SELECT crm_id, accounting_id FROM res_partner_mapping")
    res_partner_mapping = {x['crm_id']: x['accounting_id'] for x in accounting_users.db.cursor.dictfetchall()}
    # Map login with partner
    for line in user_partners:
        user_partners_dict[line['login']] = res_partner_mapping[line['partner_id']]
    accounting_users.update_partner_id(user_partners_dict)


def migrate_leads():
    logger.info("Migrating crm_lead ....")
    crm_lead = CrmLead(Database(config.options['CRM']))
    accounting_lead = CrmLead(Database(config.options['ACCOUNTING']))
    columns = ','.join(['crm_lead.%s' % x for x in accounting_lead.columns])
    # Note: 211 is ID of API user
    crm_lead.db.cursor.execute("""
        SELECT 
            %s
        FROM crm_lead 
        INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
        WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
        AND partner.create_uid = 211
    """ % columns)
    all_crm_leads = crm_lead.db.cursor.dictfetchall()
    accounting_lead.migrate(all_crm_leads, crm_lead)


def migrate_master_data(table):
    logger.info("Migrating %s ...." % table)
    crm = Table(Database(config.options['CRM']), table)
    accounting = Table(Database(config.options['ACCOUNTING']), table)
    crm.db.cursor.execute("SELECT * FROM %s" % table)
    data = crm.db.cursor.dictfetchall()
    accounting.migrate(data, crm)


def migrate_utm_medium():
    logger.info("Migrating utm_medium ....")
    crm = Table(Database(config.options['CRM']), 'utm_medium')
    accounting = Table(Database(config.options['ACCOUNTING']), 'utm_medium')
    crm.db.cursor.execute("""SELECT * FROM utm_medium WHERE id > 14""")
    data = crm.db.cursor.dictfetchall()
    accounting.migrate(data, crm, clear_acc_data=False)


def migrate_crm_tag_rel():
    logger.info("Migrating crm_tag_rel ....")
    crm = Table(Database(config.options['CRM']), 'crm_tag_rel')
    accounting = Table(Database(config.options['ACCOUNTING']), 'crm_tag_rel')
    crm.db.cursor.execute("""SELECT *
     FROM crm_tag_rel 
     WHERE lead_id IN (
        SELECT 
            crm_lead.id
        FROM crm_lead 
        INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
        WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
        AND partner.create_uid = 211
     )""")
    data = crm.db.cursor.dictfetchall()
    accounting.migrate(data, crm)


def clear_accounting_crm_lead():
    logger.info("Deleting Accounting crm_lead")
    accounting = Table(Database(config.options['ACCOUNTING']), 'crm_lead')
    accounting.db.cursor.execute("DELETE FROM crm_lead")
    accounting.db.close()


def clear_crm_lead_track_level_up():
    logger.info("Deleting Accounting crm_lead_track_level_up")
    accounting = Table(Database(config.options['ACCOUNTING']), 'crm_lead_track_level_up')
    accounting.db.cursor.execute("DELETE FROM crm_lead_track_level_up")
    accounting.db.close()


if __name__ == '__main__':
    migrate_user()
    migrate_utm_medium()
    migrate_partner()
    map_user_partner_id()
    clear_accounting_crm_lead()
    migrate_master_data('crm_lost_reason')
    migrate_master_data('crm_lead_lost')
    clear_crm_lead_track_level_up()
    migrate_master_data('crm_stage')
    migrate_leads()
    migrate_master_data('crm_lead_track_level_up')
    migrate_master_data('crm_tag')
    migrate_crm_tag_rel()
