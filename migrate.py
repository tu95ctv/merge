import logging

from models import *

logger = logging.getLogger('SIEUVIET_MIGRATE')


def map_user_partner_id():
    logger.info("Mapping partner_id for users ....")
    user_partners_dict = {}
    ru = ResUser()
    # Get all users from accounting which doesn't have partner_id
    ru.accounting.cursor.execute("SELECT login FROM res_users WHERE partner_id IS NULL")
    user_logins = tuple(x[0] for x in ru.accounting.cursor.fetchall())
    # Get all users from CRM with corresponding logins
    ru.crm.cursor.execute("SELECT login, partner_id FROM res_users WHERE login in %s" % (user_logins,))
    user_partners = ru.crm.cursor.dictfetchall()
    # Get partner mapping data
    ru.accounting.cursor.execute("SELECT crm_id, accounting_id FROM res_partner_mapping")
    res_partner_mapping = {x['crm_id']: x['accounting_id'] for x in ru.accounting.cursor.dictfetchall()}
    # Map login with partner
    for line in user_partners:
        user_partners_dict[line['login']] = res_partner_mapping[line['partner_id']]
    ru.update_partner_id(user_partners_dict)


def migrate_master_data(table, clear_acc_data=False, set_sequence=False):
    logger.info("Migrating %s ...." % table)
    BaseMasterData(name=table).migrate(clear_acc_data=clear_acc_data, set_sequence=set_sequence)


def clear_accounting_crm_lead():
    logger.info("Deleting Accounting crm_lead")
    cl = Table(name='crm_lead')
    cl.accounting.cursor.execute("DELETE FROM crm_lead")
    cl.accounting.close()


def migrate_crm_datas():
    # Clear all crm data from accounting side
    clear_accounting_crm_lead()
    # Migrate master data for CRM
    migrate_master_data('crm_lost_reason', clear_acc_data=True)
    migrate_master_data('crm_lead_lost', clear_acc_data=True)
    CrmStage().migrate()
    migrate_master_data('crm_tag', clear_acc_data=True)
    migrate_master_data('crm_probability', clear_acc_data=True)

    # Migrate CRM
    CrmLead().migrate()
    CrmTagRel().migrate(clear_acc_data=True, set_sequence=False)

    # Migrate Rel tables
    CrmLeadTrackLevelUp().migrate(clear_acc_data=True)
    migrate_master_data('crm_stage_sub_rel', clear_acc_data=True, set_sequence=False)
    migrate_master_data('crm_stage_lost_reason_rel', clear_acc_data=True, set_sequence=False)
    migrate_master_data('crm_stage_followup_result_rel', clear_acc_data=True, set_sequence=False)

    # Migrate Crm Team
    CrmTeam().migrate()
    UsersCrmTeam().migrate()


def migrate_partner_datas():
    UtmMedium().migrate()
    ResPartner().migrate()
    map_user_partner_id()
    ResPartnerCategory().migrate()
    ResPartnerResPartnerCategoryRel().migrate()


def migrate_user_datas():
    # ResUser().migrate()
    IrModuleCategory().migrate()
    ResGroups().migrate()
    ResGroupsUsersRel().migrate()
    ResUsersRole().migrate()
    ResUsersRoleLine().migrate()
    ResCompanyUsersRel().migrate()
    ResGroupsImplied().migrate()


if __name__ == '__main__':
    migrate_user_datas()
    migrate_partner_datas()
    migrate_crm_datas()
