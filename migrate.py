from tools import config
from db import Database
from table import Table


crm = Database(config.options['CRM'])
accounting = Database(config.options['ACCOUNTING'])

def migrate_user():
    crm_users = Table('res_users', crm)
    accounting_users = Table('res_users', accounting)
    accounting_users.init_mapping_table()
    all_accounting_users = accounting_users.select_all()
    all_crm_users = crm_users.select_all()
    users_mapping = {}
    existing_users = {}
    crm_users_toinsert = []
    for acc_user in all_accounting_users:
        existing_users[acc_user['login']] = acc_user['id']
    for crm_user in all_crm_users:
        if existing_users.get(crm_user['login'], False):
            users_mapping[crm_user['id']] = existing_users.get(crm_user['login'])
        else:
            # Ugly hack to map sale_team_id
            if crm_user['sale_team_id'] == 51:
                crm_user['sale_team_id'] = 52
            crm_users_toinsert.append([crm_user[k] for k in crm_user])
    ins_query, mapped_ids = crm_users.prepare_insert(crm_users_toinsert)
    users_mapping.update(mapped_ids)
    accounting_users.store_mapping_table(users_mapping)
    accounting.cursor.execute(ins_query)
    accounting.close()


def migrate_partner():
    partners_mapping = {}
    crm_partner = Table('res_partner', crm)
    accounting_partner = Table('res_partner', accounting)
    accounting_partner.init_mapping_table()

    accounting.cursor.execute("SELECT * FROM res_users_mapping")
    users_mapping = accounting.cursor.dictfetchall()
    user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

    # migrate_company_partner(crm_partner, accounting_partner, user_mapping_dict, partners_mapping)
    migrate_employer_partner(crm_partner, accounting_partner, user_mapping_dict, partners_mapping)
    # accounting.cursor.execute(ins_query)
    accounting.close()


def migrate_company_partner(crm_partner, accounting_partner, user_mapping_dict, partners_mapping):
    existing_partner = {}
    crm_partner_toinsert = []
    accounting.cursor.execute("SELECT * FROM res_partner WHERE company_type ='company' AND vat IS NOT NULL")
    all_accounting_partner = accounting.cursor.dictfetchall()
    crm.cursor.execute("SELECT * FROM res_partner WHERE company_type ='company' AND vat IS NOT NULL")
    all_crm_partner = crm.cursor.dictfetchall()
    for acc_partner in all_accounting_partner:
        if acc_partner['vat']:
            existing_partner[acc_partner['vat']] = acc_partner['id']
    partner_user_fields = [
        'write_uid',
        'create_uid',
        'user_id'
    ]
    for partner in all_crm_partner:
        if existing_partner.get(partner['vat'], False):
            partners_mapping[partner['id']] = existing_partner.get(partner['vat'])
        else:
            for field in partner_user_fields:
                if partner[field] in user_mapping_dict:
                    partner[field] = user_mapping_dict[partner[field]]
            crm_partner_toinsert.append([partner[k] for k in partner])

    ins_query, mapped_ids, lines = crm_partner.prepare_insert(crm_partner_toinsert)
    partners_mapping.update(mapped_ids)
    accounting_partner.store_mapping_table(partners_mapping)
    query = accounting.cursor.mogrify(ins_query, lines).decode('utf8')
    accounting.cursor.execute(query)


def migrate_employer_partner(crm_partner, accounting_partner, user_mapping_dict, partners_mapping):
    existing_partner = {}
    crm_partner_toinsert = []
    accounting.cursor.execute("SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % accounting_partner.columns_str)
    all_accounting_partner = accounting.cursor.dictfetchall()
    crm.cursor.execute("SELECT %s FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL" % crm_partner.columns_str)
    all_crm_partner = crm.cursor.dictfetchall()
    for acc_partner in all_accounting_partner:
        if acc_partner['ref']:
            existing_partner[acc_partner['ref']] = acc_partner['id']
    partner_user_fields = [
        'write_uid',
        'create_uid',
        'user_id'
    ]
    for partner in all_crm_partner:
        if existing_partner.get(partner['ref'], False):
            partners_mapping[partner['id']] = existing_partner.get(partner['ref'])
        else:
            for field in partner_user_fields:
                if partner[field] in user_mapping_dict:
                    partner[field] = user_mapping_dict[partner[field]]
            crm_partner_toinsert.append([partner[k] for k in partner])

    ins_query, mapped_ids, lines = crm_partner.prepare_insert(crm_partner_toinsert)
    partners_mapping.update(mapped_ids)
    accounting_partner.store_mapping_table(partners_mapping)
    query = accounting.cursor.mogrify(ins_query, lines).decode('utf8')
    accounting.cursor.execute(query)


def migrate_leads():
    crm_lead = Table('crm_lead', crm)
    accounting_lead = Table('res_users', accounting)
    crm.cursor.execute("""
        SELECT 
            crm_lead.*
        FROM crm_lead 
        INNER JOIN res_partner partner ON partner.id = crm_lead.partner_id
        WHERE partner.company_type ='employer' AND partner.ref IS NOT NULL
    """)
    all_crm_leads = crm.cursor.dictfetchall()
    accounting.cursor.execute("SELECT * FROM res_users_mapping")
    users_mapping = accounting.cursor.dictfetchall()
    user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

    accounting.cursor.execute("SELECT * FROM res_partner_mapping")
    partner_mapping = accounting.cursor.dictfetchall()
    partner_mapping_dict = {x['crm_id']: x['accounting_id'] for x in partner_mapping}

    leads_toinsert = []
    for lead in all_crm_leads:
        if lead['partner_id'] in partner_mapping_dict:
            lead['partner_id'] = partner_mapping_dict[lead['partner_id']]
        if lead['user_id'] in user_mapping_dict:
            lead['user_id'] = user_mapping_dict[lead['user_id']]
        if lead['create_uid'] in user_mapping_dict:
            lead['create_uid'] = user_mapping_dict[lead['create_uid']]
        if lead['write_uid'] in user_mapping_dict:
            lead['write_uid'] = user_mapping_dict[lead['write_uid']]
        if lead['team_id'] == 51:
            lead['sale_team_id'] = 52
        leads_toinsert.append([lead[k] for k in lead])
    partner_mapping_dict.clear()
    del partner_mapping
    ins_query, mapped_ids, lines = crm_lead.prepare_insert(leads_toinsert)
    query = accounting.cursor.mogrify(ins_query, lines).decode('utf8')
    accounting.cursor.execute(query)



    accounting.close()


if __name__ == '__main__':
    migrate_leads()
