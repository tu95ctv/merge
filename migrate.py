from tools import config
from db import Database
from table import Table
from psycopg2.extensions import adapt

crm = Database(config.options['CRM'])
accounting = Database(config.options['ACCOUNTING'])

def migrate_sales_team():
    pass

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
    # accounting.cursor.execute(ins_query)
    accounting.close()


def migrate_partner():
    partners_mapping = {}
    crm_partner = Table('res_partner', crm)
    accounting_partner = Table('res_partner', accounting)
    accounting_partner.init_mapping_table()

    accounting.cursor.execute("SELECT * FROM res_users_mapping")
    users_mapping = accounting.cursor.dictfetchall()
    user_mapping_dict = {x['crm_id']: x['accounting_id'] for x in users_mapping}

    migrate_company_partner(crm_partner, accounting_partner, user_mapping_dict, partners_mapping)
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
    accounting.cursor.execute("SELECT * FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL")
    all_accounting_partner = accounting.cursor.dictfetchall()
    crm.cursor.execute("SELECT * FROM res_partner WHERE company_type ='employer' AND ref IS NOT NULL")
    all_crm_partner = crm.cursor.dictfetchall()
    for acc_partner in all_accounting_partner:
        if acc_partner['ref']:
            existing_partner[acc_partner['ref']] = acc_partner['id']
    partner_user_fields = [
        'write_uid',
        'create_uid',
        'user_id'
    ]
    for crm_partner in all_crm_partner:
        if existing_partner.get(crm_partner['ref'], False):
            partners_mapping[crm_partner['id']] = existing_partner.get(crm_partner['ref'])
        else:
            for field in partner_user_fields:
                if crm_partner[field] in user_mapping_dict:
                    crm_partner[field] = user_mapping_dict[crm_partner[field]]
            if crm_partner['parent_id'] in partners_mapping:
                crm_partner['parent_id'] = partners_mapping[crm_partner['parent_id']]
            crm_partner_toinsert.append([crm_partner[k] for k in crm_partner])

    ins_query, mapped_ids, lines = crm_partner.prepare_insert(crm_partner_toinsert)
    partners_mapping.update(mapped_ids)
    accounting_partner.store_mapping_table(partners_mapping)
    accounting.cursor.execute(ins_query, ', '. join(lines))


def migrate_leads():
    pass


if __name__ == '__main__':
    migrate_partner()
