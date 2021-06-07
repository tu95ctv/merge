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
    for acc_user in all_accounting_users:
        existing_users[acc_user['login']] = acc_user['id']
    for crm_user in all_crm_users:
        if existing_users.get(crm_user['login'], False):
            users_mapping[crm_user['id']] = existing_users.get(crm_user['login'])
    crm.cursor.execute(
        "SELECT %s FROM res_users WHERE login not in (%s)" % (crm_users.get_columns_str(), ",".join([x for x in existing_users])))
    crm_users_toinsert = crm.cursor.fetchall()
    ins_query, mapped_ids = crm_users.prepare_insert(crm_users_toinsert)

    accounting_users.store_mapping_table(mapped_ids)
    crm.cursor.execute(ins_query)
    return mapped_ids


def migrate_partner():
    crm_partner = Table('res_partner', crm)
    accounting_partner = Table('res_partner', accounting)
    all_accounting_partner = accounting_partner.select_all()
    existing_login = ["'%s'" % x['ref'] for x in all_accounting_partner]
    crm.cursor.execute("SELECT * FROM res_partner WHERE ref not in (%s)" % ",".join(existing_login))
    crm_partner_toinsert = crm.cursor.fetchall()
    ins_query, mapped_ids = crm_partner.prepare_insert(crm_partner_toinsert)
    accounting_partner.init_mapping_table()
    accounting_partner.store_mapping_table(mapped_ids)
    accounting.cursor.execute(ins_query)
    print("Done")


def migrate_leads():
    pass


if __name__ == '__main__':
    migrate_user()
