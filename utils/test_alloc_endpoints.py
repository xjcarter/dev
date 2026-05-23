
import ib_endpoints2
import json

jj = ib_endpoints2.get_subaccounts()
print(json.dumps(jj, indent=4))

print('start: allocation groups:')
kk = ib_endpoints2.get_allocation_groups()
print(json.dumps(kk, indent=4))

account_list = []
for account_dict in jj.get('accounts'):
    account_list.append( dict(amount=10, name=account_dict.get("name")) )

group_def = {
        "name": "GROUP1",
        "accounts": account_list,
        "default_method": "S"
}
print('creating group:')
print(json.dumps(group_def, indent=4))

print('\nposting group')
ff = ib_endpoints2.create_allocation_group(group_def)
print('reply')
print(json.dumps(ff, indent=4))

print('new: allocation groups:')
kk = ib_endpoints2.get_allocation_groups()
print(json.dumps(kk, indent=4))

print('checking single group')
kk = ib_endpoints2.get_allocation_group(group_id='GROUP1')
print(json.dumps(kk, indent=4))

##print('delete group')
##mm = ib_endpoints2.delete_allocation_group(group_id='GROUP1')
##print(json.dumps(mm, indent=4))

account_list = []
for account_dict in jj['accounts']:
    account_list.append( dict(amount=99,name=account_dict.get('name')) )

group_def = {
        "name": "GROUP1",
        "accounts": account_list,
        "default_method": "S"
}
print('rewrite GROUP1')
cc = ib_endpoints2.create_allocation_group(group_def)
print(json.dumps(cc, indent=4))
print('rewritten')
qq = ib_endpoints2.get_allocation_group("GROUP1")
print(json.dumps(qq, indent=4))
