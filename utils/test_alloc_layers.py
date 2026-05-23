
from posmgr2 import AllocNode
import json

v = AllocNode('test')
v.cash = 123456
v.add_position('IBM', 1000)
v.add_target('IBM',40)

print('v')
print( json.dumps(v.to_dict(), indent=4) )

#v.update_layer(self, symbol, amt, price, order_info):
v.update_layer('IBM', 100, 10, 'layer1')
v.update_layer('IBM', 100, 20, 'layer1')

print('v2')
print( json.dumps(v.to_dict(), indent=4) )

m = v.to_dict()

j = AllocNode('test2')
j.from_dict(m)

j.update_layer('IBM', 100, 10, 'layer1')

print('j')
print( json.dumps(j.to_dict(), indent=4) )

w = j.copy()
w.update_layer('IBM', 100, 20, 'layer2')
w.update_layer('IBM''', 50, 7, 'layer2')

print('w')
print( json.dumps(w.to_dict(), indent=4) )

w.update_durations()
print('w - updated durations')
print( json.dumps(w.to_dict(), indent=4) )

