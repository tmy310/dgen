from dao.include import *

test_table = TestTable('localhost', 'root', '')
test_table.insert(1,2)
rows = test_table.find_all('col1')
for row in rows:
    print row
test_table.delete_by_col1(1)
