import sqlite3
conn = sqlite3.connect('mutomo.sqlite')
cur = conn.cursor()
cur.execute('select distinct status from orders')
rows = cur.fetchall()
for row in rows:
    print(f"Status: {row[0]} | Hex: {row[0].encode('utf-8') if row[0] else 'None'}")
conn.close()
