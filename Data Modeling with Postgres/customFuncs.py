def sql(qry_string):
    cur.execute(qry_string)
    results = cur.fetchone()
    while results:
        print(results)
        results=cur.fetchone()