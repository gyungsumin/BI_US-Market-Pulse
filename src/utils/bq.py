def run_query(client, query_path):
    with open(query_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    job = client.query(sql)
    job.result()
