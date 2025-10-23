from dataloader import get_results

with open("dummy-request.sql", "r") as f:
    sql = f.read()

print(get_results(sql))