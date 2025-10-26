import pandas as pd
df = pd.DataFrame([
    {'nom': 'Alice', 'age': 25},
    {'nom': 'Bob', 'age': 30}
])
query_string = "SELECT nom, age FROM users INNER JOIN orders ON users.id = orders.user_id WHERE age > 20 ORDER BY age DESC LIMIT 10;"
import re
select_join_match = re.match(
            r'SELECT\s+(.*?)\s+FROM\s+(\w+)\s+((?:INNER|LEFT|RIGHT)\s+JOIN\s+\w+\s+ON\s+.*?)(?:\s+WHERE\s+(.*?))?(?:\s+ORDER\s+BY\s+(.*?))?(?:\s+LIMIT\s+(\d+))?;',
            query_string, 
            re.IGNORECASE
        )

print(select_join_match.groups())