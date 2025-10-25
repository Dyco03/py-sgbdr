import pandas as pd
df = pd.DataFrame([
    {'nom': 'Alice', 'age': 25},
    {'nom': 'Bob', 'age': 30}
])

query_str = "age == 25 and nom == 'Alice'"
mask = df.eval(query_str)
print(df[mask])