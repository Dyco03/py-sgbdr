import shlex

args_str = 'age = 32, name = john'
result = shlex.split(args_str.replace(',', ' , ').replace('(', ' ( ').replace(')', ' ) '))

print(result)
