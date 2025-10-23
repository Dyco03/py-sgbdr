import shlex
args_str = "id INT,name VARCHAR"
resultat = shlex.split(args_str.replace(',', ' , ').replace('(', ' ( ').replace(')', ' ) '))
print(args_str.split(","))