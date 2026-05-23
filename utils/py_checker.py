import py_compile
import sys

file_path = sys.argv[1]
print(f'checking: {file_path}')

try:
    py_compile.compile(file_path, doraise=True)
    print(f"Syntax of {file_path} is valid.")
except py_compile.PyCompileError as e:
    print(f"Syntax error in {file_path}:\n{e}")

