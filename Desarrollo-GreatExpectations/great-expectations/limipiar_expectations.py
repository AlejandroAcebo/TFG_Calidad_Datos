import os

file1 = "./gx/great_expectations.yml"
file2 = "./gx/checkpoints/Checkpoint.json"
file3 = "./gx/expectations/Suite.json"
file4 = "./gx/validation_definitions/Validation Definition.json"
file5 = "./gx/uncommitted/config_variables.yml"

files = [file1, file2, file3, file4, file5]


for file_path in files:
    print(file_path)
    if os.path.exists(file_path):
        os.remove(file_path)
