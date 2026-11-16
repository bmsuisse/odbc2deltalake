import importlib

try:
    importlib.import_module("deltalake")

    raise ImportError("I should not be able to import deltalake")
    exit(-1)
except ImportError:
    print("I could not import deltalake, which is good")

print("I could import ")
