import sys
import struct

target_folder = sys.argv[1]
print("analyzing folder:", target_folder)

with open(target_folder + "/objects.dat", "rb") as file:
	while True:
		t = file.read(1)
		if t == b"":
			break
		print(t, *struct.unpack("<IIII", file.read(16)))

"""
with open(target_folder + "/data.dat", "rb") as file:
	while True:
		t = file.read(1)
		if t == b"":
			break
		print(t, *struct.unpack("<IIII", file.read(16)))
"""