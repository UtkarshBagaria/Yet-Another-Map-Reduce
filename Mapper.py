import sys
# print("In mapper")
for line in sys.stdin:
    o = line.strip().split(" ")
    for i in o:
        print(f"{i},1")
