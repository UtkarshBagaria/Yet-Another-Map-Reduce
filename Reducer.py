import sys

# query_word = sys.argv[1]
word=""
count = 1
for line in sys.stdin:
    line = line.strip()
    current_word, c = line.split(",")
    if word !=current_word:
        if(word !=""):
            print(f"{word} {count}")
            count=1
            word=current_word
        else:
            count=1
            word=current_word
    else:
        count=count+1
print(f"{word} {count}")