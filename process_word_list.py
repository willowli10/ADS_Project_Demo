import random

unique_list = []

with open("client/10000.txt", encoding="utf-8") as f:
    for line in f:
        word = line.strip()
        unique_list.append(word)

with open("client/10000_rep.txt", mode='w', encoding="utf-8") as f:
    for i in range(10000):
        f.write(unique_list[random.randint(0, len(unique_list)-1)])
        f.write("\n")