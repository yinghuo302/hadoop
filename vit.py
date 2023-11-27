import re
import sys


class Node:
    pr:float
    label:str
    edges:list[str]
    weights:list[float]
    def __init__(self) -> None:
        self.pr = 0.
        self.label = ""
        self.edges = []
        self.weights = []
                

def readFromFile(pr_file:str,lpa_file:str) -> dict[str,Node]:
    items:dict[str,Node] = {}
    with open(pr_file) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            strs = re.split(r"[|,;\t]+",line)
            strs.pop()
            if not items.__contains__(strs[0]):
                items[strs[0]] = Node()
            node = items[strs[0]]
            node.pr = strs[1]
            i , size = 3,len(strs)
            while i < size:
                node.edges.append(strs[i])
                node.weights.append(strs[i+1])
                i += 2
            items[strs[0]] = node
    with open(lpa_file) as f:
        while True:
            line = f.readline()
            if line == "":
                break
            strs = re.split(r"[|\t]+",line)
            strs.pop()
            i ,size = 1,len(strs)
            while i< size:
                items[strs[i]].label = strs[0]
                i += 1
            
    return items
        
def generateCSV(items:dict[str,Node]):
    with open("node.csv","w+") as f:
        f.write("Id,Label,PageRank,Tag\n")
        for item in items.items():
            f.write(f"{item[0]},{item[0]},{item[1].pr},{item[1].label}\n")
    with open("edges.csv","w+") as f:
        f.write("Source,Target,Weight,Tag\n")
        for item in items.items():
            size = len(item[1].edges)
            for i in range(size):
                f.write(f"{item[0]},{item[1].edges[i]},{item[1].weights[i]},{item[1].label}\n")
    

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: pr_file lpa_file")
        exit(0)
    items = readFromFile(sys.argv[1],sys.argv[2])
    generateCSV(items)