import csv
import os

def max_calc(file_name):
  if not os.path.isfile(file_name):
    return
  with open(file_name, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    max = 0
    min = 1000000000
    ave = 0
    count = 0
    for row in spamreader:
      if float(row[3]) > max:
        max = float(row[3])
      if float(row[3]) < min:
        min = float(row[3])
      ave += float(row[3])  
      count += 1
    print file_name, max, min, ave/count


for subdir, dirs, files in os.walk('.'):
  for d in dirs:
    max_calc(d + "/" + "merged_" + d) 
  
