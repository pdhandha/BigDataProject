import matplotlib.pyplot as plt

file = open("../Spark Scripts/offense_description.out/part-00000","r")

plt.interactive(False)
offense_desc = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    offense_desc.append(x)
    no_of_crime.append(int(y))
print(dict)
print(no_of_crime)
print(offense_desc)
num=range(1,len(offense_desc)+1)

plt.rcParams.update({'font.size': 9})
plt.bar(num, no_of_crime, align='center', alpha=0.5, color = 'r')

plt.xticks(num, offense_desc, rotation = "vertical")

plt.gcf().subplots_adjust(bottom = 0.5)

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+5000)
plt.xlim(0, max(num)+1)

# naming the x axis
plt.xlabel('Offence Description')
# naming the y axis
plt.ylabel('Brooklyn Crime Frequency')

# giving a title to my graph
plt.title('Offense frequency in Brooklyn')

plt.grid()
# function to show the plot
plt.show()


