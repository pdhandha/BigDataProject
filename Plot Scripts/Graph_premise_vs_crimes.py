import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

file = open("../Spark Scripts/premises_desc.out/part-00000","r")

plt.interactive(False)
place = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    place.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
print(place)
num=range(1, len(place) + 1)

plt.rcParams.update({'font.size': 8})
plt.bar(num, no_of_crime, align='center', alpha=0.5, color = 'r')

plt.xticks(num, place, rotation ="vertical")

plt.gcf().subplots_adjust(bottom = 0.4)

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+10000)
plt.xlim(0, max(num)+1)

# naming the x axis
plt.xlabel('Offence Place')
# naming the y axis
plt.ylabel('Number of Crimes')

# giving a title to my graph
plt.title('Offense Place v/s frequency')

plt.grid()
# function to show the plot
plt.show()
