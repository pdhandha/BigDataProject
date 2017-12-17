import matplotlib.pyplot as plt

file = open("../Spark Scripts/brooklyn_year_crime_freq.out/part-00000","r")

plt.interactive(False)
year = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    year.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
year.sort()
print(year)
num=range(1, len(year) + 1)

plt.rcParams.update({'font.size': 9})
plt.bar(num, no_of_crime, align='center', alpha=0.5, color = 'r')

plt.xticks(num, year, rotation ="vertical")

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+10)
plt.xlim(0, max(num)+1)

# naming the x axis
plt.xlabel('Year')
# naming the y axis
plt.ylabel('Number of Crimes in Brooklyn')

# giving a title to my graph
plt.title('Year vs Brooklyn crime frequency')

plt.grid()
# function to show the plot
plt.show()


