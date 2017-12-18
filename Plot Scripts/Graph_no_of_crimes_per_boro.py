import matplotlib.pyplot as plt

file = open("../Spark Scripts/crimes_per_borough.out/part-00000","r")

num = [1, 2, 3, 4, 5]
LABELS = ["Bronx", "Brooklyn", "Staten Island", "Manhattan", "Queens"]

plt.interactive(False)
broughs = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    broughs.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
print(broughs)
print(num)

plt.bar(num, no_of_crime, align='center', alpha=0.5)


plt.xticks(num, broughs)

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+100000)
plt.xlim(0, 6)

# naming the x axis
plt.xlabel('boroughs')
# naming the y axis
plt.ylabel('Number of Crimes')

# giving a title to my graph
plt.title('Crimes v/s Borough')

# function to show the plot
plt.show()


