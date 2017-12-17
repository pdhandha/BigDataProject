import matplotlib.pyplot as plt

file = open("../Spark Scripts/brooklyn_time.out/part-00000","r")

plt.interactive(False)
hour_of_day = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    hour_of_day.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
hour_of_day.sort()
print(hour_of_day)
num=range(1, len(hour_of_day) + 1)

plt.rcParams.update({'font.size': 9})
plt.bar(num, no_of_crime, align='center', alpha=0.5, color = 'r')

plt.xticks(num, hour_of_day, rotation ="vertical")

plt.gcf().subplots_adjust(bottom = 0.6)

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+10)
plt.xlim(0, max(num)+1)

# naming the x axis
plt.xlabel('Hour of the day')
# naming the y axis
plt.ylabel('Number of Crimes')

# giving a title to my graph
plt.title('Hour of the day vs Brooklyn crime frequency')

plt.grid()
# function to show the plot
plt.show()


