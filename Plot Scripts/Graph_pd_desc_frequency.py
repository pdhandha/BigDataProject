import matplotlib.pyplot as plt

file = open("../Spark Scripts/pd_desc_freq.out/part-00000","r")

plt.interactive(False)
pd_desc = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    pd_desc.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
print(pd_desc)
num=range(1, len(pd_desc) + 1)

plt.rcParams.update({'font.size': 9})
plt.bar(num, no_of_crime, align='center', alpha=0.5, color = 'r')

plt.xticks(num, pd_desc, rotation ="vertical")

plt.gcf().subplots_adjust(bottom = 0.2)

# setting x and y axis range
plt.ylim(0,  max(no_of_crime)+1000)
plt.xlim(0, max(num)+1)

# naming the x axis
plt.xlabel('PD Code')
# naming the y axis
plt.ylabel('Number of Crimes')

# giving a title to my graph
plt.title('PD Code vs Crime frequency')

plt.grid()
# function to show the plot
plt.show()


