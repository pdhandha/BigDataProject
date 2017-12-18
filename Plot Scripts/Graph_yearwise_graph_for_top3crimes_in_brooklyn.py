import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


file_topcrime1 = open("../Spark Scripts/topCrime_brooklyn_year_crime_freq.out/part-00000", "r")
file_topcrime2 = open("../Spark Scripts/secondTopCrime_brooklyn_year_crime_freq.out/part-00000", "r")
file_topcrime3 = open("../Spark Scripts/thirdTopCrime_brooklyn_year_crime_freq.out/part-00000", "r")


plt.interactive(False)
year_topCrime1 = []
crimes_per_year_topCrime1 = []

year_topCrime2 = []
crimes_per_year_topCrime2 = []
year_topCrime3 = []
crimes_per_year_topCrime3 = []

dict_year_topCrime1 = {}
dict_year_topCrime2 = {}
dict_year_topCrime3 = {}

def add_missing_values(dict):
    new_dict = {}
    for y in range(2006,2016):
        ystr = str(y)
        if ystr in dict:
            new_dict[ystr] = dict[ystr]
        else:
            new_dict[ystr] = None
    return new_dict


for line1 in file_topcrime1:
    line1 = line1.strip()
    x, y = line1.split('\t', 1)
    dict_year_topCrime1[x] = y
dict_year_topCrime1 = add_missing_values(dict_year_topCrime1)
dict_year_topCrime1 = sorted(dict_year_topCrime1.items())
year_topCrime1, crimes_per_year_topCrime1 = zip(*dict_year_topCrime1)
crimes_per_year_topCrime1 = list(map(int,crimes_per_year_topCrime1))
# print(dict_year_topCrime1)
# print(year_topCrime1)
# print(crimes_per_year_topCrime1)

for line2 in file_topcrime2:
    line2 = line2.strip()
    x, y = line2.split('\t', 1)
    dict_year_topCrime2[x] = y
dict_year_topCrime2 = add_missing_values(dict_year_topCrime2)
dict_year_topCrime2 = sorted(dict_year_topCrime2.items())
year_topCrime2, crimes_per_year_topCrime2 = zip(*dict_year_topCrime2)
crimes_per_year_topCrime2 = list(map(int,crimes_per_year_topCrime2))


for line3 in file_topcrime3:
    line3 = line3.strip()
    x, y = line3.split('\t', 1)
    dict_year_topCrime3[x] = y
dict_year_topCrime3 = add_missing_values(dict_year_topCrime3)
dict_year_topCrime3 = sorted(dict_year_topCrime3.items())
year_topCrime3, crimes_per_year_topCrime3 = zip(*dict_year_topCrime3)
crimes_per_year_topCrime3 = list(map(int,crimes_per_year_topCrime3))
print(dict_year_topCrime3)
print(year_topCrime3)
print(crimes_per_year_topCrime3)

plt.rcParams.update({'font.size': 9})

# plt.yticks(np.arange(0,15000,500))
plt.plot(year_topCrime1, crimes_per_year_topCrime1, 'ro', year_topCrime2, crimes_per_year_topCrime2, 'bs', year_topCrime3, crimes_per_year_topCrime3, 'g^',linestyle='dashed')


red_dot = mlines.Line2D([], [], color='red', marker='o',markersize=10, label='ASSAULT 3')
blue_square = mlines.Line2D([], [], color='blue', marker='s',markersize=10, label='WEAPONS POSSESSION 3')
green_Arrow = mlines.Line2D([], [], color='green', marker='^',markersize=10, label='VIOLATION OF ORDER OF PROTECTI')
plt.legend(handles=[red_dot,blue_square,green_Arrow])

# plt.legend(loc='upper left')

# naming the x axis
plt.xlabel('Year')
# naming the y axis
plt.ylabel('Number of Crimes in Brooklyn')


# giving a title to my graph
plt.title('Top 3 crimes per year in Brooklyn')

# plt.grid()
# function to show the plot
plt.show()


