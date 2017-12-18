import matplotlib.pyplot as plt

file = open("../Spark Scripts/law_cat_cd_freq_all.out/part-00000","r")


num = [1, 2, 3,]
LABELS = ["Bronx", "Brooklyn", "Staten Island", "Manhattan", "Queens"]

plt.interactive(False)
level_of_offence = []
no_of_crime = []
for line in file:
    line = line.strip()
    x, y = line.split('\t', 1)
    level_of_offence.append(x)
    no_of_crime.append(int(y))
print(no_of_crime)
print(level_of_offence)
print(num)

# plt.bar(num, no_of_crime, align='center', color = "red")
#
# plt.xticks(num, level_of_offence)
#
# # setting x and y axis range
# plt.ylim(0,  max(no_of_crime)+10000)
# plt.xlim(0, 4)
#
# # naming the x axis
# plt.xlabel('Level of crime')
# # naming the y axis
# plt.ylabel('Number of Crimes')
fig1, ax1 = plt.subplots()
ax1.pie(no_of_crime, labels=level_of_offence, autopct='%1.1f%%', shadow=True, startangle=90)

# giving a title to my graph
plt.title('Level of Crimes in percentage')

#setting background as grid
# plt.grid()

# function to show the plot
plt.show()


