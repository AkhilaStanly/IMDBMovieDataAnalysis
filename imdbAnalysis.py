from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, lit, to_date, count,avg,year,current_date
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("project").getOrCreate()

#Load data to dataframe

movies = spark.read.csv("s3://cpsc531moviedataset/imdb_movie_dataset.csv", header=True,sep=",")
movies.show()


print("To print datas in dataframes")
print(movies.columns)

print("To print datatype in each column")
print(movies.printSchema())

print("To print total rows of datas")
print(movies.count())

# To remove the null values in the released column
movies=movies.na.drop()
movies.show()

print("To print total rows of datas")
print(movies.count())


# print("select the release column")
# release_date=movies.select("released")
# release_date.show()

#converting releasedate column from string to date format

movies = movies.withColumn("release_date", to_date(col("released"), "yyyy-MM-dd"))
# movies.show()

#Adding  column 'season'

movies = movies.withColumn("season", when(month(movies["release_date"]) <= 2, lit("winter"))\
                           .when(month(movies["release_date"]) <= 5, lit("spring"))\
                           .when(month(movies["release_date"]) <= 8, lit("summer"))\
                           .otherwise(lit("fall")))
# movies.show()

#Adding column 'profit'

movies = movies.withColumn('profit',(movies['gross']-movies['budget'])/movies['budget'])
# movies.show()



# Question-1: When is the best time of year to release a movie to be successful?

print("When is the best time of year to release a movie to be successful?")

 # Get the current year
current_year = year(current_date())

# Filter the data to only include the last 10 years
movies = movies.filter(col('year') >= current_year - 10)
# movies.show()

# Taking the count of profitable movies in each season

winter_count= movies.filter((movies["profit"] >= 1) & (movies["season"] == "winter"))\
              .groupBy('year') \
              .agg(count('*').alias("winter"))
winter_count.show()
summer_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "summer")) \
                    .groupBy('year') \
                    .agg(count('*').alias("summer"))
summer_count.show()
fall_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "fall")) \
                    .groupBy('year') \
                    .agg(count('*').alias("fall"))
fall_count.show()
spring_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "spring")) \
                    .groupBy('year') \
                    .agg(count('*').alias("spring"))
spring_count.show()

# Join the counts for all seasons on the 'year' column
result = winter_count.join(spring_count, on='year', how='outer') \
                     .join(summer_count, on='year', how='outer') \
                     .join(fall_count, on='year', how='outer') \
                     .fillna(0)

result.show()

# To plot the graph

x_values = result.select("year").collect()
y1_values = result.select("winter").collect()
y2_values = result.select("spring").collect()
y3_values = result.select("summer").collect()
y4_values = result.select("fall").collect()

x_col = []
y1_col = []
y2_col = []
y3_col = []
y4_col = []

for x in x_values:
    x_col.append(x.__getitem__('year'))
for y1 in y1_values:
    y1_col.append(y1.__getitem__('winter'))
for y2 in y2_values:
    y2_col.append(y2.__getitem__('spring'))
for y3 in y3_values:
    y3_col.append(y3.__getitem__('summer'))
for y4 in y4_values:
    y4_col.append(y4.__getitem__('fall'))


# print(x_col)
# print(y1_col)
# print(y2_col)
# print(y3_col)
# print(y4_col)

plt.plot(x_col,y1_col,c='b',marker = 'o')
plt.plot(x_col,y2_col,c='g',marker = 'o')
plt.plot(x_col,y3_col,c='y',marker = 'o')
plt.plot(x_col,y4_col,c='r',marker = 'o')

plt.xlabel('year')
plt.ylabel('profitable_movies_count')
plt.title('best time of year to release a movie to be successful')
plt.legend(["winter","spring","summer","fall"], fontsize="small")
plt.show()

# Question-2: When is the best time for a particular genre to be released?

print("When is the best time for a particular genre to be released?")

winter_count= movies.filter((movies["profit"] >= 1) & (movies["season"] == "winter"))\
              .groupBy('genre') \
              .agg(count('*').alias("winter"))

summer_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "summer")) \
                    .groupBy('genre') \
                    .agg(count('*').alias("summer"))

fall_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "fall")) \
                    .groupBy('genre') \
                    .agg(count('*').alias("fall"))

spring_count = movies.filter((movies["profit"] >= 1) & (movies["season"] == "spring")) \
                    .groupBy('genre') \
                    .agg(count('*').alias("spring"))

# Join the counts for all seasons on the 'year' column
result = winter_count.join(spring_count, on='genre', how='outer') \
                    .join(summer_count, on='genre', how='outer') \
                    .join(fall_count, on='genre', how='outer') \
                    .fillna(0)

result.show()

# To plot the graph

x_values = result.select("genre").collect()
y1_values = result.select("winter").collect()
y2_values = result.select("spring").collect()
y3_values = result.select("summer").collect()
y4_values = result.select("fall").collect()


# print(x_values)
# print(y_values)
# #print(x_values[0].__getitem__('score_impact'))

x_col = []
y1_col = []
y2_col = []
y3_col = []
y4_col = []

for x in x_values:
    x_col.append(x.__getitem__('genre'))
for y1 in y1_values:
    y1_col.append(y1.__getitem__('winter'))
for y2 in y2_values:
    y2_col.append(y2.__getitem__('spring'))
for y3 in y3_values:
    y3_col.append(y3.__getitem__('summer'))
for y4 in y4_values:
    y4_col.append(y4.__getitem__('fall'))


# print(x_col)
# print(y1_col)
# print(y2_col)
# print(y3_col)
# print(y4_col)

plt.plot(x_col,y1_col,c='b',marker = 'o')
plt.plot(x_col,y2_col,c='g',marker = 'o')
plt.plot(x_col,y3_col,c='y',marker = 'o')
plt.plot(x_col,y4_col,c='r',marker = 'o')

plt.xlabel('genre')
plt.ylabel('profitable_movies_count')
plt.title('Best time for a particular genre to be released')
plt.legend(["winter","spring","summer","fall"], fontsize="small")
plt.show()



# Question-3: What are the popular genres of movies?

print("What are the popular genres of movies?")

# Grouping movies by genre and finding the total count of movies in each genre based on profit

popularity_count = movies.filter(movies["profit"] >= 1) \
                    .groupBy('genre') \
                    .agg(count('*').alias('popularity'))

popularity_count.show()
#
#
# To plot the graph

x_values = popularity_count.select("genre").collect()
y_values = popularity_count.select("popularity").collect()
# print(x_values)
# print(y_values)
#print(x_values[0].__getitem__('score_impact'))
x_col = []
y_col = []
for x in x_values:
    x_col.append(x.__getitem__('genre'))
for y in y_values:
    y_col.append(y.__getitem__('popularity'))
# print(x_col)
# print(y_col)


plt.xlabel('genre')
plt.ylabel('popularity')
plt.title('popular genres of movies')

plt.bar(x_col,y_col)
plt.show()



# Question-4: What are the popular MPAA rating of movies?

print("What are the popular MPAA rating of movies?")


popularity_count = movies.filter(movies["profit"] >= 1) \
                    .groupBy('rating') \
                    .agg(count('*').alias('popularity'))

popularity_count.show()


# To plot the graph

x_values = popularity_count.select("rating").collect()
y_values = popularity_count.select("popularity").collect()
print(x_values)
print(y_values)
#print(x_values[0].__getitem__('score_impact'))
x_col = []
y_col = []
for x in x_values:
    x_col.append(x.__getitem__('rating'))
for y in y_values:
    y_col.append(y.__getitem__('popularity'))
print(x_col)
print(y_col)


plt.xlabel('rating')
plt.ylabel('popularity')
plt.title('popular rating of movies')

plt.bar(x_col,y_col)
plt.show()

# Question-5 : Does movie runtime have any impact on IMDB score?

new =movies.withColumn('hours', movies['runtime']/60)

# new.show()
new1 = new.withColumn("hrs",when(new.hours<=1,'below 1 hr')\
                      .when((new.hours>1) & (new.hours<=1.5) ,'btw 1 and 1.5 hr')\
                      .when((new.hours>1.5) & (new.hours<=2) ,'btw 1.5 and 2 hr')\
                      .when((new.hours>2) & (new.hours<=2.5) ,'btw 2 and 2.5 hr')\
                      .when(new.hours>2 ,'above 2 hr'))
# new1.show()


runtime_impact = new1.groupBy('hrs').agg(avg('score').alias('score_impact'))\
                            .sort(col('score_impact').desc())

# runtime_impact.show()

# To plot the graph
x_values = runtime_impact.select("hrs").collect()
y_values = runtime_impact.select("score_impact").collect()
print(x_values)
print(y_values)
print(x_values[0].__getitem__('score_impact'))

x_col = []
y_col = []
for x in x_values:
    x_col.append(x.__getitem__('hrs'))
for y in y_values:
    y_col.append(y.__getitem__('score_impact'))
# print(x_col)
# print(y_col)

plt.plot(x_col,y_col)
plt.show()
