# Map-Reduce-of-Vehicles
This is for the course of Big Data Management.
This project is created to work on Hadoop Framework for vehicles Data-Set collected from this website :
https://datasource.kapsarc.org/explore/dataset/us-vehicle-fuel-economy-data-1984-2017/information/?disjunctive.make&disjunctive.model&sort=-year
It contains 4 different queries:
1. Write a MapReduce job to count models of different makes of vehicles. Display the top 5makes.
2. For all vehicles, write a MapReduce job to get the average annual petroleum consumption in barrels for fuelType1for each make of vehicle. Your MapReduce jobs should print the make of vehicle make in descending order. 
3. For electric cars only, write a MapReduce job to display the average city MPG for fuelType1   
4. Write a MapReduce job to calculate the difference in the average annual petroleum consumption in barrels for fuelType1between electric cars and gasoline cars 
 
ssh bbod
ssh hadoop1

compile:  javac Habib_Boloorchi_Program_(number).java 

create jar :  jar cfe Habib_Boloorchi_Program_1.jar Habib_Boloorchi_Program_1 *.class



export HADOOP_CLASSPATH=Habib_Boloorchi_Program_(number).jar


run in hadoop: hadoop Habib_Boloorchi_Program_(number)  /CS5433/2019/vehicles.csv autohome/username/out

fetch output: hadoop fs -get autohome/hboloor/out/
