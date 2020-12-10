## Sparkify data lake:
##### In this project, I built an ELT pipeline using Apache Spark. I have extracted the data from the AWS s3 bucket and load back into another bucket which will help analysts find some insight inside the schema.


## ELT Pipeline:
#####  The etl.py script has three methods the main one will provide the path of the data and calling two other methods. The other methods will extract the data from the s3 bucket and load it to tables. finally, transformation applied to manage datatype that will help to filter the data by month and year. 

## Data allocation
#### Data:Create an s3 bucket to upload two folders:
        * data: log/song files 
        * output: empty  folder


## Tools and Technologies 
    * AWS - Create an EMR cluster to execute the ETL pipeline script
    * Install Python3, Spark, Panda
    * AWS - IAM account , and s3 storage 
  
## Scehma: 
- fact table: songsplay
- dimension: user
- dimension: artists 
- dimension: song
- dimension:  datetime

### project files:
- etl.py ETL script
- local_etl.py test the ETL localy
- Sparkify.ipynb notebook inside the EMR - elastic MapReduce cluster
  
