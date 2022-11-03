# SparkETL-Scala
## The project aims to provide an example of how a SparkETL can be written.

We will be using the events.csv file for our tests.
https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset?select=events.csv

The aim of the ETL is to answer a rudementary question:

1. Convert the timestamp  column which is a unix epoch to a date (evenDate).
2. On each date track the number of times an item was added to cart ,transacted or just viewed.
3. An additional rule i added is if an item was transacted  we do not count in under the metrics of addedToCart or view.
Similarly if it was addedToCart we do not count it under view.
4. The idea behind this is to just gauge how many items are sold, how many items were viewed and how many were addedtocart.

Sample Output

| eventDate | itemid | addtocart | transaction | view  |
| :---:     | :---:  | :---:     | :---:       | :---: |      
|2015-05-08 |94660   |0          |0            |1      |
|2015-06-15 |39439   |0          |0            |6      |
|2015-06-19 |281103  |0          |0            |3      |



