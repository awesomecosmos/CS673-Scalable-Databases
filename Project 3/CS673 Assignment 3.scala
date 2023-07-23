// Databricks notebook source
// MAGIC %md
// MAGIC # CS673 Scalable Databases - Assignment #3
// MAGIC
// MAGIC Aayushi Verma

// COMMAND ----------

// instantiating Spark context
sc.appName

// COMMAND ----------

// MAGIC %md
// MAGIC **Count the total amount ordered by each customer in Scala using RDD.**
// MAGIC - Split each comma-delimited line into RDD
// MAGIC - Map each line to key/value pairs to cust_id and amount spent
// MAGIC - Use reduce by key to add up amount for each customer
// MAGIC - Collect() the results and print

// COMMAND ----------

// reading the CSV file into an RDD
// RDD stands for Resilient Distributed Dataset
val lines = sc.textFile("/FileStore/tables/customer_orders-2.csv")

// COMMAND ----------

// checking results
lines.collect

// COMMAND ----------

// splitting CSV by comma
val orders = lines.map(x => x.split(","))

// COMMAND ----------

// viewing results
orders.collect

// COMMAND ----------

// mapping key-value pairs of Cust_ID and Amount Spent
val customers = orders.map(x => (x(0), x(2).toDouble))

// COMMAND ----------

// checking results
customers.collect

// COMMAND ----------

// adding amount spent for each customer
val customer_totals = customers.reduceByKey((x,y) => x+y)

// COMMAND ----------

// checking results
customer_totals.collect

// COMMAND ----------

// MAGIC %md
// MAGIC **List the customer who spent highest amount.**

// COMMAND ----------

// using the reduce method with an if/else loop to find the highest amount spent
val highest_amt = customer_totals.reduce((x, y) => if (x._2 > y._2) x else y)

// COMMAND ----------

// checking highest amount spent
highest_amt._2

// COMMAND ----------

// MAGIC %md
// MAGIC **List Top 5 customers based on the amount which they have spent, along with the customer ID and Product ID.**

// COMMAND ----------

// mapping key-value pairs of Cust_ID, Prod_ID and Amount Spent
val customers_orders = orders.map(x => (x(0), x(1).toInt, x(2).toDouble))

// COMMAND ----------

// checking values
customers_orders.collect

// COMMAND ----------

// sorting customers by amount spent in descending order
val desc_customer_totals = customers_orders.sortBy(_._3, ascending = false)

// COMMAND ----------

// checking results
desc_customer_totals.collect

// COMMAND ----------

// finding top 5 customers by amount spent
val top5_customers = desc_customer_totals.take(5)

// COMMAND ----------

// printing top 5 customers by total amount spent
// printing Customer_ID, Product_ID, Amount_Spent
top5_customers.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC **List the Bottom 5 customers based on the amount spent, along with the customer ID and Product ID.**

// COMMAND ----------

// sorting customers by amount spent in descending order
val asc_customer_totals = customers_orders.sortBy(_._3)

// COMMAND ----------

// checking results
asc_customer_totals.collect

// COMMAND ----------

// finding top 5 customers by total amount spent
val bottom5_customers = asc_customer_totals.take(5)

// COMMAND ----------

// printing top 5 customers by total amount spent
// printing Customer_ID, Product_ID, Amount_Spent
bottom5_customers.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC **Find out customers who spent an average amount of the total amount spent in data.**

// COMMAND ----------

// finding total amount spent by all customers
val total_amount_spent = customer_totals.map(_._2).sum()

// checking results
println(total_amount_spent)

// COMMAND ----------

// getting total number of customers
val total_customers = customer_totals.count()

// checking results
println(total_customers)

// COMMAND ----------

// getting average amount spent by customers
val average_spent = total_amount_spent / total_customers

// checking results
println(average_spent)

// COMMAND ----------

// filtering out customers whose total amount spent is equal to the average amount spent
val customers_who_spent_avg = customer_totals.filter{case(_, amount) => amount == average_spent}

// checking results
customers_who_spent_avg.collect

// COMMAND ----------

// MAGIC %md
// MAGIC **Create a function to give rewards ($5) to all the customers who spent more than the average of the total amount.**

// COMMAND ----------

def customer_rewards(rdd: RDD[String]): RDD[(String, Double)] = {
  // splitting CSV by comma
  val lines = rdd.map(line => line.split(","))

  // mapping key-value pairs of Cust_ID and Amount Spent
  val customers = lines.map(x => (x(0), x(2).toDouble))

  // adding amount spent for each customer
  val customer_totals = customers.reduceByKey(_ + _)

  // getting average amount spent by customers
  val total_amount = customer_totals.map(_._2).sum()
  val total_customers = customer_totals.count()
  val avg_amount = total_amount / total_customers

  // filtering out customers whose total amount spent is equal to the average amount spent
  val loyal_customers = customer_totals.filter{case(_, amount) => amount > avg_amount}

  // rewarding $5 to all loyal customers
  val customer_rewards = loyal_customers.mapValues(amount => amount + 5)

  // returning RDD with rewarded customers
  customer_rewards
}

// COMMAND ----------

// checking function
val rewarded_customers = customer_rewards(lines)

// COMMAND ----------

// checking results
rewarded_customers.collect

// COMMAND ----------

// MAGIC %md
// MAGIC **Sort the customers based on the amount spent (high to low)**

// COMMAND ----------

// sorting customer_totals in descending order
val desc_customer_amt_spent = customer_totals.sortBy(_._2, ascending = false)

// COMMAND ----------

// checking results
desc_customer_amt_spent.collect

// COMMAND ----------

// MAGIC %md
// MAGIC **List the product ID's of Top 5 customers who have purchased**

// COMMAND ----------

// printing Product_ID of top 5 customers
println("Top 5 Customers' Product ID:")
top5_customers.foreach{case(_, product_id, _) =>
  println(s"Product ID: $product_id")
}

// COMMAND ----------

// MAGIC %md
// MAGIC **List most sold product ID's**

// COMMAND ----------

// mapping key-value pairs of Product_ID and Amount
val products = orders.map(x => (x(1), x(2).toDouble))

// COMMAND ----------

// checking results
products.collect

// COMMAND ----------

// adding amount spent for each Product_ID
val product_totals = products.reduceByKey(_ + _)

// COMMAND ----------

// checking results
product_totals.collect

// COMMAND ----------

// sorting in descending order
val top_products = product_totals.sortBy(_._2, ascending = false)

// COMMAND ----------

// finding top 5 products
val top5_products = top_products.take(5)

// COMMAND ----------

// checking results and printing
println("Most Sold Product IDs:")
top5_products.foreach { case (product_id, total) =>
  println(s"Product ID: $product_id, Total Amount: $total")
}
