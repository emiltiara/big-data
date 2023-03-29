import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

with SparkSession.builder.getOrCreate() as spark:
    spark.conf.set("temporaryGcsBucket", "homework-alk-2-temp")

    accounts = spark.read \
        .format("bigquery") \
        .option("project", "big-data-alk-emil") \
        .option("table", "big-data-alk-emil.homework2.accounts") \
        .load()

    accounts.show()

    customers = spark.read \
        .format("bigquery") \
        .option("project", "big-data-alk-emil") \
        .option("table", "big-data-alk-emil.homework2.customers") \
        .load()

    customers.show()

    transactions = spark.read \
        .format("bigquery") \
        .option("project", "big-data-alk-emil") \
        .option("table", "big-data-alk-emil.homework2.transactions") \
        .load()

    transactions.show()

    new_transactions = transactions.withColumnRenamed("id", "transaction_id")
    new_transactions.show()

    accounts_with_transaction = new_transactions.join(accounts, new_transactions.account_id == accounts.id)

    accounts_with_transaction.show()

    accounts_with_transaction.withColumn("rounded_account_balance", f.round(f.col("account_balance"), 2))
    accounts_with_transaction.drop("id").show()

    accounts_with_transaction

    full_df = customers.join(accounts_with_transaction, accounts_with_transaction.customer_id == customers.id)

    full_df.show()

    full_df.drop("id").show()

    full_df.createOrReplaceTempView("full_df")

    new_sql_quer_test = spark.sql("""SELECT * FROM full_df where account_balance > 0""").show()

    customers_with_debit = spark.sql("""SELECT name, surname, SUM(account_balance) AS total_balance FROM full_df GROUP BY name, surname
    HAVING total_balance < 0;""")


    customers_with_debit.show()

    suspicious_customers = spark.sql("""SELECT name, surname, account_id, SUM(amount) AS total_transactions, account_balance
    FROM full_df
    GROUP BY name, surname, account_id, account_balance
    HAVING ROUND(SUM(amount), 2) != ROUND(account_balance, 2)""")

    suspicious_customers.show()

    # zapis do bigquery
    spark.conf.set("temporaryGcsBucket", "homework-alk-2")
    customers_with_debit.write.format("bigquery").option("table","big-data-alk-emil.homework2.customers_with_debit").mode("overwrite").save()


    # zapis do bigquery
    spark.conf.set("temporaryGcsBucket", "homework-alk-2")
    suspicious_customers.write.format("bigquery").option("table","big-data-alk-emil.homework2.suspicious_customers").mode("overwrite").save()
