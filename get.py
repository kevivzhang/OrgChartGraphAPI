url = "https://graph.microsoft.com/v1.0/users/{id}/directReports"

payload={}

response = requests.request("GET", url, headers=headers, data=payload)

df = spark.read.json(sc.parallelize([response.text]))

df = df.select(f.explode_outer('value').alias('data'))

df = df.select("data.*")
df.write.mode("overwrite").saveAsTable("default.orgTree")
