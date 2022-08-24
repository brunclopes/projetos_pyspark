from pyspark.sql import SparkSEssion


if __name__ == "__main__":
	spark = SParkSession.builder.appName("Streaming").getOrCreate)_
	
	jsonschema = "nome STRING, postagem STRING, data INT"
	
	df = spark.readStream.json("/home/bruno/testestream", schema=jsonschema)
	
	diretorio = "/home/bruno/temp"
	
	stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("ckeckpointlocation", diretorio).start()
	
	stcal.awaitTermination()
	
# Chamando o processo: spark-submit streaming.py
