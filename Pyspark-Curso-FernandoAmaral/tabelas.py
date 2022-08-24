# Faça você mesmo - teste

# Criando o banco de dados 
#sudo -u postgres psql
#create database tabelas

# Conectando ao banco de dados
#\c tabelas;

# Criando aplicação:

import sys. getopt
from pyspark.sql import SparkSession

if __name__=="__main__":
	spark = SparkSession.builder.appName("Tabelas").getOrCreate()
	opts, args = getopt.getopt(sys.argv[1:], "a:t:")
	arquivo, tabela = "",""
	for opt, arg in opts:
		if opt== "-a":
			arquivo = arg
		elif opt == "-t":
			tabela = arg
	
	# Lendo o arquivo 
	df = spark.read.load(arquivo)
	df.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/-tabelas").option("dbtable",tabela).option("user","postgres").option("password","19941994").option("driver","org.postgresql.Driver")
	
	spark.stop()
	

# Chamando o arquivo: spark-submit --jars /home/bruno/postgresql-42.3.3.jar tabelas.py -a /home/bruno/download/Atividades/Vendas.parquet -t Vendas 

# Verificando no banco 
#select * from vendas
