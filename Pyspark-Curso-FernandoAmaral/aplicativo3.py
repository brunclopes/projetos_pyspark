# Criando o primeiro aplicativo

import sys, getopt

# Iniciando a sessão do spark
from pyspark.sql import SparkSession

# Importando as funções (para calcular o ano)
from pyspark.sql.functions import * 

# Criando o módulo
if __name__ == "__main__":
	spark = SparkSession.builder.appName("Exemplo").getOrCreate()
	opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")
	formato, infile, outdir = "","",""
	
	for opt, arg in opts:
		if opt == "-t":
			formato = arg
		elif opt == "-i":
			infile = arg 
		elif opt == "-o":
			outdir = arg
		
	dados = spark.read.csv(infile, header = False, inferSchema = True) 
	dados.write.format(formato).save(outdir)
	
	spark.stop()

# Chamando o arquivo no terminal: spark-submit aplicativo3.py -t parquet -i /home/bruno/download/despachantes.csv -o /home/bruno/testesparquet/

# Novo teste: spark-submit aplicativo3.py -t orc -i /home/bruno/download/reclamacoes.csv -o /home/bruno/testesorc/

# Novo teste: spark-submit aplicativo3.py -t json -i /home/bruno/download/reclamacoes.csv -o /home/bruno/testesjson/
