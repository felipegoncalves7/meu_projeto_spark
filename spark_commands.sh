#Token Github
ghp_dHmphG1s9fJ1vsiiXESXVu727EL8Kq32HBJ1

# Iniciar um nó mestre do Spark
/opt/spark/sbin/start-master.sh

# Parar o nó mestre do Spark
/opt/spark/sbin/stop-master.sh


# Rodando EXEMPLOS
# Obter o valor de PI
run-example SparkPi

# Contador de palavras de um determinado arquivo
run-example JavaWordCount kern.log

# Executando alguns comandos no Pyspark

#Aplicando filtro na minha lista numeros - Transformação para filtrar
filtro = numeros.filter(lambda filtro: filtro > 2)
#Ação para mostrar o resultado
filtro.collect()

# Multiplicando cada número da minha lista por 2 - Ação para transformar
mapa = numeros.map(lambda mapa: mapa * 2)
#Ação para mostrar o resultado
mapa.collect()

#Registrar dados em comum de dois conjuntos diferentes
#Unificando os dados
uniao = numeros.union(numeros2)
#Chamando o método
interseccao = numeros.intersection(numeros2)
#Exibindo
interseccao.collet()

#Exibindo dados de um conjunto que não aparecem no outro
subtrai = numeros.subtract(numeros2)

#Criando variável com chave-valor
compras = sc.parallelize([(1,200),(2,300),(3,120),(4,250),(5,78)])

#Método RDD para armazenar as minhas chaves em uma variável e depois mostrá-la
chaves = compras.keys()
chaves.collect()

#Armazenando os valores
valores = compras.values()
valores.collect()

#Somando +1 em cada valor
soma = compras.mapValues(lambda soma: soma + 1)
soma.collect()

#Criando a variável debitos para conter as keys que tiveram debito
debitos = sc.parallelize([(1,20),(2,300)])

#Assimilando qual o valor da compra e quanto foi debitado
resultado = compras.join(debitos)
resultado.colletc()

#Trazendo quais as compras que não tiveram debitos
semdebito = compras.subtractByKey(debitos)
semdebito.collect()

#Criando um dataframe sem definir um schema
from pyspark.sql import SparkSession
df1 = spark.createDataFrame([("Pedro",10),("Maria",20),("José",40)])
df1.show()

#Definindo um Schema
schmema = "Id INT, Nome STRING"
dados = [[1,"Pedro"],[2,"Maria"]]

df2 = spark.createDataFrame(dados, schmema)

#Agrupando os dados
# Biblioteca Utilizada
from pyspark.sql.functions import sum
schema2 = "Produtos STRING, Vendas INT"
vendas = [["Caneta",10],["Lápis",20],["Caneta",40]]
df3 = spark.createDataFrame(vendas, schema2)
agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))

#Visualizando colunas
df3.select("Produtos").show()

#Usando expressões junto com o Select no spark
#Importando a biblioteca
from pyspark.sql.functions import expr
df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()

#Visualizando propriedades do meu DataFrame
#Tipos de dados - String, Int, float, etc
df3.schema

#Colunas
df3.colums

#PROCESSO DE INGESTÃO DE DADOS
from pyspark.sql.types import *
#Definindo o schema
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

#Importando um arquivo .CSV através de um schema pré-estabelecido
despachantes = spark.read.csv("/home/felipe/download/despachantes.csv",header=False, schema = arqschema)

#Importando com load e sem um schema pré-definido - Ou seja, o própio Spark faz a inferência dos tipos de dados
desp_autoschema = spark.read.load("/home/felipe/download/despachantes.csv", header=False, format="csv", sep=",", inferSchema=True)

#importando funções do spark
from pyspark.sql import functions as Func

despachantes.select("id", "nome", "vendas").where(Func.col("vendas") > 20).show()
#OU
despachantes.select("id", "nome","vendas").where(expr("vendas > 20")).show()

#Utilizando operadores lógicos
despachantes.select("id", "nome","vendas").where(expr("vendas > 20") & expr("vendas < 40")).show()
despachantes.select("id", "nome", "vendas").where((Func.col("vendas") > 20) & (Func.col("vendas") < 40)).show()

#É necessário a criação de um novo df para as devidas alterações
#Alterando nome da coluna
novodf = despachantes.withColumnRenamed("nome","nomes")

#Alterando o tipo de dado
from pyspark.sql.functions import *
despachantes2 = despachantes.withColumn("data2", to_timestamp(Func.col("data"), "yyyy-MM-dd"))

#Algumas funcionalidades de dados com valor de Data
despachantes2.select(year("data")).show()
despachantes2.select(year("data")).distinct().show()
despachantes2.select("nome", year("data")).orderBy(year("data")).show()

#PRINCIPAIS AÇÕES E TRANSFORMAÇÕES
#EXIBINDO OS DADOS COM O TAKE
despachantes.take(2)
#EXIBINDO OS DADOS COM O COLLECT
despachantes.collect()
#CONTANDO O NÚMERO DE LINHAS DO MEU DATAFRAME
despachantes.count()

#ORDENANDO EM ORDEM CRESCENTE AS VENDAS
despachantes.orderBy("vendas").show()
#ORDENANDO EM ORDEM DESCRECENTE AS VENDAS
despachantes.orderBy(Func.col("vendas").desc()).show()
#ORDENANDO EM ORDEM DESCRESCENTE AS VENDAS COM BASE NA ORDEM DECRECENTE DAS CIDADES
despachantes.orderBy(Func.col("cidades").desc(), Func.col("vendas")).show()

#AGRUPANDO CIDADES COM SEUS RESPECTIVOS NÚMEROS DE VENDAS
despachantes.groupBy("cidade").agg(sum("vendas")).show()
#AGRUPANDO CIDADES COM SEUS RESPECTIVOS NÚMEROS DE VENDAS - EM ORDEM DESCRECENTE
despchantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()
#AGRUPANDO CIDADES COM SEUS RESPECTIVOS NÚMEROS DE VENDAS - EM ORDEM DESCRECENTE E MAIOR QUE 40
despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).where(Func.col("sum(vendas)") > 40).show()

#FILTRANDO POR UM NOME EM ESPECÍFICO
despachantes.filter(Func.col("nome") == "Deolinda Vilela").show()

#EXPORTANDO E IMPORTANDO DADOS

#EXPORTANDO EM DIFERENTES FORMATOS
#PARQUET
despachantes.write.format("parquet").save("/home/felipe/dfimportparquet")
#CSV
despachantes.write.format("csv").save("/home/felipe/dfimportcsv")
#JSON
despachantes.write.format("json").save("/home/felipe/dfimportjson")
#ORC
despachantes.write.format("orc").save("/home/felipe/dfimportorc")


#IMPORTANDO DADOS

#PARQUET
par = spark.read.format("parquet").load("/home/felipe/dfimportparquet/despachantes.parquet")

#JSON
js = spark.read.format("json").load("/home/felipe/dfimportjson/despachantes.json")

#ORC
orc = spark.read.format("orc").load("/home/felipe/dfimportorc/despachantes.orc")

#ATIVIDADE - SEÇÃO 3

#01 - CRIE UMA CONSULTA QUE MOSTRE NESTA ORDEM, NOME, ESTADOS E STATUS
cli = spark.read.format("parquet").load("/home/felipe/download/Atividades/Clientes.parquet")

cli.select("Cliente", "Estado", "Status").show()

#02 - CRIE UMA CONSULTA QUE MOSTRE APENAS OS CLIENTES DO STATUS "PLATINUM" E "GOLD
cli.filter(Func.col("Status") != "Silver").show()

#03 - DEMONSTRE QUANTO CADA STATUS DE CLIENTES REPRESENTA EM VENDAS
vend = spark.read.format("parquet").load("/home/felipe/download/Atividades/Vendas.parquet")

df_inner = cli.join(vend, cli["ClienteID"] == vend["ClienteID"], "inner")

df_inner.groupBy("Status").agg(sum("Total")).show()
