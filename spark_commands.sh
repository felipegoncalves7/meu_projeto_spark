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

