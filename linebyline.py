# importanto Sparksession

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit , col , lower


# estabelecendo a conexão (porta de entrada)
spark = SparkSession.builder.appName("PrimeiroProjeto").getOrCreate() # getOrCreate cria uma nova sessão caso não exista

# Exibindo a versão do Spark que está rodando
spark.version

# Meus dados para serem usados
data = [("Ramon",29),
        ("Daniela",30),
         ("Roney",32),
         ("Igor",20),
         ("Marislei",60),
         ("Maristela",62),
         ("Pedro",30),
         ("Gabriel",30),
         ("Gustavo",39),
         ("Marcos", 18)

        ]
    # outra lista com dados: name, job e dream
data2 = [
    ("Alice", "Engineer", "Travel the world"),
    ("Ramon", "Teacher", "Write a book"),
    ("Cris", "Designer", "Start a fashion brand"),
    ("David", "Chef", "Own a Michelin restaurant"),
    ("Eva", "Doctor", "Help in poor countries"),
    ("Gabriel", "Developer", "Build a tech startup"),
    ("Grace", "Nurse", "Open a health clinic"),
    ("Igor", "Artist", "Paint in Paris"),
    ("Ivy", "Scientist", "Win a Nobel Prize"),
    ("Jack", "Pilot", "Fly around the globe")
    ]


    # usando a ferramenta df(dataframe dados em forma de tabela) para manipular através de funções e métodos

df = spark.createDataFrame(data,["Name", "age"]) # criando dados em forma de tabela com colunas de nome e idade
df2 = spark.createDataFrame(data2,["Name", "Job", "Dream"])



df.show(truncate= False) # exibindo a tabela com as colunas nome e idade e os dados preenchidos
df2.show(truncate= False) # o truncate jovem é para eu podem ver todos os dados sem cortas frases longas

# tudo funcinando até aqui, agora vamos manipular esses dados com clareza, fique calmo, tome água,respira e vem com o tio!

# selecionando tabela Name apenas, praticamente SQL
df.select("Name").show()

df.filter(df["age"]> 30).show() # se tudo estiver certo, deverá fazer uma filtragem literalmente dos dados e mostrar apenas os de acima 30 anos

df.sort("age").show() # vai ordenar minha tabela em crescente por idade

# agora jovem jedi vamos ordenar de forma decrecente (um pouquinho mais chatinho mais vou veio até aqui né)

df.sort(df["age"].desc()).show() # desc == decrecente jovem

# agora vamos adicionar uma coluna nova que tal? isso mesmo uma coluna nova
# paises com restrição fixa para USA (MAKE AMERICAN GREAT AGAIN) todos se tornam americanos

df = df.withColumn("Country", lit("USA"))
df.show()

# agora nobre guerreiro vamos adicionar uma coluna aumentanto a idade em 5

df = df.withColumn("agePlus5", col("age") + 5)
df.show()

# agora vamos pensar? vou usar um join juntar duas coisas, porém só tenho uma lista de dados, logo jovem vou precisa de mais
# vamos criar outro dataframe(tabelinha/entidade para intimos em sql).
# vamos juntar, lembrando join só junta os que gosto parecido, tem correspondencia na duas tabelas que no caso é name
# lembrando(eu esqueci ou não sabia, nunca saberemos) padronize os tipos de dados para uma exibição mais confiável!

df = df.withColumn("Name", lower(df["Name"]))
df2 = df2.withColumn("Name", lower(df2["Name"]))
# o que foi feito aqui? estou deixando tudo minusculo padronizando tudo
df.select("Name").show(truncate=False)
df2.select("Name").show(truncate=False)


join_df = df.join(df2, on="Name", how="inner")
join_df = df.join(df2, on="Name", how="left")
join_df = df.join(df2, on="Name", how="right")

join_df.show(truncate= False)
# juntamos as colunas viu?

# vamos ordenar a tabela agora



