from azure.cosmos import exceptions, CosmosClient, PartitionKey

url = "informe aqui a URL"
key = "Informe aqui a Chave"
client = CosmosClient(url, key)

database_name = "Migration"
try:
    database = client.create_database(id=database_name)
except exceptions.CosmosResourceExistsError:
    database = client.get_database_client(database=database_name)

containercli = "Clientes"
try:
    container_cliente = database.create_container(
        id=containercli, partition_key=PartitionKey(path="/id")
    )
except exceptions.CosmosResourceExistsError:
    container_cliente = database.get_container_client(containercli)

containerfor = "Fornecedores"
try:
    container_fornecedor = database.create_container(
        id=containerfor, partition_key=PartitionKey(path="/id")
    )
except exceptions.CosmosResourceExistsError:
    container_fornecedor = database.get_container_client(containerfor)


database = client.get_database_client(database_name)

container_cliente = database.get_container_client(containercli)
container_fornecedor = database.get_container_client(containerfor)

import pandas as pd

confirmed_cli = pd.read_csv("caminho_arquivo.csv")
confirmed_cli.head()

confirmed_cli["id"].unique()
confirmed_cli.drop_duplicates
qtd_cli = confirmed_cli.count

containercli = database.get_container_client(container_cliente)
for i in confirmed_cli.count:
    containercli.upsert_item(
        dict(id=confirmed_cli['id'], Name=confirmed_cli['nome'], productModel="Model {}".format(i))
    )

confirmed_for = pd.read_csv("caminho_arquivo.csv")
confirmed_for.head()

confirmed_for["id"].unique()
confirmed_for.drop_duplicates
qtd_for = confirmed_for.count

containerfor = database.get_container_client(container_fornecedor)
for i in confirmed_for.count:
    containerfor.upsert_item(
        dict(id=confirmed_for['id'], Name=confirmed_for['nome'], productModel="Model {}".format(i))
    )

qtd_cli = containercli.__doc__.count
try:
    print('Migração de Clientes concluída com sucesso.')    
except exceptions.CosmosResourceExistsError:
    print('Falha na Migração de Clientes.')

qtd_for = containerfor.__doc__.count
try:
    print('Migração de Fornecedores concluída com sucesso.')    
except exceptions.CosmosResourceExistsError:
    print('Falha na Migração de Fornecedores.')

try:
    database.create_user(dict(id="Wiliam Rosa"))
except exceptions.CosmosResourceExistsError:
    print("Já existe um usuário com este ID.")
except exceptions.CosmosHttpResponseError as failure:
    print("Falha ao criar usuário. codigo:{}".format(failure.status_code))
