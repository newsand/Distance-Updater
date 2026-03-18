# project Structure

## runtime
This project must be  python  3.13 and run with UV manager
Execute: `uv sync` to sync dependencies
run using uv run python main.py

## file structure
project. and its very simple. it only uses 3 python files. one for mapping DB one for google conection and one for main routine. AT ROOT
it need a nixpack file
### pyfiles
-googletools.py
-database.py
-main.py
### deploy files
-nixpacks.toml

-other structural files like tomls, lock, requirements, etc
## database 
this project must conect to a postgres database with variables providade in .env
    "dbname":
    "user":    
    "password": 
    "host": 
    "port": 
    "sslmode":

### tables and columns
the project will update only one table which is "solicitacao". this project must update the column "distancia" it can create temporary tables. and its not mandatory its optional. 
column distancia must be a integer



## deploy
its going to be deployed on coolify with NIXPACKS. 

## libs
Use pandas dataframe for listing processing and updating data
use pyscopg2 as postgres driver
dotenv
use pydantic to validade fields

## rules and objectives

### selection 
we will update data within the following criteria:(sql-like-ish)
select from solicitacao where 
and distancia = 0
and data_conclusao IS NOT NULL 

"probably : SELECT s.id, origem.nome AS origem, origem.sigla_uf AS uf_origem, destino.nome AS destino, destino.sigla_uf AS uf_destino
        FROM solicitacao s
        JOIN rel_solicitacao_veiculo rsv ON s.id = rsv.solicitacao_id
        JOIN endereco e ON e.id = rsv.origem
        JOIN endereco ee ON ee.id = rsv.destino
        JOIN municipio origem ON e.municipio_id = origem.id
        JOIN municipio destino ON ee.municipio_id = destino.id
        WHERE s.data_conclusao IS NOT NULL;
        AND distancia = 0
        """
### update distance
- we will get the origen and  destination  as the name of the origin city and destiny city
- we will get this distance from a file  distancias.csv or from google maps API in case the route is not present in the file
- the final distance at the database must be saved like this: google/file distance +40KM stored as INTEGER
for example 

WENCESLAU BRAZ/MG:RIO DE JANEIRO/RJ,WENCESLAU BRAZ/MG,RIO DE JANEIRO/RJ,300.829
save in database round(300.829 + 40)

- Make a bulk UPDATE one by one will take to long make some sort of bulk or at least a batch update
when getting distance from google api Update the distancias.csv file for using it as future cache. (update the distance dataframe aswell we may save a lot of requests)


## tests
Before  deploying we are going to create a few tests.. the test is we get a sample of 10 lines from the  distance file. and we get the route. we test if get distance function + the function that will calculate the distance +40 is working properly.

## versioning
ad version ßeta by now. and print the version as the first thing when code is running

##