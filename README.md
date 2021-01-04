# GDPR_PROJET

Dans les grandes lignes, il va falloir mettre en place un projet qui nous permette de créer un système de compliance "GDPR". On va limiter le scope de ce problème à la manipulation des fichiers et des données dans HDFS.

L'idée est de développer 3 services :

1. Un service qui nous permette de supprimer des données depuis des fichiers CSV ou PARQUET (par exemple, nous pouvons avoir des données client, et un des clients voudrait supprimer les données qui lui sont relatives en nous fournissant son identifiant. Nous devrons être en mesure de le faire)
2. Pareil que pour le service 1., ce service va nous permettre de hasher les données d'un client lorsqu'il le demande.
3. Enfin, un 3ème service qui recevra en entrée l'identifiant d'un client, et qui devra générer un fichier CSV contenant toutes les données relatives à celui-ci, et les lui envoyer par email, par exemple.
Nous utiliserons les technologies suivantes :
4. Spark
5. Scala
6. Scopt (je vous laisse regarder ça)
7. Github et Github Actions
8. Spray JSON
je suis entrain de regarder pour trouver une base client avec quelques données que nous pourrons utiliser, et si je ne trouve pas, nous pourrons les générer (si vous pouvez m'aider dans cette recherche, ça serait appréciable)
Pour commencer, vous pouvez créer un fichier CSV contenant plusieurs clients (une 100ene suffirait) avec les schéma suivant :

ID, Nom, Prenom

Suite à ça, écrivez (peu importe la forme, juste histoire de manipuler l'API de Spark) un programme qui nous permettrait, pour un client donné (identifié par son ID), nous retourner toutes les lignes qui lui correspondent dans les données que nous avons, et ensuite, continuer ce programme en "supprimant" ces lignes depuis la donnée initiale.

### Suite

Les données que nous recevons (CSV) ne sont pas tout le temps correctement typées. Dans cette partie, il faudra écrire un fichier de configuration permettant de `mapper` les données dans le fichier CSV avec les types fournis en configuration. La configuration doit être au format JSON et ressembler à ceci :


```json
{
columns: [
{
"name": "columnName1",
"type": "columnType2"
},
{
"name": "columnName2",
"type": "columnType2"
}

]
}
```

Les données qui seront lues doivent avoir le nom défini dans le fichier de configuration et le type qui y est associé.

### Use StructType and StructField.
