from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
# import pyspark.sql.types as t
# import pyspark.sql.functions as f
import DataFrames as df
import Task01
import Task02
import Task03
import Task04
import Task05
import Task06
import Task07
import Task08

srcPath = ".\\DataSet\\"
Files = [ 
            f"{srcPath}title.akas.tsv.gz",          # 0
            f"{srcPath}title.basics.tsv.gz",        # 1
            f"{srcPath}title.crew.tsv.gz",          # 2
            f"{srcPath}title.episode.tsv.gz",       # 3
            f"{srcPath}title.principals.tsv.gz",    # 4
            f"{srcPath}title.ratings.tsv.gz",       # 5
            f"{srcPath}name.basics.tsv.gz"          # 6
        ]

def main():

    session = (SparkSession.builder.master("local").appName("Spark Task App")
               .config(conf=SparkConf()).getOrCreate())

    TitleAkas = df.readTitleAkas(session, Files[0])
    TitleBasics = df.readTitleBasic(session, Files[1])
    TitleCrew = df.readTitleCrew(session, Files[2])
    TitleEpisode = df.readTitleEpisode(session, Files[3])
    TitlePrincipals = df.readTitlePrincipals(session, Files[4])
    TitleRatings = df.readTitleRatings(session, Files[5])
    NameBasics = df.readNameBasics(session, Files[6])

    print('Data Frames loaded...')

    Task01.Run(TitleAkas)
    Task02.Run(NameBasics, 19)
    Task03.Run(TitleBasics, 'movie', 120)
    Task04.Run(TitlePrincipals, NameBasics, TitleBasics)
    Task05.Run(TitleBasics, TitleAkas, 100)
    Task06.Run(TitleEpisode, TitleBasics, 50)
    Task07.Run(TitleBasics, TitleRatings, 10)
    Task08.Run(TitleBasics, TitleRatings, 10)

main()

