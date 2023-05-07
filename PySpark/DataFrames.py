import pyspark.sql.types as t
import pyspark.sql.functions as f

def getColomn(name):
    return f.when(f.col(name) == r'\N', None).otherwise(f.col(name))

def readTitleAkas(session, path):
    '''
    Read 'data/title.akas.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
        path: Full file name
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('titleId', t.StringType(), False),
                             t.StructField('ordering', t.IntegerType(), False),
                             t.StructField('title', t.StringType(), True),
                             t.StructField('region', t.StringType(), True),
                             t.StructField('language', t.StringType(), True),
                             t.StructField('types', t.StringType(), True),
                             t.StructField('attributes', t.StringType(), True),
                             t.StructField('isOriginalTitle', t.IntegerType(), True) ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)

    df = df.withColumn('region', getColomn('region'))
    df = df.withColumn('language', getColomn('language'))
    df = df.withColumn('types', getColomn('types'))
    df = df.withColumn('attributes', getColomn('attributes'))

    # df.show(truncate=False)

    return df

def readTitleBasic(session, path):
    '''
    Read 'data/title.basics.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
        path: Full file name
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('tconst', t.StringType(), False),
                             t.StructField('titleType', t.StringType(), True),
                             t.StructField('primaryTitle', t.StringType(), True),
                             t.StructField('originalTitle', t.StringType(), True),
                             t.StructField('isAdult', t.IntegerType(), True),
                             t.StructField('startYear', t.IntegerType(), True),
                             t.StructField('endYear', t.DateType(), True),
                             t.StructField('runtimeMinutes', t.IntegerType(), True),
                             t.StructField('genres', t.StringType(), True)  ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)
    df = df.withColumn('genres', 
                       f.when(f.col('genres') == r'\N', None).otherwise(f.split(df.genres, ',')))

    # df.show(truncate=False)

    return df

def readTitleCrew(session, path):
    '''
    Read 'data/title.crew.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
        path: Full file name
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('tconst', t.StringType(), False),
                             t.StructField('directors', t.StringType(), True),
                             t.StructField('writers', t.StringType(), True)    ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)
    df = df.withColumn('writers', getColomn('writers'))

    df.show(truncate=False)

    return df

def readTitleEpisode(session, path):
    '''
    Read 'data/title.episode.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
        path: Full file name
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('tconst', t.StringType(), False),
                             t.StructField('parentTconst', t.StringType(), True),
                             t.StructField('seasonNumber', t.IntegerType(), True),
                             t.StructField('episodeNumber', t.IntegerType(), True)  ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)

    # df.show(truncate=False)

    return df


def readTitlePrincipals(session, path):
    '''
    Read 'data/title.principals.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('tconst', t.StringType(), False),
                                            t.StructField('ordering', t.IntegerType(), True),
                                            t.StructField('nconst', t.StringType(), True),
                                            t.StructField('category', t.StringType(), True),
                                            t.StructField('job', t.StringType(), True),
                                            t.StructField('characters', t.StringType(), True)
                                            ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null',
                                                 schema=dfSchema)
    df = df.withColumn('job', getColomn('job'))
    df = df.withColumn('characters', getColomn('characters'))

    # df.show(truncate=False)

    return df


# path = 'data/title.ratings.tsv.gz'
def readTitleRatings(session, path):
    '''
    Read 'data/title.ratings.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('tconst', t.StringType(), False),
                             t.StructField('averageRating', t.DoubleType(), True),
                             t.StructField('numVotes', t.IntegerType(), True)  ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)

    return df


# path = 'data/name.basics.tsv.gz'
def readNameBasics(session, path):
    '''
    Read 'data/name.basics.tsv.gz'-file in DataFrame
    Args:
        session: Spark session
    Returns:
        DataFrame
    '''
    dfSchema = t.StructType([t.StructField('nconst', t.StringType(), False),
                             t.StructField('primaryName', t.StringType(), True),
                             t.StructField('birthYear', t.IntegerType(), True),
                             t.StructField('deathYear', t.IntegerType(), True),
                             t.StructField('primaryProfession', t.StringType(), True),
                             t.StructField('knownForTitles', t.StringType(), True)   ])

    df = session.read.csv(path, sep=r'\t', header=True, nullValue='null', schema=dfSchema)

    return df
