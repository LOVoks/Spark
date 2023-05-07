import pyspark.sql.functions as f


def Run(basics, ratings, n, path = 'Result/07'):
    '''
    Get n titles of the most popular movies/series etc. by each decade.
    Args:
        basics: dataframe from name.basics.tsv.gz
        ratings: dataframe from title.ratings.tsv.gz
        n: top titles
    Returns:
        csv-file with result of task in 'Result/07'
    '''
    
    i = 1
    result = None
    for start in range(1950, 2020, n):
        tmp = basics.filter((f.col('startYear') >= start) & (f.col('startYear') <= start + 9))
        tmp = tmp.withColumn('decade', f.lit(str(start) + '-' + str(start + 9)))
        tmp = (tmp.join(ratings, on='tconst', how='left')
                  .select('decade', 'originalTitle', 'startYear', 'averageRating'))
        tmp = tmp.orderBy('averageRating', ascending=False).limit(10)
        if i == 1:
            result = tmp
            i += 1
        else:
            result = result.union(tmp)

    result.write.csv(path, header=True, mode='overwrite')
    
    # result.show(100, truncate=False)

    print('Task # 07 - done')
