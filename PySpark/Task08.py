import pyspark.sql.functions as f


def Run(basics, ratings, n, path = 'Result/08'):
    '''
    Get n titles of the most popular movies/series etc. by each genre.
    Args:
        basics: dataframe from name.basics.tsv.gz
        ratings: dataframe from title.ratings.tsv.gz
        n: top titles
    Returns:
        csv-file with result of task in 'Result/08'
    '''
    
    genres = ['Adventure', 'Animation', 'Comedy', 'Drama', 'Family', 'Short']

    i = 1
    result = None
    for g in genres:
        tmp = basics.select('tconst', 'originalTitle',
                f.explode(basics.genres).alias('genre')).filter(f.col('genre') == g)
        tmp = (tmp.join(ratings, on='tconst', how='left')
                  .select('originalTitle', 'genre', 'averageRating'))
        tmp = tmp.orderBy('averageRating', ascending=False).limit(n)
        if i == 1:
            result = tmp
            i += 1
        else:
            result = result.union(tmp)

    result.write.csv(path, header=True, mode='overwrite')
    
    # result.show(60, truncate=False)

    print('Task # 08 - done')