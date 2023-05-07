import pyspark.sql.functions as f


def Run(data, names, titles, path = 'Result/04'):
    '''
    Get names of people, corresponding movies/series 
    and characters they played in those films.
    Args:
        data: dataframe from title.principals.tsv.gz
        names: dataframe from name.basics.tsv.gz
        titles: dataframe from name.basics.tsv.gz
    Returns:
        csv-file with result of task in 'Result/04'
    '''

    result = (data.filter(f.col('category') == 'actor')
        .select('tconst', 'nconst', 'category', 'characters'))
    result = (names.join(result, on='nconst', how='left')
        .select('primaryName', 'characters', 'tconst'))
    result = (result.join(titles, on='tconst', how='left')
        .select('primaryName', 'originalTitle', 'characters'))
    result.write.csv(path, header=True, mode='overwrite')

    # result.show(truncate=False)

    print('Task # 04 - done')
