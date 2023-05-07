import pyspark.sql.functions as f


def Run(episode, basics, n, path = 'Result/06'):
    '''
    Get information about how many episodes in each TV Series.
    Get the top n of them starting from the TV Series with 
    the biggest quantity of episodes.
    Args:
        episode: dataframe from title.episode.tsv.gz
        basics: dataframe from name.basics.tsv.gz
        n: top regions
    Returns:
        csv-file with result of task in 'Result/06'
    '''

    result = episode.groupBy('parentTconst').count()
    result = result.join(basics, basics.tconst == result.parentTconst, 
                         how='left').select('originalTitle', 'count')
    result = result.orderBy('count', ascending=False).limit(n)

    result.write.csv(path, header=True, mode='overwrite')

    # result_df.show(truncate=False)

    print('Task # 06 - done')
