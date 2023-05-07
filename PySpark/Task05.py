import pyspark.sql.functions as f


def Run(basics, akas, n, path = 'Result/05'):
    '''
    Get information about how many adult movies/series etc. there are per region.
    Get the top n of them from the region with the biggest count 
    to the region with the smallest one.
    Args:
        basics: dataframe from name.basics.tsv.gz
        akas: dataframe from title.akas.tsv.gz
        n: top region
    Returns:
        csv-file with result of task in 'Result/05'
    '''

    adult = basics.filter((f.col('isAdult') == 1))
    result = adult.join(akas, adult.tconst == akas.titleId, how='left')
    result = result.groupBy('region').count().orderBy('count', ascending=False).limit(n)
    
    result.write.csv(path, header=True, mode='overwrite')

    # result_df.show(truncate=False)

    print('Task # 05 - done')
