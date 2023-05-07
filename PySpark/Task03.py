import pyspark.sql.functions as f


def Run(data, film_type, t, path = "Result/03"):
    '''
    Get titles of all movies that last more than 2 hours.
    Args:
        data: dataframe from title.basics.tsv.gz
        film_type: film type (e.g. 'movie')
        t: time in minutes
    Returns:
        csv-file with result of task in 'Result/03'
    '''
    
    result = (data.filter((f.col('titleType') == film_type) & 
                          (f.col('runtimeMinutes') > t))
        .select('originalTitle', 'runtimeMinutes', 'titleType'))

    result.write.csv(path, header=True, mode='overwrite')
    
    # result.show(truncate=False)

    print('Task # 03 - done')

