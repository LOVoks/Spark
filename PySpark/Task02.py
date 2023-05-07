import pyspark.sql.functions as f


def Run(data, century, path = "Result/02"):
    '''
    Get the list of people’s names, who were born in the -th century.
    Args:
        data: dataframe from name.basics.tsv.gz
        century: century number
    Returns:
        csv-file with result of task in 'Result/02'
    '''
    
    start = (century-1)*100
    result = (data.filter((f.col('birthYear') >= start) & 
                          (f.col('birthYear') <= start + 99))
        .select('primaryName', 'birthYear'))

    result.write.csv(path, header=True, mode='overwrite')

    # result.show()

    print('Task # 02 - done')

