import pyspark.sql.functions as f

def Run(data, path = 'Result/01'):
    '''
    Get all titles of series/movies etc. that are available in UA region.
    Args:
        data: dataframe from title.akas.tsv.gz
    Returns:
        csv-file with result of task in 'Result/01'
    '''
    
    result = data.filter(f.col('region') == 'UA').select('title', 'region')
    result.write.csv(path, header=True, mode='overwrite')

    #result.show()

    print('Task # 01 - done')

