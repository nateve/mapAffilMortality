import pandas as pd

iter_csv = pd.read_table('./data/mapaffil2016_3.tsv', sep='\t', encoding='latin-1',
                         dtype={'PMID': object, 
                                'au_order': np.int64,
                                'lastname': object,
                                'firstname': object,
                                'year': np.int64,
                                'type': object, 
                                'city': object, 
                                'state':object, 
                                'country': object,
                                'lat': np.float64, 
                                'lon': np.float64, 
                                'fips': object},
                         iterator=True, chunksize=10**6)
#remove rows where fips is null
mapAffil = pd.concat([chunk[pd.notnull(chunk['fips'])] for chunk in iter_csv])
mapAffil.reset_index(inplace=True)

#drop columns as necessary
mapAffil = mapAffil.drop(['index', 'lastname', 'firstname'], 1)

#write to csv file
mapAffil.to_csv('./data/mapAffil_2016_nonull.tsv', sep='\t')