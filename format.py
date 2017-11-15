import pandas as pd

iter_csv = pd.read_table('./data/mapAffil_2016_nonull.tsv', sep='\t', encoding='latin-1',
                         dtype={'PMID': object, 
                                'au_order': np.int64, 
                                'year': np.int64,
                                'type': object, 
                                'city': object, 
                                'state':object, 
                                'country': object,
                                'lat': np.float64, 
                                'lon': np.float64, 
                                'fips': object},
                         iterator=True, chunksize=10**6)

mapAffil = pd.concat([chunk[pd.notnull(chunk['fips'])] for chunk in iter_csv])
mapAffil = mapAffil.drop(['Unnamed: 0'], 1)

### mortality data
mort = pd.read_table('./data/mortality.tsv',sep='\t', encoding='latin-1', na_values='Missing',
                    dtype={'fips': object})
#reshape
mort_rate = mort.loc[(mort.metric == 'rate')]
mort_expanded = mort_rate.drop(labels='metric', axis=1)
mort_expanded.index.name = 'original'
mort_expanded.set_index(['fips', 'cause'], append=True, inplace=True)
mort_expanded = mort_expanded.reorder_levels(['original','fips', 'cause'])
# .stack() removes NaN value rows
mort_expanded = mort_expanded.stack().to_frame().reset_index()
mort_expanded = mort_expanded.rename(columns={"level_3":"year", 0:"rate"})
#create year columns
mort_expanded.year = mort_expanded.year.str[1:]
mort_expanded['year'] = pd.to_numeric(mort_expanded['year'])

#Create mapAffil publication count columns
count_PMID_by_year_fips = mapAffil.groupby(['fips', 'year'])['PMID'].nunique()
count_PMID_by_year_fips = pd.DataFrame(count_PMID_by_year_fips).reset_index()
count_PMID_by_year_fips = count_PMID_by_year_fips.rename(columns={'PMID': 'PMID_count'})

# pivot years and pub count values
years_pubs = pd.pivot_table(count_PMID_by_year_fips, values='PMID_count', index=['fips'], columns=['year'])

### drop years before 1988
drop_yrs = []
for col in years_pubs.iloc[:,:]:
    if years_pubs[col].name < 1988:
        drop_yrs.append(years_pubs[col].name)

years_pubs = years_pubs.drop(labels=drop_yrs, axis=1)

years_pubs_headers = {}
for col in years_pubs.iloc[:,:]:
    year = years_pubs[col].name
    name = '{0}{1}'.format('pubs_', str(year))
    years_pubs_headers[year] = name
# print(years_pubs_headers)

# map new column names
years_pubs = years_pubs.rename(columns = years_pubs_headers)
years_pubs = years_pubs.reset_index()

### merge
mort_pubs = pd.merge(mort_expanded, years_pubs, on=['fips'], how='left')

#create pubs_cols variable
pubs_cols = list(mort_pubs.columns.str.contains('pubs'))
pubs_cols = list(mort_pubs.columns[pubs_cols])

#Replace pubs NaNs with 0s
mort_pubs.loc[:,pubs_cols] = mort_pubs.loc[:,pubs_cols].fillna(0)

### write?
# write out to tsv file in data folder
#mort_pubs.to_csv('./data/mort_pubs.tsv', sep='\t')

# add log column to count_PMID_by_year_fips
count_PMID_by_year_fips['log_PMID'] = np.log10(count_PMID_by_year_fips['PMID_count'] + 1)

#create dictionary of fips and pub change rate
delta_pubs_dict = {}
for fips in count_PMID_by_year_fips.fips.unique():
    #2017 data is incomplete, so remove those when calculating regression coefficient
    df = count_PMID_by_year_fips.loc[(count_PMID_by_year_fips.fips == fips) & (count_PMID_by_year_fips.year < 2017), ['fips', 'year', 'log_PMID']]
    df = df.drop_duplicates()
    #df['log_PMID'] = np.log10(df['PMID'] + 1)
    #display(df.head())

    regr = linear_model.LinearRegression()
    regr.fit(df['year'].to_frame(), df['log_PMID'].to_frame())
    #print(regr.coef_)
    delta_pubs_dict[fips] = regr.coef_[0][0]

#change dictionary to df for merge with mort_pubs
delta_pubs = pd.DataFrame.from_dict(delta_pubs_dict, orient='index').reset_index()
delta_pubs.columns = ['fips', 'delta_pub']
