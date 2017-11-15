#DATA
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

iter_csv = pd.read_csv('./data/mesh_groups.tsv', 
                        sep='\t',
                        dtype={'PMID': object}, 
                        iterator=True, 
                        chunksize=10**6)

mdf = pd.concat([chunk for chunk in iter_csv])

mdf_nonull = mdf.loc[pd.isnull(mdf.mesh_groups) == False, ['PMID', 'mesh_groups']]
mdf_nonull.is_copy = False

merge = pd.merge(mdf_nonull, mapAffil, how='right', on=['PMID'])
# remove all years before 1987
merge = merge.loc[(merge.year > 1987),:]

# split mesh_groups into list
merge.loc[(pd.isnull(merge.mesh_groups) == False),'mesh_groups'] = merge.loc[(pd.isnull(merge.mesh_groups) == False),'mesh_groups'].str.split(',')

# cast types into list
merge.loc[(pd.isnull(merge.type) == False),'type'] = merge.loc[(pd.isnull(merge.type) == False),'type'].str.split(',')
merge.head(10)

# # convert nulls to empty lists for summing counter objects
for row in merge.loc[merge.mesh_groups.isnull(), 'mesh_groups'].index:
    merge.at[row, 'mesh_groups'] = [np.nan]
    
# # convert nulls to empty lists for summing counter objects
for row in merge.loc[merge.type.isnull(), 'type'].index:
    merge.at[row, 'type'] = [np.nan]
    

def count_groups_PMID(x):
    return pd.Series(dict(
        mesh_groups = list(set(x['mesh_groups'].sum())),
        type_groups = list(set(x['type'].sum()))))

def count_groups_papers(x):
    return pd.Series(dict(
        PMID_count = x['PMID'].nunique(),
        mesh_groups = Counter(x['mesh_groups'].sum()),
        type_groups = Counter(x['type_groups'].sum())))

def count_groups_authors(x):
    return pd.Series(dict(
        #count rows, authors instead of unqiue papers
        author_count = len(x),
        mesh_groups = Counter(x['mesh_groups'].sum()),
        type_groups = Counter(x['type'].sum())))

count_authors_mesh_groups = merge.groupby(['fips','year']).apply(count_groups_authors)
count_authors_mesh_groups = count_authors_mesh_groups.reset_index()

count_PMID_by_groups = merge.groupby(['fips','year', 'PMID']).apply(count_groups_PMID)
count_PMID_by_groups = count_PMID_by_groups.reset_index()
count_PMID_mesh_groups = count_PMID_by_groups.groupby(['fips','year']).apply(count_groups_papers)
count_PMID_mesh_groups = count_PMID_mesh_groups.reset_index()

for idx,row in count_PMID_mesh_groups.iterrows():
    for k,v in dict(row['mesh_groups']).items():
        if (len(dict(row['mesh_groups']).items()) == 1) & (type(k) == np.float):
            count_PMID_mesh_groups.loc[idx,'no_mesh'] = True
            count_PMID_mesh_groups.loc[idx, 'nan'] = v
        elif k == 'circulatory':
            count_PMID_mesh_groups.loc[idx, 'circulatory'] = v
            count_PMID_mesh_groups.loc[idx, 'circulatory_pct'] = v/count_PMID_mesh_groups.loc[idx,'PMID_count']
        elif k == 'nervous':
            count_PMID_mesh_groups.loc[idx, 'nervous'] = v
            count_PMID_mesh_groups.loc[idx, 'nervous_pct'] = v/count_PMID_mesh_groups.loc[idx,'PMID_count']
        elif k == 'mental':
            count_PMID_mesh_groups.loc[idx, 'mental'] = v
            count_PMID_mesh_groups.loc[idx, 'mental_pct'] = v/count_PMID_mesh_groups.loc[idx,'PMID_count']
        elif k == 'neoplasms':
            count_PMID_mesh_groups.loc[idx, 'neoplasms'] = v
            count_PMID_mesh_groups.loc[idx, 'neoplasms_pct'] = v/count_PMID_mesh_groups.loc[idx,'PMID_count']
        elif k == 'respiratory':
            count_PMID_mesh_groups.loc[idx, 'respiratory'] = v
            count_PMID_mesh_groups.loc[idx, 'respiratory_pct'] = v/count_PMID_mesh_groups.loc[idx,'PMID_count']
        elif type(k) == np.float:
            count_PMID_mesh_groups.loc[idx, 'nan'] = v
        else:
            continue


# authors
for idx,row in count_authors_mesh_groups.iterrows():
    for k,v in dict(row['mesh_groups']).items():
        if (len(dict(row['mesh_groups']).items()) == 1) & (type(k) == np.float):
            count_authors_mesh_groups.loc[idx,'no_mesh'] = True
#             count_authors_mesh_groups.loc[idx, 'nan'] = v
        elif k == 'circulatory':
            count_authors_mesh_groups.loc[idx, 'circulatory'] = v
            count_authors_mesh_groups.loc[idx, 'circulatory_pct'] = v/count_authors_mesh_groups.loc[idx,'author_count']
        elif k == 'nervous':
            count_authors_mesh_groups.loc[idx, 'nervous'] = v
            count_authors_mesh_groups.loc[idx, 'nervous_pct'] = v/count_authors_mesh_groups.loc[idx,'author_count']
        elif k == 'mental':
            count_authors_mesh_groups.loc[idx, 'mental'] = v
            count_authors_mesh_groups.loc[idx, 'mental_pct'] = v/count_authors_mesh_groups.loc[idx,'author_count']
        elif k == 'neoplasms':
            count_authors_mesh_groups.loc[idx, 'neoplasms'] = v
            count_authors_mesh_groups.loc[idx, 'neoplasms_pct'] = v/count_authors_mesh_groups.loc[idx,'author_count']
        elif k == 'respiratory':
            count_authors_mesh_groups.loc[idx, 'respiratory'] = v
            count_authors_mesh_groups.loc[idx, 'respiratory_pct'] = v/count_authors_mesh_groups.loc[idx,'author_count']
        elif type(k) == np.float:
            count_authors_mesh_groups.loc[idx, 'nan'] = v
        else:
            continue