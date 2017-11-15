import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz




def scrape_and_clean(url):
    """
    Scrapes a mesh tree from www.nlm.nih.gov/mesh/2015/mesh_trees/
    and returns a list of all the mesh terms
    """
    res = requests.get(url)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, 'lxml')
    toplevel = soup.find('ul', {'class':'Level1'})
    list_all = toplevel.get_text().split('\n')
    #print(list_all[0])
    list_all = list(filter(None, list_all))
    regex = re.compile(r'\[.*\]', re.IGNORECASE)
    clean_list = [regex.sub("", line).strip().lower() for line in list_all]
    return clean_list


def check_fuzzy(check_set, word):
    """
    Compares a term to each term in a set (e.g. mesh terms from `tree_dict`)
    and returns the maximum fuzzy match
    """
    ratios = []
    for i in check_set:
        ratio = fuzz.SequenceMatcher(isjunk=None,seq1=i, seq2=word).quick_ratio()
        ratios.append((i,ratio))
    maxval = max(ratios,key=lambda item:item[1])
    return maxval

def add_mesh_groups(df, dictionary):
    """
    Iterates over each mesh term in `cleantext` column of the df 
    and checks for it in the dictionary of mesh trees. When a match is found, 
    the tree category (e.g. 'circulatory','respiratory', 'neoplasms', 'mental','nervous')
    is added to a set in `mesh_groups` column of the df
    """
    for idx, row in df.iterrows():
        for term in row['cleantext']:
            for k,v in dictionary.items():
                if term in v:
                    if pd.isnull(row['mesh_groups']) == True:
                        row['mesh_groups'] = {k}
                    else:
                        row['mesh_groups'] = row['mesh_groups'].union({k})

# uncomment this to also add fuzzy matches over 0.9 to df
# (fn will take about 0.11 seconds per row in df):

#                elif check_fuzzy(v, term)[1] > 0.9:
#                    if pd.isnull(row['mesh_groups']) == True:
#                        row['mesh_groups'] = {k}
#                    else:
#                        row['mesh_groups'] = row['mesh_groups'].union({k})

                else:
                    continue

# Tree urls
circulatory = 'https://www.nlm.nih.gov/mesh/2015/mesh_trees/C14.html'
respiratory = 'https://www.nlm.nih.gov/mesh/2015/mesh_trees/C08.html'
neoplasms = 'https://www.nlm.nih.gov/mesh/2015/mesh_trees/C04.html'
nervous = 'https://www.nlm.nih.gov/mesh/2015/mesh_trees/C10.html'
mental = 'https://www.nlm.nih.gov/mesh/2015/mesh_trees/F03.html'

# Scrape trees and create a dictionary 
# Keys are the 5 tree categories and values are a set of their mesh terms
tree_dict = dict()
tree_dict['circulatory'] = set(scrape_and_clean(circulatory))
tree_dict['mental'] = set(scrape_and_clean(mental))
tree_dict['respiratory'] = set(scrape_and_clean(respiratory))
tree_dict['nervous'] = set(scrape_and_clean(nervous))
tree_dict['neoplasms'] = set(scrape_and_clean(neoplasms))


# Import data set
iter_csv = pd.read_table('./MeSH2016.tsv', 
                        sep='\t', 
                        encoding='latin-1',
                        dtype={'PMID': object},
                        iterator=True, 
                        chunksize=10**6)
mesh = pd.concat([chunk for chunk in iter_csv])

# Clean mesh terms in new column
mesh['cleantext'] = mesh.loc[:,'mesh'].str.lower().str.split('|')

# Add a new null column to populate with `mesh_groups`
mesh.loc[:,'mesh_groups'] = None

# Populate mesh_groups column
add_mesh_groups(mesh, tree_dict)

# Convert mesh_groups sets back into strings
mesh.loc[mesh.mesh_groups == set(),'mesh_groups'] = None
mesh.loc[mesh.mesh_groups != set(),'mesh_groups'] = mesh.loc[mesh.mesh_groups != set(),'mesh_groups'].str.join(',')

# Remove cleantext column
mesh = mesh.drop('cleantext',axis=1)

# Save as a .tsv in the working directory
mesh.to_csv('./mesh_groups.tsv', sep='\t', index=False)