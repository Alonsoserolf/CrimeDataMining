import numpy as np
import pandas as pd

stmt = "SELECT * FROM la_crime_clean"
print("Reading csv...")
df = pd.read_csv('~/VirtualBox VMs/share/la_crimes3.csv')
print("Done!")

# replace 'NA' with nan, so pandas recognizes it as a missing value
df = df.replace(r'NA', np.nan)
# replace any white space w/ NaN, for same reason as above
df = df.replace(r'^\s*$', np.nan, regex=True)

# ******************************* FILL MISSING AGE/SEX/DESCENT VALUES ***************************************

# Get relevant columns for filling
frame2 = df[['Crime.Code', 'Victim.Age', 'Victim.Sex', 'Victim.Descent']]
frame2 = frame2.dropna(how='any')

# group crimes by code & find mean age
grouped = frame2.groupby(['Crime.Code'])
age_means = grouped.agg({'Victim.Age': np.mean})
victim_modes = grouped.agg(lambda x: x.value_counts().index[0])

# mean & modes if we can't find examples grouped by crime type
age_mean_all = frame2.agg({'Victim.Age': np.mean})
victim_modes_all = frame2.agg(lambda x:x.value_counts().index[0])

codes = age_means.axes[0].tolist()

print("Filling ages...")
# Replace age w/ crime mean if it exists or total mean age
df['Victim.Age'] = df.groupby('Crime.Code')['Victim.Age'].apply(lambda x: x.fillna(x.mean()))
df['Victim.Age'] = df['Victim.Age'].fillna(df['Victim.Age'].mean())
df['Victim.Age'] = df['Victim.Age'].astype(np.int64)
print("Done!")

print("Filling sexes...")
# Replace sex w/ crime mode if it exists or total sex mode
df['Victim.Sex'] = df.groupby('Crime.Code')['Victim.Sex'].apply(lambda x: x.fillna(x.value_counts()))
df['Victim.Sex'] = df['Victim.Sex'].fillna(victim_modes_all['Victim.Sex'])
print("Done!")

print("Filling descent...")
# Replace descent w/ crime mode if it exists or total descent mode
df['Victim.Descent'] = df.groupby('Crime.Code')['Victim.Descent'].apply(lambda x: x.fillna(x.value_counts()))
df['Victim.Descent'] = df['Victim.Descent'].fillna(victim_modes_all['Victim.Descent'])
print("Done!")

# *******************************************************************************************
# Now that the most commonly missing values are filled, remove some unecessary columns

df.drop('Area.ID', axis=1, inplace=True)
df.drop('Reporting.District', axis=1, inplace=True)
df.drop('Crime.Code.1', axis = 1, inplace=True)
df.drop('Status.Code', axis=1, inplace=True)
df.drop('Address', axis=1, inplace=True)
df.drop('Cross.Street', axis=1, inplace=True)

# *******************************************************************************************
# Finally, check if there are any rows w/ important values missing

# SIGNIFICANT MISSING:
# Crime code descriptions
# Premise Codes
# Premise Descriptions
# Locations

# ******************************** Missing Crime Code Description ***************************
print("Filling in missing crime code descriptions...")

frame2 = df[['Crime.Code', 'Crime.Code.Description']]
frame2 = frame2.dropna(how='any')

ccodes = {}

print("Mapping crime codes to descriptions...")
for row in frame2.itertuples():
    if row[1] not in ccodes.keys():
        ccodes[row[1]] = row[2]
print("Done!")

df['Crime.Code.Description'] = df['Crime.Code.Description'].fillna(df['Crime.Code'].map(ccodes))

print("Dropping rows w/o crime description")
# Now drop all rows w/ out code description, since I can't think of any way to guess it
df = df[df['Crime.Code.Description'].notnull()]

# ******************************** Missing Premise Code/Description *************************
print("Filling in missing premise descriptions...")

frame2 = df[['Premise.Code', 'Premise.Description']]
frame2 = frame2.dropna(how='any')

# Find rows where the code is there, but description is missing
mask = (df['Premise.Description'].isnull()) & (df['Premise.Code'].notnull())

code_to_desc = {}

print("Mapping premise codes to descriptions...")
# Dictionary mapping premise codes to descriptions
for row in frame2.itertuples():
    if row[1] not in code_to_desc.keys():
        code_to_desc[row[1]] = row[2]
print("Done!")

df['Premise.Description'] = df['Premise.Description'].fillna(df['Premise.Code'].map(code_to_desc))

print("Dropping rows w/o premise data...")
# Now drop all rows w/out premise code/description, since I can't think of any way to guess it
df = df[(df['Premise.Code'].notnull() & df['Premise.Description'].notnull())]

# ******************************** Drop rows w/o location data ******************************
print("Dropping rows w/o location data...")
df = df[df['Location'].notnull()]

print("MISSING DESCRIPTION")
print(pd.isnull(df).sum())

print("Writing output to csv...")
df.to_csv('~/CrimeMining/CSV/la_clean.csv', index=False)
print("Done!")

print("Dropping vehicle/boat theft for la mining since victim info is bad")
df = df[df['Crime.Code'] != 510]
df = df[df['Crime.Code'] != 487]

print("Writing output to csv...")
df.to_csv('~/CrimeMining/CSV/la_clean_notheft.csv', index=False)
print("Done!")
