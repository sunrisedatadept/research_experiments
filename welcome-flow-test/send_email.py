import pandas as pd
import ipdb

group_voicemail = pd.read_csv('targets/group_voicemail.csv')
group_voicemail = group_voicemail[['vanid', 'particiapnt_group']]
ipdb.set_trace()