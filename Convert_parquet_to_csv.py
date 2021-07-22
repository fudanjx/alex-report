import pandas as pd
import path as PT

source_filename = 'Combined_SOC.parquet.gzip'
dest_filename = source_filename[0:-4]+'csv'
df = pd.read_parquet(PT.path_wip_output + source_filename)
df.to_csv(PT.path_wip_output + dest_filename)
df.sample(n=10000, random_state = 1).to_csv(PT.path_wip_output + 'sample_' + dest_filename)

