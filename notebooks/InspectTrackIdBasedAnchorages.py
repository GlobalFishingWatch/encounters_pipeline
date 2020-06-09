# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import numpy as np
import pandas as pd
import pyseas
from pyseas import styles, maps
from pyseas.contrib import plot_tracks
from matplotlib import pyplot as plt
import shapely.geometry
from pandas.plotting import register_matplotlib_converters
from collections import defaultdict
import rendered
register_matplotlib_converters()

# %matplotlib inline
# -

query = """
WITH 

raw_ids as (
  SELECT vessel_1_id as id1, vessel_2_id as id2
  FROM `machine_learning_dev_ttl_120d.encounters_tracks_v20200602_`
  WHERE start_time BETWEEN TIMESTAMP('2019-12-15') and TIMESTAMP('2019-12-16') 
  GROUP BY id1, id2
  ORDER BY vessel_1_id, vessel_2_id
),

ids as (
    SELECT ROW_NUMBER() OVER () as encounter_no, id1, id2
    FROM raw_ids
  ORDER BY FARM_FINGERPRINT(id1), FARM_FINGERPRINT(id1)
  LIMIT 100
),

messages as (
  SELECT encounter_no, ssvid, track_id, lat, lon, timestamp
  FROM `machine_learning_dev_ttl_120d.thinned_messages_201912*`
  JOIN (select * from ids)
  ON (track_id = id1) or (track_id = id2)
)

SELECT * FROM messages
ORDER BY encounter_no, ssvid, track_id, timestamp
"""
encounter_msgs = pd.read_gbq(query, project_id='world-fishing-827')

track_ids = set(encounter_msgs.track_id)

query = """
select track_id, shipname.value as shipname_value, shipname.count as shipname_count
from `machine_learning_dev_ttl_120d.tracks_v20200520_20191231`
cross join unnest (shipnames) as shipname
WHERE track_id in ({})
""".format(','.join('"{}"'.format(x) for x in track_ids))
shipnames = pd.read_gbq(query, project_id='world-fishing-827')

shipname_map = defaultdict(lambda : defaultdict(int))
for x in shipnames.itertuples():
    shipname_map[x.track_id][x.shipname_value] += x.shipname_count

primary_shipname_map = {k : max(v.keys(), key=lambda x : v[x]) for
                            (k, v) in shipname_map.items() }

enc_nums = sorted(set(encounter_msgs.encounter_no))

for en in enc_nums:
    df  = encounter_msgs[encounter_msgs.encounter_no == en]
    ssvid = sorted(set(df.ssvid))
    if len(ssvid) == 1:
        continue
    # Sorted track_ids will allign with sorted ssvids since SSVID is the first
    # components
    track_id = sorted(set(df.track_id)) 
    for tid, sid in zip(track_id, ssvid):
        assert tid.split('-')[0] == sid
    names = [primary_shipname_map.get(x, '') for x in track_id]
    plt.figure(figsize=(12, 12))
    plot_tracks.plot_tracks_panel(df.timestamp, df.lon, df.lat, df.track_id)
    plt.title('{}: {}, {}\n{}, {}'.format(en, *(ssvid + names)))

# +
# rendered.publish_to_github('./InspectTrackIdBasedAnchorages.ipynb', 
#                            'pipe-encounters/notebooks', action='push')
# -


