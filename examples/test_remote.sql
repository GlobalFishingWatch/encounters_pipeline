SELECT
  lat        AS lat,
  lon        AS lon,
  speed      AS speed,
  timestamp  AS timestamp,
  mmsi       AS id
FROM
  TABLE_DATE_RANGE([world-fishing-827:pipeline_classify_p_p429_resampling_2.], 
                        TIMESTAMP('2016-01-01'), TIMESTAMP('2016-01-7'))
WHERE
  lat   IS NOT NULL AND
  lon   IS NOT NULL AND
  speed IS NOT NULL


