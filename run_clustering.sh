# #!/bin/bash

# export DATABASE_URL="postgresql://user:password@localhost:5401/mlsgrid"

# psql $DATABASE_URL -c "
# DELETE FROM property_clusters;

# -- Naye clusters ko insert karna Grid-Based Clustering ke logic ke saath
# INSERT INTO property_clusters (latitude, longitude, listingid, listprice, standardstatus, cluster_value)
# SELECT \"Latitude\", \"Longitude\", \"ListingId\", \"ListPrice\", \"StandardStatus\",
#        FLOOR(CAST(\"Latitude\" AS NUMERIC) * 10) + FLOOR(CAST(\"Longitude\" AS NUMERIC) * 10) AS cluster_value
# FROM merged_property
# WHERE \"StandardStatus\" IN ('Active', 'Pending', 'Coming Soon');
# "
