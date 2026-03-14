connect to this psql "postgresql://neondb_owner:npg_y5gFCSeLB2pz@ep-snowy-forest-adlwtnc0.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require", we will explore it


give me more advanced stats


write all the queries so far into a queries.sql file


write an intermediate sales transaction table to my disk 


write a script executable via uv (use uv add --script if you need to) that analizes sales_transactions.csv, use altair for dataviz and polars for dataframe operations.
we don't need pyproject, all scripts are self executable


add a previous step in the script that is redownloading sales_transactions.csv


add a visualization dashboard via streamlit


i think we now need a full uv init and then uv add the libraries


we need to version the downloads, move the downloads to a data/raw folder and add date and time. streamlit just takes the last version.


we have too much files being created, create a duckdb database (uv add duckdb if you need to) and let's store partitioned by download data and time.



give me queries to explore the partitions via uv run duckdb 