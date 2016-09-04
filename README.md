A personalized movie recommender system<br />
This repository has 4 scripts:<br />
1. 100KWithCosSimContBased.py: Uses 100k ratings and tests cosine similarity and uses content based to rank top 10.<br />
2. 100KWithCosSimContBasedUserNorm.py: Uses 100k user normalized ratings and tests cosine similarity and uses content based to rank top 10.<br />
3. 100KWithAdjCosSim.py: Uses 100k ratings and tests adjusted cosine similarity.<br />
4. 1MWithCosSimContBased.py: Uses 1M ratings and tests cosine similarity and uses content based to rank top 10. (Runs on cloud)<br />
It contains 4 files with data in them:<br />
1. u.data: 100 k movie ratings data<br />
2. u.item: 100 k movie name to ID mapping and content<br />
3. ratings.dat: 1M movie ratings data<br />
4. movies.dat : 1M movie name to ID mapping and content<br />
Notes on how to run last script on cloud:<br />
1. Put the folder ratings.dat and movies.dat in a S3 bucket on cloud<br />
2. Add accessKey and secretKey to access the s3 bucket in the script<br />
3. Change the path to the ratings.dat in the script<br />
3. Add script itself in the bucket<br />
4. Download movies.dat and script from S3 on master node<br />
5. Run the script using executor memory of 1g
