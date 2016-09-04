A personalized movie recommender system<br />
This repository has 4 scripts:<br />
1. 100KWithCosSimContBased.py: Uses 100k ratings and tests cosine similarity and uses content based to rank top 10.<br />
2. 100KWithCosSimContBasedUserNorm.py: Uses 100k user normalized ratings and tests cosine similarity and uses content based to rank top 10.<br />
3. 100KWithAdjCosSim.py: Uses 100k ratings and tests adjusted cosine similarity.<br />
4. 1MWithCosSimContBased.py: Uses 1M ratings and tests cosine similarity and uses content based to rank top 10. (Runs on cloud)<br />
It contains 2 directories with data in them:<br />
1. ml-1m: 1 million ratings data<br />
2. ml-100k: 100 k ratings data<br />
Notes on how to run last script on cloud:<br />
1. Put the folder ml-1m in a S3 bucket on cloud<br />
2. Add accessKey and secretKey to access the s3 bucket in the script<br />
3. Add script itself in the bucket<br />
4. Download ml-1m and script from S3 on master node<br />
5. Run the script using executor memory of 1g
