SPARK_URL=spark://ec2-52-221-189-69.ap-southeast-1.compute.amazonaws.com:7077

init:
	pip install -r requirements.txt

deploy:
	spark-submit --master $(SPARK_URL) --packages org.apache.hadoop:hadoop-aws:2.7.6 main.py
