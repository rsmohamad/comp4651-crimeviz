SPARK_URL=spark://ec2-18-136-212-151.ap-southeast-1.compute.amazonaws.com:7077

init:
	pip install -r requirements.txt

deploy:
	spark-submit --master $(SPARK_URL) --packages org.apache.hadoop:hadoop-aws:2.7.6 main.py

