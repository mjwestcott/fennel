run:
	modd -f modd.conf

benchmark:
	pytest -c /dev/null tests/performance --benchmark-sort=mean --benchmark-columns=mean,median,min,max,stddev,rounds
