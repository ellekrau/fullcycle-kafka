setup:
	docker-compose up -d --build

app_container:
	docker exec -it gokafka bash