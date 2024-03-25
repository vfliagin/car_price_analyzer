# Анализ цен автомобилей

Репозиторий содержит сервисы для оценки стоимости автомобилей. Модель можно использовать, например, для рекомендации цены при размещении объявления о продаже на сайте. Для нахождения цены используется XGBRegressor. Данные для симуляции находятся в архиве. Визуализация содержит графики цен, встречающихся марок машин и карту, иллюстрирующую распределение предложений по штатам США. Для взаимодействия сервисов использовался сервер кафка, запущенный в докере с настройками:

```dockerfile
version: "3"
services:
  kafka:
    hostname: 'kafka'
    image: 'bitnami/kafka:latest'
    ports:
      - '9097:9097'
      - '9095:9095'
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9097,CONTROLLER://:9093,EXTERNAL://:9095
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9097,EXTERNAL://localhost:9095
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
```
