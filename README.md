# Pulse

Pulse представляет собой self-hosted систему для мониторинга доступности сайтов
и HTTP-сервисов. Проект написан на Go и создаётся как практическая работа с
backend-разработкой и event-driven архитектурой.

В будущем Pulse будет регулярно проверять заданные URL, сохранять историю
проверок, показывать текущее состояние сервисов и отправлять уведомления при
падении или восстановлении.

Сейчас завершён второй этап разработки. Уже можно создавать и настраивать
мониторы через HTTP API, а их данные сохраняются в PostgreSQL.

## Архитектура

Проект состоит из трёх отдельных долгоживущих Go-сервисов:

- `api` принимает HTTP-запросы и управляет мониторами
- `pinger` будет проверять доступность сайтов и измерять время ответа
- `consumer` будет обрабатывать результаты проверок

Отдельная утилита `migrate` применяет изменения схемы PostgreSQL перед запуском
API и сразу завершает работу.

Планируемый поток данных выглядит так:

```text
Monitor
  -> Pinger
  -> Kafka
  -> Consumer
  -> PostgreSQL и Redis
  -> Notification
```

Сервисы запускаются независимо друг от друга. Это позволит масштабировать их
отдельно, например запускать несколько экземпляров Pinger.

## Что уже работает

Сейчас реализовано:

- три отдельно собираемых сервиса: API, Pinger и Consumer
- одноразовая утилита для применения миграций
- CRUD API для управления мониторами
- хранение мониторов в PostgreSQL через `pgx`
- версионируемые SQL-миграции с отдельным migration runner
- маршрутизация HTTP-запросов через `chi`
- конфигурация через переменные окружения
- проверка конфигурации при запуске
- структурированные JSON-логи через `log/slog`
- корректное завершение работы по `SIGINT` и `SIGTERM`
- PostgreSQL, Redis и Kafka в Docker Compose
- Kafka в одноузловом KRaft-режиме
- multi-stage Docker-сборка
- запуск Go-сервисов от непривилегированного пользователя
- проверка состояния API через `GET /healthz`

Pinger и Consumer пока не выполняют бизнес-логику. На этом этапе они нужны для
проверки конфигурации, сборки, запуска и корректной остановки сервисов.

## Стек

- Go 1.23
- PostgreSQL 18
- Redis 8
- Apache Kafka 4
- Docker и Docker Compose

На следующих этапах появятся `go-redis`, `kafka-go`, Prometheus и интеграция с
Telegram Bot API.

## Быстрый запуск

Для запуска понадобится Docker с поддержкой Docker Compose v2.

Клонируйте репозиторий, перейдите в папку проекта и выполните:

```bash
docker compose up --build
```

Compose соберёт Go-приложения, запустит инфраструктуру, применит миграции и
дождётся готовности API.

После запуска API будет доступен по адресу:

```text
http://localhost:8080
```

Проверить его можно командой:

```bash
curl http://localhost:8080/healthz
```

Ожидаемый ответ:

```json
{"status":"ok"}
```

Посмотреть состояние контейнеров:

```bash
docker compose ps
```

Посмотреть логи:

```bash
docker compose logs -f
```

Остановить проект:

```bash
docker compose down
```

PostgreSQL, Redis и Kafka используют именованные Docker volumes, поэтому данные
сохраняются после обычной остановки.

Чтобы остановить проект и удалить локальные данные:

```bash
docker compose down --volumes
```

Внимание: последняя команда удаляет данные без возможности восстановления.

## Работа с monitors

Создать monitor:

```bash
curl -X POST http://localhost:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Example",
    "url": "https://example.com",
    "interval_seconds": 60,
    "timeout_ms": 5000
  }'
```

Если не передавать дополнительные настройки, API использует `GET`, ожидаемый
HTTP status `200` и включает monitor сразу после создания.

Получить список monitors:

```bash
curl http://localhost:8080/api/v1/monitors
```

Получить один monitor:

```bash
curl http://localhost:8080/api/v1/monitors/{id}
```

Изменить monitor:

```bash
curl -X PATCH http://localhost:8080/api/v1/monitors/{id} \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

Удалить monitor:

```bash
curl -X DELETE http://localhost:8080/api/v1/monitors/{id}
```

Поддерживаются методы проверки `GET` и `HEAD`. Интервал задаётся в секундах,
timeout в миллисекундах. API проверяет UUID, URL, HTTP method, интервалы и
ожидаемый status code и возвращает ошибки в JSON.

## Локальный запуск Go-сервисов

Каждое приложение можно запустить отдельно:

```bash
go run ./cmd/api
go run ./cmd/pinger
go run ./cmd/consumer
```

Значения по умолчанию рассчитаны на инфраструктуру, запущенную через Docker
Compose с опубликованными локальными портами.

Пример доступных переменных находится в `.env.example`. Docker Compose читает
файл `.env` автоматически, но сами Go-приложения этого не делают. При запуске
через `go run` переменные нужно экспортировать в текущей оболочке.

## Основные настройки

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `PULSE_ENV` | `development` | Название окружения в логах |
| `PULSE_LOG_LEVEL` | `info` | Уровень логирования |
| `PULSE_HTTP_ADDR` | `:8080` | Адрес API |
| `PULSE_SHUTDOWN_TIMEOUT` | `10s` | Время на корректную остановку HTTP-сервера |
| `PULSE_POSTGRES_URL` | локальный DSN | Подключение к PostgreSQL |
| `PULSE_REDIS_ADDR` | `localhost:6379` | Адрес Redis |
| `PULSE_KAFKA_BROKERS` | `localhost:9092` | Список Kafka brokers через запятую |
| `PULSE_KAFKA_CHECK_RESULTS_TOPIC` | `check.result` | Topic с результатами проверок |

Значения в Docker Compose предназначены только для локальной разработки.
Секреты не следует добавлять в репозиторий. Локальный файл `.env` уже указан в
`.gitignore`.

## Структура проекта

```text
cmd/
  api/              точка входа API
  pinger/           точка входа Pinger
  consumer/         точка входа Consumer
  migrate/          запуск миграций PostgreSQL

internal/
  app/              общий запуск и обработка сигналов
  config/           загрузка и проверка конфигурации
  logging/          настройка структурированных логов
  api/              HTTP-сервер и handlers
  monitor/          модель monitor и правила валидации
  postgres/         пул соединений и PostgreSQL store
  migrate/          применение миграций
  pinger/           процесс Pinger
  consumer/         процесс Consumer

docker/             общий Dockerfile для Go-сервисов
migrations/         SQL-миграции и их встраивание в binary
deploy/k8s/         будущие Kubernetes manifests
docker-compose.yml  локальное окружение проекта
```

## Проверка кода

```bash
go fmt ./...
go vet ./...
go test ./...
docker compose config --quiet
```

## Что дальше

Следующий этап посвящён одному экземпляру Pinger. Он будет получать активные
monitors из PostgreSQL, запускать HTTP-проверки по расписанию, учитывать timeout
и ограничивать количество одновременных запросов. Результаты проверок на этом
этапе будут выводиться в структурированные логи.

Kafka, Redis state и Telegram notifications будут подключаться позже согласно
roadmap проекта.
