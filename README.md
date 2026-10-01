# Pulse

Pulse представляет собой self-hosted систему для мониторинга доступности сайтов
и HTTP-сервисов. Проект написан на Go и создаётся как практическая работа с
backend-разработкой и event-driven архитектурой.

Pulse регулярно проверяет заданные URL, сохраняет историю, показывает текущее
состояние сервисов и отправляет уведомления при падении или восстановлении.

Сейчас завершён десятый этап разработки. Monitors можно создавать и управлять
ими через web UI или HTTP API. Pinger проверяет активные URL по расписанию,
отправляет результаты в Kafka, а Consumer сохраняет историю в PostgreSQL и
актуальное состояние в Redis. При падении и восстановлении Pulse может
отправить сообщение в Telegram.

## Архитектура

Проект состоит из трёх отдельных долгоживущих Go-сервисов:

- `api` принимает HTTP-запросы и управляет мониторами
- `pinger` проверяет доступность сайтов и измеряет время ответа
- `consumer` читает результаты из Kafka, сохраняет историю, обновляет статус и
  обрабатывает уведомления

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
- загрузка активных monitors из PostgreSQL
- планирование проверок по индивидуальным интервалам
- HTTP-проверки с timeout и измерением latency
- ограниченный worker pool без неконтролируемых goroutines
- классификация timeout, network error и неожиданного HTTP status
- публикация каждого результата в Kafka topic `check.result`
- JSON-события с уникальным `event_id`
- Kafka key равный `monitor_id` для сохранения порядка событий одного monitor
- синхронная отправка в Kafka с `acks=all` и ограниченным timeout
- Consumer group `pulse-consumer` с ручным подтверждением Kafka offsets
- идемпотентная запись истории по уникальному `event_id`
- проверка JSON-событий и соответствия Kafka key полю `monitor_id`
- таблица `checks` с индексом для будущего получения истории monitor
- актуальное состояние каждого monitor в Redis без ограничения срока хранения
- состояния `UNKNOWN`, `UP` и `DOWN`
- получение состояния через `GET /api/v1/monitors/{id}/status`
- получение последних проверок через `GET /api/v1/monitors/{id}/checks`
- проверка существования monitor перед чтением состояния из Redis
- обнаружение переходов `UP -> DOWN` и `DOWN -> UP`
- отсутствие уведомлений при начальном переходе из `UNKNOWN`
- журнал переходов и статуса доставки в PostgreSQL
- Telegram alerts с повторной попыткой через Kafka при временной ошибке
- защита от обычной повторной отправки alert по тому же `event_id`
- несколько экземпляров Pinger в Docker Compose
- Redis locks с TTL для защиты от одновременной проверки одного monitor
- Prometheus metrics без high-cardinality labels
- endpoints `/metrics`, `/healthz`, `/livez` и `/readyz`
- web UI со сводкой, текущими статусами и историей проверок
- создание, приостановка, возобновление и удаление monitors через web UI
- конфигурация через переменные окружения
- проверка конфигурации при запуске
- структурированные JSON-логи через `log/slog`
- корректное завершение работы по `SIGINT` и `SIGTERM`
- PostgreSQL, Redis и Kafka в Docker Compose
- Kafka в одноузловом KRaft-режиме
- автоматическое создание topic `check.result` перед запуском Pinger
- multi-stage Docker-сборка
- запуск Go-сервисов от непривилегированного пользователя
- проверка состояния API через `GET /healthz`

## Стек

- Go 1.23
- PostgreSQL 18
- Redis 8
- Apache Kafka 4
- Docker и Docker Compose

Для работы с Redis используется `go-redis`, для Kafka используется `kafka-go`,
а Telegram Bot API вызывается стандартным HTTP-клиентом Go. Redis координирует
несколько экземпляров Pinger, а Prometheus собирает технические metrics. На
следующих этапах проект получит дополнительные integration tests и Kubernetes
manifests.

## Быстрый запуск

Для запуска понадобится Docker с поддержкой Docker Compose v2.

Клонируйте репозиторий, перейдите в папку проекта и выполните:

```bash
docker compose up --build
```

Compose соберёт Go-приложения, запустит инфраструктуру и два экземпляра Pinger,
применит миграции, создаст Kafka topic `check.result` с тремя partitions и
дождётся готовности API.

После запуска API будет доступен по адресу:

```text
http://localhost:8080
```

По этому адресу открывается web UI. В нём можно добавить monitor, увидеть
текущий статус и latency, открыть последние 50 проверок, временно остановить
проверки или удалить monitor. Страница автоматически обновляется раз в 30
секунд.

В web UI пока нет авторизации. Не публикуйте API-порт в интернет без reverse
proxy, HTTPS и контроля доступа.

Prometheus будет доступен по адресу:

```text
http://localhost:9091
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

Получить его текущее состояние:

```bash
curl http://localhost:8080/api/v1/monitors/{id}/status
```

После успешной проверки ответ выглядит примерно так:

```json
{
  "state": {
    "monitor_id": "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
    "status": "UP",
    "checked_at": "2026-09-11T12:00:00Z",
    "status_code": 200,
    "latency_ms": 42,
    "error": null
  }
}
```

Если monitor существует, но ещё ни разу не проверялся, API возвращает статус
`UNKNOWN`. Поля последней проверки в таком ответе равны `null`.

Получить последние проверки monitor:

```bash
curl "http://localhost:8080/api/v1/monitors/{id}/checks?limit=50"
```

Параметр `limit` необязателен. Допустимы значения от `1` до `200`, по умолчанию
API возвращает последние `50` результатов.

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

## HTTP-проверки

Pinger загружает активные monitors из PostgreSQL. Новый monitor проверяется при
ближайшем цикле планировщика, а следующие проверки запускаются через заданный
`interval_seconds`.

По умолчанию Docker Compose запускает два экземпляра Pinger. Посмотреть их
результаты публикации можно одной командой:

```bash
docker compose logs -f pinger
```

Пример успешной проверки:

```json
{
  "level": "INFO",
  "msg": "check result published",
  "event_id": "...",
  "monitor_id": "...",
  "success": true,
  "status_code": 200,
  "latency_ms": 142
}
```

Неуспешные проверки записываются с уровнем `WARN` и полями `error_kind` и
`error`. Возможные категории: `timeout`, `network`, `request` и
`unexpected_status`. Ошибка отправки в Kafka записывается отдельно с уровнем
`ERROR`.

## Несколько экземпляров Pinger

Перед HTTP-запросом worker пытается создать в Redis ключ
`monitor:{id}:lock` командой `SET NX` с TTL. В значении хранится уникальный ID
экземпляра и локального worker. Только получивший lock worker выполняет проверку
и публикует результат в Kafka. Остальные экземпляры пропускают этот цикл.

Во время проверки TTL складывается из timeout конкретного monitor, timeout
публикации в Kafka и небольшого запаса `PULSE_PINGER_LOCK_GRACE`. После
публикации worker атомарно сокращает TTL до оставшейся части
`interval_seconds`. Поэтому другой экземпляр не повторит уже завершённую
проверку в том же интервале. Если Pinger аварийно завершится, Redis сам удалит
lock после TTL. Любое изменение lock выполняется только при совпадении ID
владельца, поэтому старый worker не может изменить уже обновлённый lock.

Изменить число экземпляров можно через `.env`:

```dotenv
PULSE_PINGER_REPLICAS=3
```

Или только для одного запуска:

```bash
docker compose up --build --scale pinger=3
```

Если Redis временно недоступен, Pinger не выполняет проверку без lock. Ошибка
появляется в логах, а следующий цикл снова пытается получить lock. Такой выбор
защищает от duplicate checks во время проблем с координацией.

## События Kafka

После каждой проверки Pinger публикует одно JSON-событие в topic `check.result`:

```json
{
  "event_id": "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
  "monitor_id": "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
  "checked_at": "2026-09-10T16:30:00Z",
  "success": true,
  "status_code": 200,
  "latency_ms": 142,
  "error": null
}
```

Для неуспешной проверки событие также содержит `error_kind`, а `error` содержит
описание причины. Kafka key всегда равен `monitor_id`. Благодаря этому события
одного monitor попадают в одну partition и сохраняют порядок.

Pinger использует синхронную отправку и ждёт подтверждения Kafka. `kafka-go`
повторяет временно неудачные попытки. Если публикация так и не удалась до
истечения timeout, Pinger пишет ошибку в лог и продолжает работу. Локального
хранилища неотправленных событий на этом этапе нет.

При запуске через Docker Compose topic создаётся автоматически одноразовым
сервисом `kafka-init`. Если Pinger запускается через `go run`, topic нужно заранее
создать в используемом Kafka cluster.

Посмотреть сырые события в локальном окружении можно так:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic check.result \
  --from-beginning
```

## Consumer и история проверок

Consumer входит в Kafka group `pulse-consumer` и последовательно обрабатывает
сообщения. Для каждого события порядок действий такой:

```text
прочитать сообщение
→ проверить JSON и Kafka key
→ записать результат в PostgreSQL
→ прочитать прошлое состояние из Redis
→ определить и сохранить переход состояния
→ отправить или пропустить Telegram notification
→ обновить текущее состояние в Redis
→ подтвердить Kafka offset
```

Если PostgreSQL, Redis или настроенный Telegram недоступен либо commit offset
завершился ошибкой, Consumer останавливается. Docker перезапускает процесс,
после чего Kafka повторно отдаёт неподтверждённое сообщение. Повторная доставка
безопасна, потому что `event_id` является первичным ключом таблиц `checks` и
`status_transitions`, а запись в Redis заменяет прошлое текущее состояние.

У таблицы `checks` намеренно нет foreign key на `monitors`. Monitor может быть
удалён, пока его результат ещё находится в Kafka. История при этом сохраняется,
а сообщение не блокирует обработку всей partition.

Посмотреть последние сохранённые проверки:

```bash
docker compose exec postgres psql -U pulse -d pulse -c \
  "SELECT event_id, monitor_id, checked_at, success, status_code, latency_ms, error FROM checks ORDER BY checked_at DESC LIMIT 20;"
```

На этом этапе malformed message не подтверждается и останавливает Consumer.
Dead letter queue будет отдельным улучшением после завершения основного MVP.

## Текущее состояние в Redis

Consumer преобразует каждый результат проверки в одно из двух состояний:

- `UP`, если HTTP-проверка прошла успешно
- `DOWN`, если возникла ошибка или получен неожиданный HTTP status

Состояние хранится в Redis под ключом `monitor:{id}:status` в формате JSON.
Redis содержит только последний результат и нужен для быстрого чтения через
API. Полная история остаётся в PostgreSQL. У ключа нет TTL, поэтому редкие
проверки не превращаются в `UNKNOWN` только из-за прошедшего времени. Поле
`last_status_change_at` сохраняется при повторных результатах того же типа и
меняется только вместе со статусом.

Если ключ отсутствует, это не считается ошибкой Redis. API возвращает
`UNKNOWN`, потому что monitor мог быть создан совсем недавно. Перед чтением
Redis API проверяет monitor в PostgreSQL, поэтому для удалённого или
несуществующего ID возвращается `404`, даже если старый ключ ещё сохранился.

Посмотреть значение напрямую в локальном окружении:

```bash
docker compose exec redis redis-cli GET monitor:{id}:status
```

## Переходы и Telegram

Начальный результат переводит monitor из `UNKNOWN` в `UP` или `DOWN`, но не
отправляет сообщение. Уведомление создаётся только для двух переходов:

```text
UP -> DOWN    alert о падении
DOWN -> UP    сообщение о восстановлении
```

Повторные `UP -> UP` и `DOWN -> DOWN` обновляют текущее состояние, но не
создают новый alert. Каждый значимый переход записывается в таблицу
`status_transitions` по уникальному `event_id`.

Telegram по умолчанию выключен. Чтобы включить его, создайте локальный файл
`.env` и задайте оба значения:

```dotenv
PULSE_TELEGRAM_BOT_TOKEN=токен_бота
PULSE_TELEGRAM_CHAT_ID=идентификатор_чата
```

Если значения не заданы, переход записывается со статусом `skipped`, после чего
Consumer продолжает работу. Токен не попадает в логи и не должен добавляться в
репозиторий.

При включённом Telegram Consumer сначала отправляет alert, отмечает его как
`sent`, затем обновляет Redis и подтверждает Kafka offset. Если Telegram
временно недоступен, состояние и offset не меняются, а сообщение Kafka будет
обработано повторно. Статус `sent` защищает от обычной повторной отправки после
сбоя Redis или commit offset.

Telegram не поддерживает idempotency key. Поэтому остаётся небольшой крайний
случай: если Telegram принял сообщение, а Consumer завершился до записи
`sent` в PostgreSQL, после перезапуска alert может прийти повторно. Такая
at-least-once семантика выбрана вместо риска потерять уведомление.

Посмотреть последние переходы:

```bash
docker compose exec postgres psql -U pulse -d pulse -c \
  "SELECT event_id, monitor_id, previous_status, new_status, changed_at, notification_status FROM status_transitions ORDER BY changed_at DESC LIMIT 20;"
```

## Observability

API публикует служебные endpoints на основном HTTP-порту:

```text
GET /metrics
GET /healthz
GET /livez
GET /readyz
```

Pinger и Consumer публикуют те же endpoints на внутреннем порту `9090` своих
контейнеров. `/livez` проверяет, что процесс отвечает. `/readyz` и `/healthz`
проверяют используемые сервисом PostgreSQL, Redis и Kafka. Docker healthchecks
используют `/readyz`.

Основные metrics:

- `pulse_checks_total`
- `pulse_check_duration_seconds`
- `pulse_check_errors_total`
- `pulse_monitors_total`
- `pulse_monitors_down`
- `pulse_kafka_events_total`
- `pulse_notifications_total`
- `pulse_lock_attempts_total`
- `pulse_http_requests_total`
- `pulse_http_request_duration_seconds`

Labels ограничены небольшими наборами значений, например `service`, `result`,
`kind`, `direction` и шаблон HTTP route. `monitor_id`, URL и другие значения с
высокой cardinality в labels не используются.

Prometheus автоматически находит оба экземпляра Pinger через Docker DNS.
Посмотреть состояние targets можно на странице:

```text
http://localhost:9091/targets
```

Примеры PromQL:

```promql
sum(rate(pulse_checks_total[5m])) by (result)
max(pulse_monitors_down)
sum(rate(pulse_kafka_events_total[5m])) by (direction, result)
```

HTTP-запросы API записываются в структурированный лог с request ID, route,
status, размером ответа и duration. Secrets в логи и metrics не попадают.

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
| `PULSE_OBSERVABILITY_ADDR` | `:9090` | Адрес metrics и probes у worker-сервисов |
| `PULSE_SHUTDOWN_TIMEOUT` | `10s` | Время на корректную остановку HTTP-сервера |
| `PULSE_HEALTH_CHECK_TIMEOUT` | `2s` | Timeout одной проверки зависимости в `/readyz` |
| `PULSE_POSTGRES_URL` | локальный DSN | Подключение к PostgreSQL |
| `PULSE_REDIS_ADDR` | `localhost:6379` | Адрес Redis |
| `PULSE_REDIS_OPERATION_TIMEOUT` | `2s` | Timeout подключения и операций Redis |
| `PULSE_KAFKA_BROKERS` | `localhost:9092` | Список Kafka brokers через запятую |
| `PULSE_KAFKA_CHECK_RESULTS_TOPIC` | `check.result` | Topic с результатами проверок |
| `PULSE_KAFKA_PUBLISH_TIMEOUT` | `10s` | Максимальное время одной публикации в Kafka |
| `PULSE_KAFKA_CONSUMER_GROUP` | `pulse-consumer` | Consumer group для обработки результатов |
| `PULSE_CONSUMER_PROCESS_TIMEOUT` | `10s` | Timeout записи в БД и подтверждения offset |
| `PULSE_PINGER_POLL_INTERVAL` | `1s` | Частота загрузки активных monitors |
| `PULSE_PINGER_MAX_CONCURRENCY` | `20` | Максимальное число одновременных HTTP-проверок |
| `PULSE_PINGER_USER_AGENT` | `Pulse/0.1` | User-Agent исходящих запросов |
| `PULSE_PINGER_LOCK_GRACE` | `5s` | Запас времени для TTL Redis lock |
| `PULSE_PINGER_REPLICAS` | `2` | Число Pinger в Docker Compose |
| `PROMETHEUS_PORT` | `9091` | Локальный порт Prometheus в Docker Compose |
| `PULSE_TELEGRAM_BOT_TOKEN` | пусто | Токен Telegram bot, секрет |
| `PULSE_TELEGRAM_CHAT_ID` | пусто | Chat ID для уведомлений |
| `PULSE_TELEGRAM_API_URL` | `https://api.telegram.org` | Базовый URL Telegram Bot API |
| `PULSE_TELEGRAM_REQUEST_TIMEOUT` | `10s` | Timeout запроса к Telegram |

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
  api/              HTTP-сервер, JSON handlers, web UI и встроенные assets
  monitor/          модель monitor и правила валидации
  monitorstate/     модель текущего состояния UNKNOWN, UP или DOWN
  check/            HTTP checker и CheckResult
  event/            JSON-контракты событий
  kafka/            Kafka publisher
  postgres/         пул соединений, monitor store и check history store
  redis/            клиент Redis, текущее состояние и distributed locks
  notification/     Telegram client и формат сообщений
  observability/    Prometheus metrics и health endpoints
  migrate/          применение миграций
  pinger/           scheduler и worker pool
  consumer/         процесс Consumer

docker/             общий Dockerfile для Go-сервисов
migrations/         SQL-миграции и их встраивание в binary
deploy/prometheus/  конфигурация Prometheus
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

Следующий этап по roadmap: integration tests для полного потока от создания
monitor до записи проверки и обновления статуса.

После тестов останутся Docker hardening, Kubernetes manifests и финальная
документация по развёртыванию.
