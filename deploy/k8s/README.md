# Pulse в Kubernetes

Здесь находятся манифесты этапа 13. Go-код не меняется: Kubernetes запускает те
же образы, что и Docker Compose, следит за состоянием процессов и управляет
числом экземпляров.

## Что лежит в папках

- `bootstrap` создаёт namespace `pulse` и ConfigMap с обычными настройками.
- `base` запускает API, два Pinger и один Consumer, создаёт внутренние Services.
- `migrate` запускает отдельный Job с SQL-миграциями.
- `autoscaling` добавляет HPA для Pinger вместо фиксированного числа replicas.
- `demo` содержит временные PostgreSQL, Redis и Kafka для локального знакомства.
- `e2e` запускает существующий E2E-тест в отдельном Job внутри кластера.
- `secret.example.yaml` показывает форму Secret, но не применяется автоматически.

В каждом каталоге есть `kustomization.yaml`. Поэтому можно собирать и применять
наборы командой `kubectl ... -k`, без установки Helm или отдельного Kustomize.

## Перед запуском

Нужны Kubernetes-кластер, настроенный `kubectl` и образы четырёх приложений.
Сначала убедитесь, что выбран правильный кластер:

```powershell
kubectl config current-context
kubectl get nodes
```

Все команды ниже выполняются из корня Pulse. Обычный Docker Compose продолжает
работать независимо от Kubernetes. Само наличие Docker не означает, что в нём
включён Kubernetes.

## Локальный запуск с kind

Для проверки можно использовать [kind](https://kind.sigs.k8s.io/docs/user/quick-start/).
Он создаёт Kubernetes-кластер внутри Docker. Это отдельное тестовое окружение,
оно не использует данные и Telegram-токен из вашего Compose `.env`.

```powershell
kind create cluster --name pulse
docker compose build api pinger consumer migrate
kind load docker-image pulse-api:dev pulse-pinger:dev pulse-consumer:dev pulse-migrate:dev --name pulse
kubectl apply -k deploy/k8s/bootstrap
kubectl apply -k deploy/k8s/demo
kubectl -n pulse-infra rollout status deployment/postgres --timeout=180s
kubectl -n pulse-infra rollout status deployment/redis --timeout=180s
kubectl -n pulse-infra rollout status deployment/kafka --timeout=180s
kubectl -n pulse-infra wait --for=condition=complete job/pulse-kafka-topic --timeout=300s
```

При ошибке `kind load` о недостающем content digest в Docker image store можно
загрузить только вариант для вашей архитектуры. На обычном Windows x64:

```powershell
docker image save --platform linux/amd64 -o pulse-images.tar pulse-api:dev pulse-pinger:dev pulse-consumer:dev pulse-migrate:dev
kind load image-archive pulse-images.tar --name pulse
Remove-Item -LiteralPath pulse-images.tar
```

Важно: `demo` использует `emptyDir` и общедоступный пароль `pulse`. Данные
теряются при замене или удалении infrastructure Pod. Это не production setup,
не резервная копия и не замена Compose volumes. Не применяйте `demo` в рабочем
кластере: он также заменит Secret `pulse-secrets` демонстрационными значениями.

После подготовки инфраструктуры примените миграции, а потом приложения:

```powershell
kubectl apply -k deploy/k8s/migrate
kubectl -n pulse wait --for=condition=complete job/pulse-migrate --timeout=180s
kubectl apply -k deploy/k8s/base
kubectl -n pulse rollout status deployment/pulse-api --timeout=180s
kubectl -n pulse rollout status deployment/pulse-pinger --timeout=180s
kubectl -n pulse rollout status deployment/pulse-consumer --timeout=180s
kubectl -n pulse port-forward service/pulse-api 8081:80
```

Последняя команда остаётся запущенной. Откройте `http://localhost:8081`.
Порт отличается от Compose `8080`, поэтому оба окружения могут работать вместе.
Для первого monitor в тестовом кластере подойдёт URL
`http://pulse-api.pulse.svc.cluster.local/healthz`.

В `base` Services имеют тип `ClusterIP`, публичного Ingress нет. Web UI пока
без авторизации, поэтому не открывайте его в интернет без HTTPS и контроля
доступа. `port-forward` по умолчанию доступен только на localhost.

## Запуск с вашей инфраструктурой

Вместо `demo` нужны отдельно подготовленные PostgreSQL, Redis и Kafka. Манифесты
приложения не создают production-кластеры этих систем, диски, backups или TLS.
В текущем приложении Redis и Kafka подключаются без пароля, SASL и TLS: такие
подключения должны быть доступны только в доверенной приватной сети. Для иной
политики безопасности сначала потребуется добавить поддержку в Go-клиенты.

1. В `bootstrap/configmap.yaml` задайте адрес Redis, список Kafka brokers и имя
   topic. Адреса должны разрешаться и быть доступны из Pod, а не только с вашего
   компьютера. Все Kafka `advertised.listeners` тоже должны быть доступны из
   кластера. `localhost` внутри Pod означает сам Pod.
2. Заранее создайте Kafka topic `check.result`, например с тремя partitions.
   Число Consumer больше числа partitions не увеличит параллелизм обработки.
3. Примените `bootstrap`, скопируйте `secret.example.yaml` в `secret.yaml` и
   заполните подключение к PostgreSQL. Для Telegram заполните оба поля или
   оставьте оба пустыми. Затем примените Secret:

```powershell
kubectl apply -k deploy/k8s/bootstrap
Copy-Item deploy/k8s/secret.example.yaml deploy/k8s/secret.yaml
# Заполните deploy/k8s/secret.yaml в редакторе.
kubectl apply -f deploy/k8s/secret.yaml
```

`secret.yaml` и каталог `.local` исключены из Git. Не коммитьте другие копии
секретов и не вставляйте их в README. Kubernetes Secret сам по себе не шифрует
данные: для рабочего кластера нужны соответствующие RBAC, encryption at rest
и управление секретами. Telegram-токен получает только Consumer. API, Pinger
и migration Job получают только PostgreSQL DSN.

### Образы в registry

В `base` указаны локальные образы `pulse-*:dev`. Для удалённого кластера соберите
и опубликуйте образы под уникальным release tag или digest. Создайте локальный
overlay `deploy/k8s/.local/apps/kustomization.yaml`, например:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
images:
  - name: pulse-api
    newName: registry.example.com/team/pulse-api
    newTag: v0.1.0
  - name: pulse-pinger
    newName: registry.example.com/team/pulse-pinger
    newTag: v0.1.0
  - name: pulse-consumer
    newName: registry.example.com/team/pulse-consumer
    newTag: v0.1.0
```

Адрес registry в примере нужно заменить своим. Для migration Job создайте
аналогичный overlay с `resources: [../../migrate]` и образом `pulse-migrate`.
Для приватного registry добавьте `imagePullSecrets` в Pod templates; пароль
registry не должен попадать в Git. Применяйте миграционный overlay первым,
дождитесь успешного Job, затем применяйте overlay приложений.

## Обновление и миграции

Job не перезапускается от повторного `apply`, а его Pod template неизменяемый.
Перед очередной миграцией удалите только старый завершённый Job, убедившись,
что он сейчас не выполняется:

```powershell
kubectl -n pulse get job pulse-migrate
kubectl -n pulse logs job/pulse-migrate
kubectl -n pulse delete job pulse-migrate
kubectl apply -k deploy/k8s/migrate
kubectl -n pulse wait --for=condition=complete job/pulse-migrate --timeout=180s
kubectl apply -k deploy/k8s/base
```

С registry используйте ваши overlays вместо `migrate` и `base`. Сначала убедитесь,
что миграции совместимы с работающей версией и есть backup PostgreSQL. Сам runner
не применяет уже выполненные миграции повторно и использует advisory lock.
Не запускайте приложения после неудачной миграции.

Изменения ConfigMap и Secret не обновляют environment уже работающих Pod.
После их применения перезапустите нужные Deployments:

```powershell
kubectl -n pulse rollout restart deployment/pulse-api deployment/pulse-pinger deployment/pulse-consumer
```

Смена только Telegram-токена требует перезапуска Consumer. С новым release tag
изменение image в overlay само запускает rollout. При локальной пересборке того
же `:dev` сначала повторно загрузите образы в kind, затем выполните restart.

## Probes и безопасность

`startupProbe` и `livenessProbe` проверяют `/livez`, то есть сам процесс.
`readinessProbe` использует `/readyz` и проверяет его инфраструктуру. Проблемы
с БД не должны сами по себе запускать бесконечные рестарты через liveness.
При этом worker может завершиться с ошибкой, и Kubernetes перезапустит его.
Readiness не приостанавливает scheduler или Kafka reader, а только отмечает
Pod неготовым и исключает его из Service.

У приложений заданы CPU/memory requests и limits. Эти стартовые значения нужно
подбирать по реальной нагрузке. Контейнеры работают от UID `10001`, без Linux
capabilities и privilege escalation, с read-only filesystem, отдельным `/tmp`
и `RuntimeDefault` seccomp. ServiceAccount token не монтируется. Namespace
`pulse` применяет Pod Security `restricted` версии `v1.32`.

На graceful shutdown отводится 30 секунд, у HTTP-сервера timeout 10 секунд.
Rolling update сначала запускает новый Pod и ждёт readiness. Если приложению
не хватает ресурсов, rollout может остановиться, не убирая готовую старую копию.

## Автомасштабирование Pinger

По умолчанию работают две копии. Необязательный HPA увеличивает их число до
пяти при средней загрузке CPU выше 70% от request. Для него нужен
[Metrics Server](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/),
это не тот же компонент, что Prometheus. Сначала проверьте наличие metrics:

```powershell
kubectl top pods -n pulse
kubectl apply -k deploy/k8s/autoscaling
kubectl -n pulse get hpa
```

После включения HPA применяйте только `autoscaling` или основанный на нём overlay,
а не `base`: в `autoscaling` поле `replicas` Pinger удалено, чтобы `apply` не
сбрасывал решение HPA. CPU для HTTP checker не всегда отражает нагрузку хорошо:
это простой начальный вариант, не масштабирование по lag или числу monitors.
Redis locks продолжают координировать проверки между экземплярами.

Вернуться к фиксированным двум копиям:

```powershell
kubectl -n pulse delete hpa pulse-pinger
kubectl apply -k deploy/k8s/base
```

## Metrics и диагностика

Pod помечены аннотациями `prometheus.io/*`. Они только описывают endpoints,
сами по себе ничего не собирают. Настройте Prometheus Kubernetes discovery и
соответствующие RBAC отдельно. Для Pinger собирайте метрики каждого Pod, а не
одного балансируемого Service, иначе данные разных экземпляров смешаются.

```powershell
kubectl -n pulse get pods,services,jobs
kubectl -n pulse logs deployment/pulse-pinger --all-pods=true --tail=50
kubectl -n pulse logs deployment/pulse-consumer --tail=50
kubectl -n pulse describe deployment pulse-api
kubectl -n pulse get events --sort-by=.lastTimestamp
```

Собрать YAML без кластера:

```powershell
kubectl kustomize deploy/k8s/base
kubectl kustomize deploy/k8s/migrate
kubectl kustomize deploy/k8s/autoscaling
kubectl kustomize deploy/k8s/demo
```

Проверить через Kubernetes API без создания приложений можно после создания
namespace. Проверка схем не доказывает доступность инфраструктуры или образов:

```powershell
kubectl apply -k deploy/k8s/base --dry-run=server
kubectl apply -k deploy/k8s/migrate --dry-run=server
kubectl apply -k deploy/k8s/autoscaling --dry-run=server
```

## E2E-проверка в тестовом кластере

После успешного запуска приложений можно проверить полный поток тем же тестом,
который используется в Docker Compose. Он создаёт monitor с уникальным UUID,
ждёт статус `UP` и историю, проверяет UI и удаляет только свои данные.
Используйте тестовую БД, а не production-окружение:

```powershell
docker compose --profile e2e build e2e-tests
kind load docker-image pulse-e2e-tests:dev --name pulse
kubectl apply -k deploy/k8s/e2e
kubectl -n pulse wait --for=condition=complete job/pulse-e2e --timeout=300s
kubectl -n pulse logs job/pulse-e2e
```

Для повторного теста удалите только завершённый `job/pulse-e2e` и снова
примените `e2e`. Для ошибки image store используйте такой же `docker image save
--platform ...`, как для сервисов. С registry нужен overlay образа тестов.
Тестовый Job не получает Telegram credentials.

`go test ./tests/deployment` без кластера собирает overlays через `kubectl
kustomize` и проверяет настройки probes, безопасность, Services, число Pinger
и отделение миграций и secrets. Без `kubectl` этот тест пропускается.

## Удаление тестового окружения

Когда локальный кластер `pulse` больше не нужен:

```powershell
kind delete cluster --name pulse
```

Это удалит весь этот kind-кластер и все его тестовые данные. Compose volumes
останутся на месте. Не выполняйте эту команду для кластера с нужными данными.
