# Competitors analysis tool

## Автоматизированный сбор данных 

### Описание

[load_competitors_data_script.py](/flows/load_competitors_data_script.py) содержит реализацию flow, выполняющего:
- выгрузку содержимого страниц с веб-сайтов трех конкурентов
- обработку и приведение к единой структуре данных, полученных с веб-сайтов 
- сохранение данных в таблицу базы данных

[comparison.py](/flows/comparison.py) содержит реализацию flow, выполняющего:
- выгрузку данных о конфигурациях компании с внутренней базы данных 
- преобразование данных компании к единой структуре 
- подбор схожих конфигураций компании и конкурентов с GPU
- подбор схожих конфигураций компании и конкурентов без GPU

### Запуск
Локальный запуск flow
``` flow.run()```

В разработанном инструменте код запускается на облачном сервере через пользовательский интерфейс Prefect.  

Для того, чтобы управлять flow из пользовательского интерфейса Prefect необходимо:
1. Создать проект, в котором будут хранится наши flow

```! prefect create project "COMPETITOR_ANALYSIS" --description "This is the project to competitor analysis"```

2. Зарегистрировать в нашем проекте flow

- ```! prefect register --project COMPETITOR_ANALYSIS -p flows/load_competitors_data.py -l local-agent```
- ```! prefect register --project COMPETITOR_ANALYSIS -p flows/comparison.py -l local-agent```

После этого можно запускать flow из пользовательского интерфейса Prefect. А также выполнять в нем настройку запуска по расписанию и мониторинг выполнения потоков (просмотр логов и т.п.)

