# Выгрузка данных биржевого стакана Binance

Данный скрипт выгружает и оптимизирует данные торгового стакана по заданному инструменту с заданным интервалом
времени на торговой бирже Binance

##Подготовка
python -m venv .venv && source .venv/bin/activate
pip install -r -requirements.txt

### Как использовать

Для использования понадобятся ваши apikey и secretkey.
Иснтрументы которые требуется выгрузить указываются в конфиге:
{
    "symbol": "ATOMUSDT",           # Наименование выгружаемого иснтрумента
    "start": "01.03.2022 00:00:00", # Дата и время начала выгрузки
    "end": "15.04.2022 23:59:59",   # Дата и время окончания выгрузки
    "id": 0,                        # Идентификтаор запроса (для новых 0)
    "link": "",                     # Ссылка на архив (для новых - пустая строка)
    "status": "new"                 # Статус в зависимости от текущего состояния (для новых - new)
}

Максимальный период одной выгрузки составляет 2 месяца, разбивайте болтшие периоды на более мелкие.
Более подробное описание в статье.