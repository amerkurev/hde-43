# Описание работы

### Установить пароль в переменной окружения

```bash
export PGPASSWORD=<password>
```

### Создать таблицы в БД (если они не созданы) и наполнить их данными

```bash
./load_data.sh -h <host> -p <port> -d <dbname> -U <user>
```

### Подключиться к БД и выполнить SQL-запросы

```bash
psql -h <host> -p <port> -d <dbname> -U <user>
```
