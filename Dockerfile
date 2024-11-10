# Используем образ с компилятором gcc для сборки программы на C
FROM gcc:latest

# Создаем рабочую директорию
WORKDIR /app

# Копируем исходные файлы в контейнер
COPY . .

# Компилируем исходные файлы
RUN gcc -o master master.c -lpthread
RUN gcc -o worker worker.c -lpthread

# В зависимости от значения переменной ROLE выбираем, какой исполняемый файл запустить
ENTRYPOINT ["sh", "-c", "if [ \"$ROLE\" = 'master' ]; then ./master; else ./worker; fi"]
