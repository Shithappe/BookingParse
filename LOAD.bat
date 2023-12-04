@echo off

echo Запуск spider update_rooms
scrapy crawl update_rooms
echo.

echo Запуск spider rooms_2_day
scrapy crawl rooms_2_day
echo.

echo Запуск spider rooms_30_day
scrapy crawl rooms_30_day
echo.

echo Завершение
