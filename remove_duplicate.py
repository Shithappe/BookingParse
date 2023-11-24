def get_base_url(url):
    # Функция для извлечения базовой части URL (до символа ?)
    base_url = url.split('?')[0]
    return base_url

def remove_duplicate_urls(input_file, output_file):
    unique_urls = set()

    # Чтение из файла и удаление дубликатов
    with open(input_file, 'r') as file:
        for line in file:
            base_url = get_base_url(line.strip())
            unique_urls.add(base_url)

    # Запись уникальных URL в файл
    with open(output_file, 'w') as file:
        for url in unique_urls:
            file.write(url + '\n')

# Указать путь к файлу с ссылками и файлу для записи уникальных ссылок
input_filename = 'booking_links.txt'
output_filename = 'clean_links.txt'

# Удаление дубликатов из файла с ссылками и сохранение уникальных ссылок в новый файл
remove_duplicate_urls(input_filename, output_filename)
