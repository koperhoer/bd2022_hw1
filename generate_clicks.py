## Создавать тестовые данные об ID юзеров и координатах нажатий будем фейкером:
from faker import Faker
## Будем использовать дату и время в качестве сида.
from datetime import datetime
## Также пригодится стандартный питоновский random.
import random
## Будем держать справочники для мапперов в JSON-файлах.
import json

## Создадим инстанцию Faker и скормим ей как сид текущую дату и время.
fake = Faker()
Faker.seed(datetime.now())
## Для встроенного random - то же самое.
random.seed()

## Пусть на нашем сайте от 50 до 100 тысяч ежедневных уникальных посещений.
unique_visitors = 50000 + (int(fake.pydecimal()) % 50000)
## Правила генерации ID юзеров не заданы. Допустим, что это целые числа от 0
## до 10 млн, не обязательно относящиеся к конкретному посещению страницы.
uids_list = random.sample(range(10000000), unique_visitors)

## Создадим справочник областей экрана. Пусть на сайте есть кнопки, декоративные
## элементы, рекламные баннеры, элементы навигации и поля для ввода какой-то информации.
element_names = ["button", "decoration", "ad_banner", "navigation", "input_field"]
screen_elements = [
    ## Кнопочки: домой, войти/зарегистрироваться, о нас, новый пост, сайты-партнёры.
    {
        'type' : 'button',
        'logic_name' : 'main_page',
        'startX' : '50',
        'startY' : '25',
        'endX' : '150',
        'endY' : '75'
    },
    {
        'type' : 'button',
        'logic_name' : 'login_signup',
        'startX' : '1800',
        'startY' : '25',
        'endX' : '1900',
        'endY' : '75'
    },
    {
        'type' : 'button',
        'logic_name' : 'about_us',
        'startX' : '1700',
        'startY' : '25',
        'endX' : '1775',
        'endY' : '75'
    },
    {
        'type' : 'button',
        'logic_name' : 'new_post',
        'startX' : '1700',
        'startY' : '900',
        'endX' : '1800',
        'endY' : '950'
    },
    {
        'type' : 'button',
        'logic_name' : 'our_partners',
        'startX' : '50',
        'startY' : '900',
        'endX' : '150',
        'endY' : '950'
    },
    ## Рекламные баннеры: микрокредитные организации, квартиры в ЖК в центре Москвы за 150
    ## млн. рублей, сайты с распутными женщинами, скандалы интриги расследования,
    ## дедовский способ накачаться за две минуты.
    {
        'type' : 'ad_banner',
        'logic_name' : 'quick_loans',
        'startX' : '50',
        'startY' : '150',
        'endX' : '200',
        'endY' : '300'
    },
    {
        'type' : 'ad_banner',
        'logic_name' : 'expensive_flat',
        'startX' : '50',
        'startY' : '350',
        'endX' : '200',
        'endY' : '500'
    },
    {
        'type' : 'ad_banner',
        'logic_name' : 'XXX_site',
        'startX' : '50',
        'startY' : '550',
        'endX' : '200',
        'endY' : '700'
    },
    {
        'type' : 'ad_banner',
        'logic_name' : 'tabloid_rubbish',
        'startX' : '300',
        'startY' : '900',
        'endX' : '500',
        'endY' : '950'
    },
    {
        'type' : 'ad_banner',
        'logic_name' : 'ancient_workout',
        'startX' : '1500',
        'startY' : '700',
        'endX' : '1600',
        'endY' : '850'
    },
    ## Декоративные элементы в самом верху и в самом низу.
    {
        'type' : 'decoration',
        'logic_name' : 'long_logo',
        'startX' : '200',
        'startY' : '0',
        'endX' : '1650',
        'endY' : '75'
    },
    {
        'type' : 'decoration',
        'logic_name' : 'subdued_logo',
        'startX' : '200',
        'startY' : '950',
        'endX' : '1650',
        'endY' : '1080'
    },
    ## Элементы навигации: все темы, все посты, список разделов.
    {
        'type' : 'navigation',
        'logic_name' : 'all_topics',
        'startX' : '400',
        'startY' : '150',
        'endX' : '600',
        'endY' : '750'
    },
    {
        'type' : 'navigation',
        'logic_name' : 'all_posts',
        'startX' : '650',
        'startY' : '150',
        'endX' : '850',
        'endY' : '750'
    },
    {
        'type' : 'navigation',
        'logic_name' : 'categories',
        'startX' : '900',
        'startY' : '150',
        'endX' : '1100',
        'endY' : '750'
    },
    ## Поля для ввода: обычный поиск, поиск с гуглом.
    {
        'type' : 'input_field',
        'logic_name' : 'search',
        'startX' : '400',
        'startY' : '800',
        'endX' : '475',
        'endY' : '850'
    },
    {
        'type' : 'input_field',
        'logic_name' : 'search_with_google',
        'startX' : '500',
        'startY' : '800',
        'endX' : '575',
        'endY' : '850'
    }
]

## Мы гарантируем, что элементы управления не накладываются друг на друга. Тогда у
## мапредьюсера может не болеть об этом голова.
def rectangle_overlap(startX1, startY1, endX1, endY1, startX2, startY2, endX2, endY2):
    if (startX1 > endX2 or startX2 > endX1):
        return False
    if (startY1 > endY2 or startY2 > endY1):
        return False
    return True

for elem1 in screen_elements:
    for elem2 in screen_elements:
        if elem1['logic_name'] != elem2['logic_name']:
            if(rectangle_overlap(elem1['startX'], elem1['startY'], elem1['endX'], elem1['endY'],\
                                 elem2['startX'], elem2['startY'], elem2['endX'], elem2['endY'])):
                print("WARNING: elements \'" + str(elem1['logic_name']) + "\' and \'"\
                      + str(elem2['logic_name']) + "\' are overlapping.")

## Вспомогательный метод для выбора случайного элемента страницы заданного типа:
def any_elem_by_type(dictionary, typee):
    elems = []
    for i in range(len(dictionary)):
        if(dictionary[i]['type'] == typee):
            elems.append(dictionary[i])
    return random.choice(elems)

## Откроем файл для записи.
input_file = open("input_data.txt", "w+")

## Также экспортируем в JSON справочник элементов страницы.
json_dict = json.dumps(screen_elements, indent = 4)
with open("element_dictionary.json", "w+") as elem_dict:
    elem_dict.write(json_dict)

## Генерим статистику нажатий отдельно для каждого юзера.
ct = 1
for uid in uids_list:
    print(str(ct) + "/" + str(len(uids_list)))
    ct += 1
    ## Пусть каждый пользователь делает от 25 до 200 нажатий:
    num_clicks = int(random.uniform(25, 200))
    for _ in range(num_clicks):
        chance = fake.pyint()
        ## С вероятностью 40% пользователь хочет полазать по сайту с помощью элементов навигации.
        if(chance < 4000):
            elem = any_elem_by_type(screen_elements, 'navigation')
        ## С вероятностью 20% он хочет какую-нибудь кнопку.
        elif(chance < 6000):
            elem = any_elem_by_type(screen_elements, 'button')
        ## С вероятностью 20% он хочет что-то поискать или ввести:
        elif(chance < 8000):
            elem = any_elem_by_type(screen_elements, 'input_field')
        ## С вероятностью 15% пользователь захочет нажать на рекламу:
        elif(chance < 9500):
            elem = any_elem_by_type(screen_elements, 'ad_banner')
        ## С вероятностью 5% пользователь обознается и нажмёт на декорации, думая, что это кнопка:
        else:
            elem = any_elem_by_type(screen_elements, 'decoration')
        ## Сгенерим координаты нажатия. Пусть пользователи изредка чуть-чуть промахиваются по кнопке.
        click_x = int(random.uniform(int(elem['startX']) - 1, int(elem['endX']) + 1))
        click_y = int(random.uniform(int(elem['startY']) - 1, int(elem['endY']) + 1))
        ## Сгенерим метку времени. Берём интервал в одни сутки.
        timestamp = fake.date_time_between(start_date='-1d', end_date='now').time()
        ## Сформируем строку и запишем в файл.
        click_str = str(click_x) + "," + str(click_y) + "," + str(uid) + "," + str(timestamp)
        input_file.write(click_str + '\n')

## Файл закрыть.
input_file.close()

## Создадим справочник температур.
temperatures = [
    {
        'temperature' : 'low',
        'start' : '0',
        'end' : '900000'
    },
    {
        'temperature' : 'medium',
        'start' : '900001',
        'end' : '1800000'
    },
    {
        'temperature' : 'high',
        'start' : '1800001',
        'end' : '999999999'
    }
]

## Экспортируем его в JSON.
json_dict = json.dumps(temperatures, indent = 4)
with open("temperatures_dictionary.json", "w+") as temp_dict:
    temp_dict.write(json_dict)
