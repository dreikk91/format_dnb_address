import fdb
import yaml
from geopy.geocoders import GoogleV3, Nominatim
from loguru import logger

logger.add(
    sink="format_address.log",
    enqueue=True,
    rotation="4 weeks",
    retention="4 months",
    encoding="utf-8",
    backtrace=True,
    diagnose=True,
)

try:
    with open("format_address.yaml") as f:
        yaml_config = yaml.safe_load(f)
        logger.info("Config opened successful")
except FileNotFoundError:
    logger.info("Can't open config, generating new")
    geolocator = "Nominatim"
    api_key = "BpKmlnBpKmlnhdUiJSPAI16qAVqo2Ks2MHV0pKQ"
    database_location = "D:\\Venbest\\DanubePro\\Data\\dpc2.fdb"
    to_yaml = {
        "api_key": api_key,
        "geolocator": "Nominatim",
        "database_location": database_location,
    }

    with open("format_address.yaml", "w") as f:
        yaml.dump(to_yaml, f, default_flow_style=False)

    with open("format_address.yaml") as f:
        yaml_config = yaml.safe_load(f)

if yaml_config["geolocator"] == "GoogleV3":
    geolocator = GoogleV3(api_key=yaml_config["api_key"])
else:
    geolocator = Nominatim(
        user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36"
                   " (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
    )

logger.info("Current geolocator is " + str(yaml_config["geolocator"]))


def main_city_list():
    main_city_list = [
        "Івано-Франківськ",
        "Луцьк",
        "Рівне",
        "Хмельницький",
        "Тернопіль",
        "Львів",
        "Ужгород",
        "Чернігів",
        "Чернівці",
    ]
    return main_city_list


def add_and_replace_data_for_geocoding(address):
    cutted_address = "Україна " + address.replace(
        "Миколаїв", "Львівська обл. Миколаїв"
    ).replace("Острів", "Тернопільська обл., Острів с.,").replace(
        "Зіньків", "Хмельницька обл., Зіньків с.,"
    )
    return cutted_address


def find_block_with_region_v2(region):
    is_region = None
    if yaml_config["geolocator"] == "GoogleV3":
        count = 0
        while True:
            if (
                region["address_components"][count]["types"][0]
                == "administrative_area_level_1"
            ):
                print(region["address_components"][count]["long_name"])
                is_region = (
                    region["address_components"][count]["long_name"]
                    .replace(" область", "")
                    .replace(" oblast'", "")
                    + " обл"
                )
                break
            else:
                count += 1
    if yaml_config["geolocator"] == "Nominatim":
        print("you need to enter the Google Api key")
        count = 0
        word = "область"
        display_name = region["display_name"].split(",")
        while True:
            if word in display_name[count]:
                is_region = display_name[count].replace("область", "обл")
                break
            else:
                count += 1
    return is_region


def find_lat_long(address):
    location = None
    cutted_address = "Україна " + address.replace(
        "Миколаїв", "Львівська обл. Миколаїв"
    ).replace("Підгородне", "Тернопільська обл. Підгородне с.,").replace(
        "Острів", "Тернопільська обл., Острів с.,"
    ).replace(
        "Зіньків", "Хмельницька обл., Зіньків с.,"
    ).replace(
        "Заводське", "Тернопільська обл., Заводське с.,"
    ).replace(
        "Монастирське", "Тернопільська обл., м.Монастирська"
    ).replace(
        "Журавне", "Львівська обл. смт.Журавно"
    )
    while location is None:
        try:
            location = geolocator.geocode(cutted_address, language="uk")
            mixed_address = find_block_with_region_v2(location.raw)
            return mixed_address
        except IndexError as error:
            logger.debug("Handling run-time error:", error)
            break


def get_grdobg_data():
    select_grdobj = """SELECT g.ID, g.ADDRESS 
                        FROM GRDOBJ g  """
    return select_grdobj


def get_objects_data():
    select_objects_data = """SELECT 
                                    o2.ID, 
                                    o2.DESCRIPTION 
                                FROM
                                    OBJECTS o2
                                WHERE
                                    o2.CLASS_NAME = 'PPK_DNB128'
                                    OR o2.CLASS_NAME = 'PPK_VBD4'
                                    OR o2.CLASS_NAME = 'PPK_VBD6'"""
    return select_objects_data


def partial_replacement(data):
    decoded_text = (
        data[1]
        .decode("cp1251")
        .replace("вул ", "вул.")
        .replace("вул,", "вул.")
        .replace("вул. ", "вул.")
        .replace("вул, ", "вул.")
        .replace(",1", ", 1")
        .replace(",2", ", 2")
        .replace(",3", ", 3")
        .replace(",4", ", 4")
        .replace(",5", ", 5")
        .replace(",6", ", 6")
        .replace(",7", ", 7")
        .replace(",8", ", 8")
        .replace(",9", ", 9")
        .replace("м.Старокостянтинів ", "м.Старокостянтинів, ")
        .replace("м.Тернопіль ", "м.Тернопіль, ")
        .replace("м.Ужгород ", "м.Ужгород, ")
        .replace("с.Сокільники ", "с.Сокільники, ")
        .replace(",вул", ", вул")
        .replace(",пл", ", пл")
        .replace(",пр-т", ", пр-т")
        .replace(".вул", ", вул")
        .replace(".пл", ", пл")
        .replace(".пр-т", ", пр-т")
        .replace("вул ", ", вул.")
        .replace("пл ", ", пл.")
        .replace("пр-т ", "пр-т.")
        .replace("..", ".")
        .replace(",,", ",")
        .replace("  ", " ")
    )
    return decoded_text


def update_sql(database, text, id):
    ""
    if database == "OBJECTS":
        update = "update OBJECTS set DESCRIPTION=? WHERE ID=?;  "
        cur.execute(update, (text, id))

    if database == "GRDOBJ":
        update = "update GRDOBJ set ADDRESS=? WHERE ID=?; "
        cur.execute(update, (text, id))


def if_street_in_lviv(decoded_text):
    splited_address = decoded_text.split(",", 1)
    replace_street = splited_address[0] + " вул., " + splited_address[1]
    add_city = replace_street.split(".", 1)

    return add_city


def if_square_in_lviv(decoded_text):
    splited_address = decoded_text.split(",", 1)
    replace_street = splited_address[0] + " пл., " + splited_address[1]
    add_city = replace_street.split(".", 1)

    return add_city


def if_avenue_in_lviv(decoded_text):
    splited_address = decoded_text.split(",", 1)
    replace_street = splited_address[0] + " пр-т., " + splited_address[1]
    add_city = replace_street.split(".", 1)

    return add_city


def city(splited_address, city_pointer):
    region_and_city = None
    replace_city = splited_address[0].replace(city_pointer, "")

    if replace_city not in main_city_list():
        region = find_lat_long(splited_address[0] + " " + splited_address[1])
        region_and_city = region + "., " + replace_city + " {0}, ".format(city_pointer)

    elif replace_city in main_city_list():
        region_and_city = replace_city + " {0}, ".format(city_pointer)

    return region_and_city


def street(splited_address, street_pointer):
    replace_street = splited_address[1].replace(street_pointer, "")
    add_street = replace_street + " {0}, ".format(street_pointer)
    return add_street


def update_address(sql_result, database):
    for data in sql_result:
        splited_address = None
        replace_street = None
        add_city = None
        add_street = None
        region_and_city = None
        decoded_text = None
        region = None
        encoded_text = None
        try:
            if data[1] is not None:
                decoded_text = partial_replacement(data)
                if decoded_text[0:4] == "вул.":
                    add_city = if_street_in_lviv(decoded_text)
                    print(add_city[0].replace("вул", "Львів м., ") + add_city[1])
                    encoded_text = (
                        (add_city[0].replace("вул", "Львів м., ") + add_city[1])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )
                    update_sql(database, encoded_text, data[0])

                if decoded_text[0:3] == "пл.":
                    add_city = if_square_in_lviv(decoded_text)
                    encoded_text = (
                        (add_city[0].replace("пл", "Львів м., ") + add_city[1])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )
                    update_sql(database, encoded_text, data[0])

                if decoded_text[0:5] == "пр-т.":
                    add_city = if_avenue_in_lviv(decoded_text)
                    encoded_text = (
                        (add_city[0].replace("пр-т", "Львів м., ") + add_city[1])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )
                    update_sql(database, encoded_text, data[0])

                if decoded_text[0:2] == "м.":
                    splited_address = decoded_text.split(", ", 2)
                    region_and_city = city(splited_address, decoded_text[0:2])

                    if splited_address[1][0:4] == "вул.":
                        add_street = street(splited_address, splited_address[1][0:4])

                    if splited_address[1][0:3] == "пл.":
                        add_street = street(splited_address, splited_address[1][0:3])

                    elif splited_address[1][0:5] == "пр-т.":
                        add_street = street(splited_address, splited_address[1][0:5])

                    encoded_text = (
                        (region_and_city + add_street + splited_address[2])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )
                    update_sql(database, encoded_text, data[0])

                if decoded_text[0:2] == "с.":
                    splited_address = None
                    replace_street = None
                    add_city = None
                    add_street = None

                    region_and_city = None
                    region = None
                    logger.info(decoded_text)
                    splited_address = decoded_text.split(", ", 2)

                    region_and_city = city(splited_address, decoded_text[0:2])

                    if splited_address[1][0:4] == "вул.":
                        add_street = street(splited_address, splited_address[1][0:4])

                    if splited_address[1][0:3] == "пл.":
                        add_street = street(splited_address, splited_address[1][0:3])

                    elif splited_address[1][0:5] == "пр-т.":
                        add_street = street(splited_address, splited_address[1][0:5])

                    logger.info(region_and_city + add_street + splited_address[2])

                    encoded_text = (
                        (region_and_city + add_street + splited_address[2])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )
                    update_sql(database, encoded_text, data[0])

                if decoded_text[0:4] == "смт.":
                    splited_address = None
                    replace_street = None
                    add_city = None
                    add_street = None

                    region_and_city = None
                    region = None
                    logger.info(decoded_text)

                    splited_address = decoded_text.split(", ", 2)

                    region_and_city = city(splited_address, decoded_text[0:4])

                    if splited_address[1][0:4] == "вул.":
                        add_street = street(splited_address, splited_address[1][0:4])

                    if splited_address[1][0:3] == "пл.":
                        add_street = street(splited_address, splited_address[1][0:3])

                    elif splited_address[1][0:5] == "пр-т.":
                        add_street = street(splited_address, splited_address[1][0:5])

                    logger.info(region_and_city + add_street + splited_address[2])

                    encoded_text = (
                        (region_and_city + add_street + splited_address[2])
                        .replace("  ", " ")
                        .encode("cp1251")
                    )

                    update_sql(database, encoded_text, data[0])

        except NameError as err:
            logger.debug(err)
            # print(command)
            logger.debug(data)
        except AttributeError as err:
            logger.debug(err)
            # print(command)
            logger.debug(data)
        except TypeError as err:
            logger.debug(err)
            # print(command)
            logger.debug(data)
        except IndexError as err:
            logger.debug(err)
            # print(command)
            logger.debug(data)


if __name__ == "__main__":
    with logger.catch():
        try:
            con = fdb.connect(
                dsn=yaml_config["database_location"],
                user="SYSDBA",
                password="idonotcare",
                # necessary for all dialect 1 databases
                charset="WIN1251",  # specify a character set for the connection
            )
        except fdb.fbcore.DataError as err:
            logger.debug(
                "can't connect to database " + yaml_config["database_location"]
            )
            logger.debug(err)

        cur = con.cursor()
        select = get_grdobg_data()
        cur.execute(select)
        sql_result = cur.fetchall()
        table = "GRDOBJ"
        update_address(sql_result, table)
        select = get_objects_data()
        cur.execute(select)
        sql_result = cur.fetchall()
        table = "OBJECTS"
        update_address(sql_result, table)
        con.commit()
        cur.close()
