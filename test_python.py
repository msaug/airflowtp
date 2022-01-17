from faker import Faker
import pandas as pd
import json
import random
import urllib.request as request

fake = Faker()


def _generate_names(output_folder):
    characters = []
    for i in range(5):
        character = {'name': fake.name()}
        characters.append(character)
    data = {
        'characters': characters
    }
    json_string = json.dumps(data)
    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_level(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)

    characters = data["characters"]
    for character in characters:
        character["level"] = random.randint(1, 3)

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_race(input_folder, output_folder):
    webURL = request.urlopen("https://www.dnd5eapi.co/api/races")
    response = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    race_data = json.loads(response.decode(encoding))
    race_results = race_data["results"]

    with open(input_folder) as json_file:
        data = json.load(json_file)

    characters = data["characters"]
    for character in characters:
        randIndex = random.randint(0, len(race_results) - 1)
        character["race"] = race_results[randIndex]["index"]

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_language(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)
    characters = data["characters"]

    for character in characters:
        webURL = request.urlopen(
            f"https://www.dnd5eapi.co/api/races/{character['race']}")
        response = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        race_info = json.loads(response.decode(encoding))
        race_info_language = race_info["languages"]
        char_language = []
        for language in race_info_language:
            char_language.append(language["index"])
        character["languages"] = char_language

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_race_proficiencies(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)
    characters = data["characters"]

    for character in characters:
        webURL = request.urlopen(
            f"https://www.dnd5eapi.co/api/races/{character['race']}")
        response = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        race_info = json.loads(response.decode(encoding))
        char_proficiencies = []
        starting_proficiencies = race_info["starting_proficiencies"]

        if "starting_proficiency_options" in race_info:
            optional_proficiencies = race_info["starting_proficiency_options"]
            max_opt_proficiencies = optional_proficiencies["choose"]
            opt_prof_from = optional_proficiencies["from"]
            for i in range(max_opt_proficiencies):
                randIndex = random.randint(0, len(opt_prof_from) - 1)
                char_proficiencies.append(opt_prof_from[randIndex]["index"])
                opt_prof_from.pop(randIndex)

        for proficiency in starting_proficiencies:
            char_proficiencies.append(proficiency["index"])

        character["proficiencies"] = char_proficiencies

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_classes(input_folder, output_folder):
    webURL = request.urlopen("https://www.dnd5eapi.co/api/classes")
    response = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    classes_data = json.loads(response.decode(encoding))
    classes_result = classes_data["results"]

    with open(input_folder) as json_file:
        data = json.load(json_file)
    characters = data["characters"]
    for character in characters:
        randIndex = random.randint(0, len(classes_result) - 1)
        character["class"] = classes_result[randIndex]["index"]

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_class_proficiencies(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)
        characters = data["characters"]
    for character in characters:
        webURL = request.urlopen(f"https://www.dnd5eapi.co/api/classes/{character['class']}")
        response = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        class_info = json.loads(response.decode(encoding))
        char_proficiencies = character["proficiencies"]
        class_proficiencies_select = class_info["proficiency_choices"][0]
        class_proficiencies = class_info["proficiencies"]
        max_opt_proficiencies = class_proficiencies_select["choose"]
        opt_prof_from = class_proficiencies_select["from"]

        for proficiency in class_proficiencies:
            char_proficiencies.append(proficiency["index"])

        for i in range(max_opt_proficiencies):
            randIndex = random.randint(0, len(opt_prof_from) - 1)
            char_proficiencies.append(opt_prof_from[randIndex]["index"])
            opt_prof_from.pop(randIndex)

        with open(f"{output_folder}", 'w') as outfile:
            json.dump(data, outfile)

def _set_spells(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)
        characters = data["characters"]
    for character in characters:
        webURL = request.urlopen(f"https://www.dnd5eapi.co/api/classes/{character['class']}/spells")
        response = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        result = json.loads(response.decode(encoding))
        class_spells = result["results"]
        nbOfSpells = character["level"]+3
        charSpells = []
        done = False
        if len(class_spells)!=0:
            while nbOfSpells!=0:
                randSpell = random.randint(0,len(class_spells)-1)
                print(randSpell)
                spellIndex = class_spells[randSpell]["index"]
                webURL = request.urlopen(f"https://www.dnd5eapi.co/api/spells/{spellIndex}")
                response = webURL.read()
                encoding = webURL.info().get_content_charset('utf-8')
                result = json.loads(response.decode(encoding))
                print(result)
                if(result["level"]<=2):
                    nbOfSpells-=1
                    charSpells.append(spellIndex)

        character["spells"]=charSpells

    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


def _set_attributes(input_folder, output_folder):
    with open(input_folder) as json_file:
        data = json.load(json_file)
    characters = data["characters"]
    for character in characters:
        strength = random.randint(6, 18)
        dexterity = random.randint(2, 18)
        constitution = random.randint(2, 18)
        intelligence = random.randint(2, 18)
        wisdom = random.randint(2, 18)
        charisma = random.randint(2, 18)
        character["strength"] = strength
        character["dexterity"] = dexterity
        character["constitution"] = constitution
        character["intelligence"] = intelligence
        character["wisdom"] = wisdom
        character["charisma"] = charisma
        character["attributes"] = f"{[strength, dexterity, constitution, intelligence, wisdom, charisma]}"
    with open(f"{output_folder}", 'w') as outfile:
        json.dump(data, outfile)


_generate_names("./names.json")
_set_level("./names.json", "levels.json")
_set_race("./levels.json", "./races.json")
_set_language("./races.json", "./languages.json")
_set_race_proficiencies("./languages.json", "./race_proficiencies.json")
_set_attributes("./race_proficiencies.json", "./attributes.json")
_set_classes("./attributes.json", "./classes.json")
_set_class_proficiencies("./classes.json", "./proficiencies.json")
_set_spells("./proficiencies.json","./spells.json")

# name>>race>>[language, race_profficiencies] parallele : class>> level>>sorts parallel: class_profficiencies
