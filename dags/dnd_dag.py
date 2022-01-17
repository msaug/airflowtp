import airflow
import datetime
import urllib.request as request
import pandas as pd
import json
from faker import Faker
import random
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

fake = Faker()

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dnd_dag = DAG(
    dag_id='dnd_dag',
    default_args=default_args_dict,
    catchup=False,
)


##text=List of strings to be written to file


def _generate_names(output_folder: str, epoch: int):
    """Generates names for 5 characters and saves them

    :param output_folder: The folder location to save
    :type output_folder: str
    """
    characters = []
    for i in range(5):
        character = {'name': fake.name()}
        characters.append(character)
    data = {
        'characters': characters
    }
    json_string = json.dumps(data)
    with open(f"{output_folder}/{epoch}_names.json", 'w') as outfile:
        json.dump(data, outfile)


task_one = PythonOperator(
    task_id='generate_names',
    dag=dnd_dag,
    python_callable=_generate_names,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "epoch": "{{ execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_level(output_folder: str, previous_epoch: int):
    """Sets a random level to all our characters
    """
    with open(f"{output_folder}/{previous_epoch}_names.json") as json_file:
        data = json.load(json_file)

    characters = data["characters"]
    for character in characters:
        character["level"] = random.randint(1, 3)

    with open(f"{output_folder}/{previous_epoch}_levels.json", 'w') as outfile:
        json.dump(data, outfile)


task_two = PythonOperator(
    task_id='set_level',
    dag=dnd_dag,
    python_callable=_set_level,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_race(output_folder: str, previous_epoch: int):
    """Sets a random race to all our characters

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
    """
    webURL = request.urlopen("https://www.dnd5eapi.co/api/races")
    response = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    race_data = json.loads(response.decode(encoding))
    race_results = race_data["results"]

    with open(f"{output_folder}/{previous_epoch}_levels.json") as json_file:
        data = json.load(json_file)

    characters = data["characters"]
    for character in characters:
        randIndex = random.randint(0, len(race_results) - 1)
        character["race"] = race_results[randIndex]["index"]

    with open(f"{output_folder}/{previous_epoch}_races.json", 'w') as outfile:
        json.dump(data, outfile)


task_three = PythonOperator(
    task_id='set_race',
    dag=dnd_dag,
    python_callable=_set_race,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_language(output_folder, previous_epoch: int):
    """Gives our characters all the languages from their race

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
    """
    with open(f"{output_folder}/{previous_epoch}_races.json") as json_file:
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

    with open(f"{output_folder}/{previous_epoch}_languages.json", 'w') as outfile:
        json.dump(data, outfile)


task_four = PythonOperator(
    task_id='set_language',
    dag=dnd_dag,
    python_callable=_set_language,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_race_proficiencies(output_folder, previous_epoch: int):
    """Sets the basic race-related proficiencies and selects random ones in the
    optional proficiencies.

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
    """
    with open(f"{output_folder}/{previous_epoch}_languages.json") as json_file:
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

    with open(f"{output_folder}/{previous_epoch}_race_proficiencies.json", 'w') as outfile:
        json.dump(data, outfile)


task_five = PythonOperator(
    task_id='set_race_proficiencies',
    dag=dnd_dag,
    python_callable=_set_race_proficiencies,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_classes(output_folder, previous_epoch: int):
    """Sets a random class to our characters

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
    """
    webURL = request.urlopen("https://www.dnd5eapi.co/api/classes")
    response = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')
    classes_data = json.loads(response.decode(encoding))
    classes_result = classes_data["results"]

    with open(f"{output_folder}/{previous_epoch}_race_proficiencies.json") as json_file:
        data = json.load(json_file)
    characters = data["characters"]
    for character in characters:
        randIndex = random.randint(0, len(classes_result) - 1)
        character["class"] = classes_result[randIndex]["index"]

    with open(f"{output_folder}/{previous_epoch}_classes.json", 'w') as outfile:
        json.dump(data, outfile)


task_six = PythonOperator(
    task_id='set_classes',
    dag=dnd_dag,
    python_callable=_set_classes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_class_proficiencies(output_folder, previous_epoch: int):
    """Gives our character the basic class-related proficiencies and selects random ones
        among the optional choices

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
    """
    with open(f"{output_folder}/{previous_epoch}_classes.json") as json_file:
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

        with open(f"{output_folder}/{previous_epoch}_class_proficiencies.json", 'w') as outfile:
            json.dump(data, outfile)


task_seven = PythonOperator(
    task_id='set_class_proficiencies',
    dag=dnd_dag,
    python_callable=_set_class_proficiencies,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_spells(output_folder, previous_epoch: int):
    """Gives (n_level+3) random spells to our characters. Spells are at most level 2

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
        """
    with open(f"{output_folder}/{previous_epoch}_class_proficiencies.json") as json_file:
        data = json.load(json_file)

    characters = data["characters"]

    for character in characters:
        # get the spells available for this class
        webURL = request.urlopen(f"https://www.dnd5eapi.co/api/classes/{character['class']}/spells")
        response = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')
        result = json.loads(response.decode(encoding))
        class_spells = result["results"]

        nbOfSpells = character["level"] + 3
        charSpells = []

        # Some classes don't have spells available.
        # As long as our character doesn't have all his spells, we pick a new spell and check if it suits the
        # requirements
        if len(class_spells) != 0:
            while nbOfSpells != 0:
                randSpell = random.randint(0, len(class_spells) - 1)
                print(randSpell)
                spellIndex = class_spells[randSpell]["index"]
                webURL = request.urlopen(f"https://www.dnd5eapi.co/api/spells/{spellIndex}")
                response = webURL.read()
                encoding = webURL.info().get_content_charset('utf-8')
                result = json.loads(response.decode(encoding))
                print(result)
                if (result["level"] <= 2):
                    nbOfSpells -= 1
                    charSpells.append(spellIndex)

        character["spells"] = charSpells

    with open(f"{output_folder}/{previous_epoch}_spells.json", 'w') as outfile:
        json.dump(data, outfile)


task_eight = PythonOperator(
    task_id='set_spells',
    dag=dnd_dag,
    python_callable=_set_spells,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


def _set_attributes(output_folder, previous_epoch:int):
    """Sets random attributes to our characters.

        :param output_folder: The file location of the data
        :type output_folder: str
        :param output_file: The file location to save the new data
        :type output_file: str
        """
    with open(f"{output_folder}/{previous_epoch}_spells.json") as json_file:
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

    with open(f"{output_folder}/{previous_epoch}_attributes.json", 'w') as outfile:
        json.dump(data, outfile)


task_nine = PythonOperator(
    task_id='_set_attributes',
    dag=dnd_dag,
    python_callable=_set_attributes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags/",
        "previous_epoch": "{{ prev_execution_date.int_timestamp }}",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# oh noes :( it's xlsx... let's make it a csv.
#
# task_two = BashOperator(
#     task_id='transmute_to_csv',
#     dag=dnd_dag,
#     bash_command="xlsx2csv /opt/airflow/dags/{{ds_nodash}}.xlsx > /opt/airflow/dags/{{ds_nodash}}_correct.csv",
# )
#
# task_three = BashOperator(
#     task_id='time_filter',
#     dag=dnd_dag,
#     bash_command="awk -F, 'int($31) > 1588612377' /opt/airflow/dags/{{ds_nodash}}_correct.csv > /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv",
# )
#
# task_four = BashOperator(
#     task_id='load',
#     dag=dnd_dag,
#     bash_command="echo \"done\""
# )
#
# task_five = BashOperator(
#     task_id='cleanup',
#     dag=dnd_dag,
#     bash_command="rm /opt/airflow/dags/{{ds_nodash}}_correct.csv /opt/airflow/dags/{{ds_nodash}}_correct_filtered.csv /opt/airflow/dags/{{ds_nodash}}.xlsx",
# )
#
task_one >> task_two >> task_three >> task_four >> task_five >> task_six >> task_seven >> task_eight >> task_nine
