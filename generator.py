import os
import sys
import yaml
import networkx as nx
from datetime import datetime, timedelta
import copy


# YAML utility functions
def create_project_directory(project_name):
    """
    Create the project directory
    """
    os.makedirs(project_name, exist_ok=True)
    print(f"Created directories for {project_name}.")

def load_dag_config(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        return yaml.safe_load(file)

def convert_default_args(default_args):
    """
    Convert the default args in a proper format
    """
    if 'start_date' in default_args:
        default_args['start_date'] = datetime.strptime(default_args['start_date'], '%Y, %m, %d')
    if 'retry_delay_minutes' in default_args:
        default_args['retry_delay'] = timedelta(minutes=default_args.pop('retry_delay_minutes'))
    return default_args

class DataCollection:
     def __init__(self, name, kw_args):
        self.name = name
        self.kw_args = kw_args


class MultipleOutputDataCollectionsException(Exception):
    pass 


def create_dag_decorator(data_pipeline):
    """
    Fill the dag decorator based on the parameters passed from the yaml file
    """

    dag_args = f"""@dag(
"""
    args = data_pipeline['dag']

    idx = 0
    for k,v in args.items():
        if idx == len(args) -1:
            if (k == 'start_date'):
                dag_args += f"\t{k} = datetime({v})"
            else:
                dag_args += f"\t{k} = '{v}'"
        else:
            if (k == 'start_date'):
                dag_args += f"\t{k} = datetime({v})"
            else:
                dag_args += f"\t{k} = '{v}', \n"
        idx += 1
    dag_args += "\n)"
    return dag_args


def create_task_function(task,project_name,dag_config):
    if len(task['output_data_collection']) <= 1:
        raise Exception("output_data_collection must have the arg 'path'")
    
    if len(task['output_data_collection']) > 2:
        raise MultipleOutputDataCollectionsException(f"The output data collection of {task['id']} must be unique")


    
    
    
    
    inputs = list()
    for data_collection in task['input_data_collections']:
        inputs.append(data_collection)
    input_string = ", ".join(inputs)

    output_string = task['output_data_collection'][0]
    input_string += ", " + output_string

    #Generate task_function
    task_function = task.get('task_function')
    if task_function is not None:
        with open(os.path.join(project_name, "utils.py"), 'a') as f:
            f.write(f"def {task['task_function']}({input_string}):\n")
            f.write("\tpass\n\n")


    #Generate check_output_state_function
    check_output_state = task.get('task_function')
    if check_output_state is not None:
        with open(os.path.join(project_name,"utils.py"), 'a') as f:
            f.write(f"def {task['check_output_state']}({output_string}):\n")
            f.write("\t pass\n\n")


    dag_file_content = ""
    dag_file_content += "   @task\n"
    dag_file_content += f"   def {task['id']}({input_string}):\n"
    if task_function is not None:
         dag_file_content += f"        utils.{task_function}({input_string})\n\n"
    else:
        dag_file_content += "        pass\n\n"


    # instantiate the output data collection
    dag_file_content += f"   {task['output_data_collection'][0]} = DataCollection({task['output_data_collection'][0], task['output_data_collection'][1]})\n"



    
    dag_file_content += f"   {task['id']} = {task['id']}({input_string})\n\n"

    return dag_file_content


def find_dependencies(tasks):
    output_to_tasks = {}
    
    for task in tasks:
        outputs = task['output_data_collection']
        
        for output in outputs:
            if isinstance(output, dict):
                output_name = output['path']  
            else:
                output_name = output
            if output_name not in output_to_tasks:
                output_to_tasks[output_name] = [task['id']]
            else:
                output_to_tasks[output_name].append(task['id'])

    task_to_upstream = {task['id']: [] for task in tasks}
    
    for task in tasks:
        inputs = task['input_data_collections']
        for input_item in inputs:
            if input_item in output_to_tasks:
                task_to_upstream[task['id']].extend(output_to_tasks[input_item])

    dependencies = []
    
    for task_id, upstream_tasks in task_to_upstream.items():
        upstream_tasks = list(set(upstream_tasks))  
        if len(upstream_tasks) > 0:
            if len(upstream_tasks) == 1:
                dependencies.append(f"   {upstream_tasks[0]} >> {task_id}")
            else:
                upstream_str = ", ".join(upstream_tasks)
                dependencies.append(f"   [{upstream_str}] >> {task_id}")

    return '\n'.join(dependencies)

def is_acyclic_graph(input_string):
    """
    The function returns True if the graph is acyclic, False otherwise
    
    input example : "task_1 >> task_2",
                    "task_1 >> task_3"
    """
    G = nx.DiGraph()
    input_lines = input_string.strip().split('\n')
    for line in input_lines:
        parts = line.split(">>")
        if len(parts) == 2:
            if parts[0].startswith('[') or parts[1].startswith('['):
                 return False
            start, end = parts[0].strip(), parts[1].strip()
            G.add_edge(start, end)
    return nx.is_directed_acyclic_graph(G)

class CyclicGraphException(Exception):
    pass

def create_dag_file(project_name, dag_config):
    """
    Create the python code
    """
    data_pipeline = dag_config['data_pipeline']
    default_args = convert_default_args(dag_config['default_args'])
    
    
    dag_file_content = f"""from airflow.decorators import dag, task
from datetime import datetime, timedelta
import utils

class DataCollection:
     def __init__(self, name, kw_args):
        self.name = name
        self.kw_args = kw_args




default_args = {default_args}




"""
    #Create utils file
    with open(os.path.join(project_name, "utils.py"), 'w') as f:
        f.write("# Write here tasks callbacks\n\n")

    dag_file_content += "# Data Collections instantiation\n"
    for data in dag_config['data_collections']:
        dag_file_content += f"{data[0]} = DataCollection('{data[0]}',{data[1]})\n"

        # create folders for initial data collections 
        base_path = f"{project_name}/" + data[1]['path']
        full_path = os.path.join(base_path)
        os.makedirs(full_path, exist_ok=True)



    dag_file_content += "\n"
    dag_file_content += "\n"

    



    dag_file_content += "# Dag instantiation\n"
    dag_file_content += create_dag_decorator(data_pipeline)
    dag_file_content += "\n"
    dag_file_content += "\n"
    

    dag_file_content += "# Tasks declaration\n"
    dag_file_content += "def define_pipeline():\n"

    for task in data_pipeline['tasks']:
        dag_file_content += f"{create_task_function(task,project_name,dag_config)}"


    dag_file_content += "# Tasks dependencies\n"
    dependencies = find_dependencies(data_pipeline['tasks'])
    dag_file_content += f"{dependencies}"


    dag_file_content += "\n\npipeline = define_pipeline()"
    
    if not is_acyclic_graph(dependencies):
        raise CyclicGraphException("The graph is not acyclic")
    


    with open(os.path.join(project_name, f"{data_pipeline['dag']['dag_id']}.py"), 'w') as f:
        f.write(dag_file_content)
    print(f"DAG '{data_pipeline['dag']['dag_id']}' creato.")



def main():
    # sys.argv = gli input da terminale
    if len(sys.argv) != 3:
        print("Uso: python crea_progetto_airflow.py nome_progetto config_dag.yaml")
        # TODO : lancia un eccezione al posto di fare print

    project_name = sys.argv[1]
    yaml_file_path = sys.argv[2]
    create_project_directory(project_name)
    dag_config = load_dag_config(yaml_file_path)
    create_dag_file(project_name, dag_config)
    
    print(f"Progetto Airflow '{project_name}' creato con successo basato su {yaml_file_path}.")

if __name__ == "__main__":
    main()