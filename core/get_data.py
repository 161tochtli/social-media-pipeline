import os
import json
import time
import requests
import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
import functools
from core import alogger

log_name = "social_media_comments"
work_dir= "core"
log = alogger.get_logger(work_dir, log_name)
# local = pytz.timezone("America/Mexico_City")
# pattern = "%Y-%m-%d %H:%M:%S"

JSON_FILES = Path("json_files/")
CSV_FILES = Path("csv_files/")
CONTROL = Path("core/control/")


def build_control(source, params, control_group):
    source_name = source['name']
    filekey = source['filekey'](params) if params else source['filekey']
    control_group = control_group or filekey  # If not given name for control_group assigns source's name
    created_at = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    control_id = f"{filekey}_{created_at}"
    control_group_path = CONTROL / source_name / control_group
    controls_group_path = control_group_path / "controls"
    csv_path = (CSV_FILES / source_name / control_group / f"{source_name}_{control_group}.csv")
    responses_dir = Path(JSON_FILES / source_name / control_group)
    control_path = (controls_group_path / control_id).with_suffix(".json")
    control_md = {"source_name": source_name, "params": params, "created_at": created_at,
                  "control_group": control_group, "control_group_path": str(control_group_path),
                  "control_id": control_id, "control_path": str(control_path),
                  "responses_dir": str(responses_dir), "csv_path": str(csv_path)}
    os.makedirs(controls_group_path, exist_ok=True)
    os.makedirs(JSON_FILES / source_name / control_group / 'error', exist_ok=True)
    os.makedirs(JSON_FILES / source_name / control_group / 'headers', exist_ok=True)
    os.makedirs(JSON_FILES / source_name / control_group / 'headers' / 'error', exist_ok=True)

    # write control_group metadata
    if not os.path.isfile("meta.json"):
        meta = {"source_name": source_name, "control_group": control_group,
                "controls_group_path": str(controls_group_path), "control_meta_path": str(control_group_path/"meta.json"),
                "responses_dir": str(responses_dir), "csv_path": str(csv_path)}
        with open(meta["control_meta_path"], 'w') as mf:
            json.dump(meta, fp=mf)

    return control_md


class Control:

    def __init__(self):
        self.build_control()

    def build_control(self, source, params, control_group):
        source_name = source['name']
        filekey = source['filekey'](params) if params else source['filekey']
        control_group = control_group or filekey  # If not given name for control_group assigns source's name
        created_at = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        control_id = f"{filekey}_{created_at}"
        control_group_path = CONTROL / source_name / control_group
        controls_group_path = control_group_path / "controls"
        csv_path = (CSV_FILES / source_name / control_group / f"{source_name}_{control_group}.csv")
        responses_dir = Path(JSON_FILES / source_name / control_group)
        control_path = (controls_group_path / control_id).with_suffix(".json")
        control_md = {"source_name": source_name, "params": params, "created_at": created_at,
                      "control_group": control_group, "control_group_path": str(control_group_path),
                      "control_id": control_id, "control_path": str(control_path),
                      "responses_dir": str(responses_dir), "csv_path": str(csv_path)}
        os.makedirs(controls_group_path, exist_ok=True)
        os.makedirs(JSON_FILES / source_name / control_group / 'error', exist_ok=True)
        os.makedirs(JSON_FILES / source_name / control_group / 'headers', exist_ok=True)
        os.makedirs(JSON_FILES / source_name / control_group / 'headers' / 'error', exist_ok=True)

        # write control_group metadata
        if not os.path.isfile("meta.json"):
            meta = {"source_name": source_name, "control_group": control_group,
                    "controls_group_path": str(controls_group_path),
                    "control_meta_path": str(control_group_path / "meta.json"),
                    "responses_dir": str(responses_dir), "csv_path": str(csv_path)}
            with open(meta["control_meta_path"], 'w') as mf:
                json.dump(meta, fp=mf)

        return control_md

    def responses(self):
        files = [f for f in Path(self.responses_dir).iterdir() if f.is_file()]
        files = [f for f in files if f.stem[:len(self.control_id)] == self.control_id]
        # files.sort(key=lambda x: int(x.stem.split("_")[-1]))
        return files


def responses_for_control(control):
    responses_dir = control["responses_dir"]
    control_id = control["control_id"]
    files = [f for f in Path(responses_dir).iterdir() if f.is_file()]
    files = [f for f in files if f.stem[:len(control_id)] == control_id]
    # files.sort(key=lambda x: int(x.stem.split("_")[-1]))
    return files

def download_data(token, source, kind, params=None, control_group=None, control_path=None):

    source_name = source['name']
    controls_group_path = CONTROL / source_name / (control_group or '') / 'controls'
    control_meta_path = CONTROL / source_name / (control_group or '') / 'meta.json'

    if control_path and os.path.isfile(control_path):
        if kind == 'continue':
            controls = [get_control(control_path)]
        elif kind == 'restart':
            control_md = get_control(control_path)
            control_md.update({"last_page": None, "next_batch": None})
            with open(control_md["control_path"], "w") as jf:
                json.dump(control_md, fp=jf)
            controls = [control_md]
    elif control_group:
        if controls_group_path.is_dir() and kind != 'new':
            files = [f for f in controls_group_path.iterdir() if f.is_file()]
            files.sort(key=lambda x: int(x.stem.split("_")[-1]))
            if kind == 'continue':
                filekey = source['filekey'](params)
                files = [f for f in files if f.stem[:len(filekey)] == filekey]
                controls = [get_control(file) for file in files]
            elif kind == 'restart':
                controls = []
                for file in files:
                    control_md = get_control(file)
                    control_md.update({"last_page": None, "next_batch": None})
                    with open(control_md["control_path"], "w") as jf:
                        json.dump(control_md, fp=jf)
                    controls.append(control_md)
        else:
            control_md = build_control(source, params, control_group)
            controls = [control_md]

    url = source['url']
    # token_type = source.get('token_type')
    limit = str(source.get('limit', ''))
    limit_param = f"&limit={limit}" if limit else ''
    since = str(params.get('since', ''))
    since_param = f"&since={since}" if since else ''
    until = str(params.get('until', ''))
    until_param = f"&until={until}" if until else ''
    payload = source['payload'](params) if params else source['payload']
    full_url = f"{url}{payload}{limit_param}{since_param}{until_param}"

    calls_limit = 15
    total_calls = 0
    for control_md in tqdm(controls):
        log.info(f"Iniciando el proceso de descarga")
        log.info(f"source: {source['name']}, group: {control_md['control_group']}, control_id: {control_md['control_id']}")
        log.info(f"params: {params}")

        control_group = control_md['control_group']
        next_batch = control_md.get("next_batch", full_url)
        page = control_md.get("last_page", 0)

        while next_batch:
            # total_calls += 1
            # log.info(f"Total calls: {total_calls}")
            # if total_calls > calls_limit:
            #     total_calls = 0
            #     wait = 300
            #     log.info(f"Waiting {wait/60} minutes before next call")
            #     tqdm_sleep(wait, interval=30)

            page += 1
            log.info(f"Page {page}")
            try:

                response = requests.get(url=next_batch+f"&access_token={token}")
                headers = dict(response.headers)
                # usage = json.dumps(response.headers.get('x-business-use-case-usage', {}))
                response = response.json()

                error = 'error' in response
                # TODO: No todas las respuestas tienen la llave "data"
                to_path = Path(source_name)/control_group/('error' if error else '')
                with open((JSON_FILES/to_path/f"{control_md['control_id']}_{page}").with_suffix(".json"), "w") as jf:
                    json.dump(response, fp=jf)

                if error:
                    error = response.get('error', {})
                    log.error(f"{error.get('message')} Error type: {error.get('type')}")
                    with open((JSON_FILES / source_name / control_group / "headers" / "error" / f"{control_md['control_id']}_{page}").with_suffix(".json"), "w") as jf:
                        json.dump(headers, fp=jf)
                    raise Exception

                else:
                    with open((JSON_FILES / source_name / control_group / "headers" / f"{control_md['control_id']}_{page}").with_suffix(".json"), "w") as jf:
                        json.dump(headers, fp=jf)
                    cursor_after = response.get('paging', {}).get('cursors', {}).get('after')
                    next_batch = response.get('paging', {}).get('next')
                    next_batch = f"{full_url}&after={cursor_after}" if next_batch else None
                    # Save control data for the download task
                    control_md.update({#"last_headers":headers,
                                       "last_page": page,
                                       "next_batch": next_batch,
                                       "updated_at": datetime.datetime.now().strftime("%Y%m%d%H%M%S")})
                    with open(control_md["control_path"], "w") as jf:
                        json.dump(control_md, fp=jf)

            except Exception as e:
                log.error(f"Error en la descarga: {e}")
                break

        log.info("Fin del proceso de descarga")

    return get_control(control_meta_path)


def response_to_df(response, params=None, **kwargs):
    # transform response
    data = response.get('data', [])
    # normalize df
    new_df = pd.json_normalize(data, errors='ignore', **kwargs)
    if params:
        inter = new_df.columns.intersection(params.keys())
        if not inter.empty:
            log.error(f"{inter} columns params already in dataframe. Please add or change params_prefix.")
            raise Exception
        new_df = new_df.assign(**params)
    return new_df


def safe_append(csv_path, new_df):
    if os.path.isfile(csv_path):
        # df.to_csv(file_name, mode='a',index=False)

        # Assuming `new_df` is your new DataFrame to append
        # Replace 'your_csv_file.csv' with the path to your existing CSV
        existing_df = pd.read_csv(csv_path)

        # Get the union of columns from both DataFrames
        combined_columns = existing_df.columns.union(new_df.columns)

        # Reindex both DataFrames to have the same columns, filling missing ones with NaNs
        existing_df_aligned = existing_df.reindex(columns=combined_columns, fill_value=np.nan)
        new_df_aligned = new_df.reindex(columns=combined_columns, fill_value=np.nan)

        # Concatenate the DataFrames
        final_df = pd.concat([existing_df_aligned, new_df_aligned], ignore_index=True)
        # Save the combined DataFrame back to CSV
        final_df.to_csv(csv_path, index=False)#, sep="|")

    else:
        os.makedirs(Path(csv_path).parent, exist_ok=True)
        new_df.to_csv(csv_path, mode='w', index=False)#, sep="|")


def process_single_response(response, file_path, params=None, **kwargs):
    try:
        params = params or {}
        new_df = response_to_df(response, params, **kwargs)
        safe_append(file_path, new_df)
    except Exception as e:
        log.error(f"Error processing response: {e}")
        raise e


def process_responses(source, control_group=None, control_path=None, params_cols=False, params_prefix=None, transform_response=None, **kwargs):
    log.info(f"Iniciando el proceso")
    source_name = source['name']
    control_path = Path(control_path) if control_path else None
    control_meta_path = CONTROL / source_name / (control_group or '') / 'meta.json'
    controls = []
    if control_path and control_path.is_file():
        control_md = get_control(control_path)
        csv_path = Path(control_md['csv_path'])
        controls = [control_md]
    elif control_group or control_group == '':
        controls_group_path = CONTROL / source_name / control_group / "controls"
        csv_path = CSV_FILES / source_name / control_group / f"{source_name}_{control_group}.csv"
        controls = [get_control(c) for c in controls_group_path.iterdir()]

    log.info(f"Output: {csv_path.resolve()}")



    for control_md in tqdm(controls):
        try:
            files = responses_for_control(control_md)
            files.sort(key=lambda x: int(x.stem.split("_")[-1]))

            if params_cols:
                params = control_md.get('params', {})
                pfx = params_prefix or ''
                if isinstance(params_cols, bool):
                    col_params = {pfx + k: v for k, v in params.items()}
                else:
                    col_params = {pfx + k: v for k, v in params.items() if k in params_cols}
            else:
                col_params = None

            for response_file in files:
                with open(response_file, "r") as jf:
                    response = json.load(fp=jf)
                if transform_response is not None:
                    response = transform_response(response)
                process_single_response(response, csv_path, col_params, **kwargs)

            try:
                if os.path.isfile(csv_path):
                    df = pd.read_csv(csv_path)
                    log.info(f"Total records: {len(df)}")
            except Exception as e:
                log.error(f"Error reading file: {e}")

        except Exception as e:
            log.error(f"Error in process_responses: {e}")
        finally:
            log.info(f"Finalizando el proceso")
    return get_control(control_meta_path)

def get_control(control_path):
    with open(control_path, "r") as f:
        control_md = json.load(fp=f)
    return control_md


def tqdm_sleep(secs,interval=30):
    # Calcula el número total de intervalos
    num_intervals = int(secs / interval)

    # Usa tqdm para mostrar una barra de progreso
    for _ in tqdm(range(num_intervals), desc="Esperando"):
        time.sleep(interval)


# va a ser un decorador
def test_transform_response(func):
    @functools.wraps(func)
    def wrapper(json_path):
        output_dir = Path("test_transform")
        output_dir.mkdir(parents=True, exist_ok=True)

        json_path = Path(json_path)

        # Attempt to read and parse the JSON file
        try:
            with open(json_path, 'r') as jf:
                response = json.load(jf)
        except json.JSONDecodeError:
            # Handle empty or invalid JSON file
            print(f"Error: The file '{json_path}' is empty or not a valid JSON.")
            return None

        transformed_response = func(response)

        output_file = output_dir / json_path.name
        with open(output_file, 'w') as jf:
            json.dump(transformed_response, jf)

        return transformed_response

    return wrapper


def get_group_control(source, control_group):
    meta = get_control(CONTROL/source/control_group/'meta.json')
    return meta
