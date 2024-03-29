import sys
import os
import argparse
import itertools
import re
import json
from pprint import pprint

import inquirer

try:
    import pandas as pd
    import numpy as np
except ImportError:
    sys.exit("~ Make sure you install pandas and numpy. Run `pip install pandas numpy` and try this again ~")


def detect_boolean(v):

    if type(v) == type(True) and v:
        return True
    if type(v) == type(True) and not v:
        return False
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

# Print to the termnal in color!
def print_in_color(text, color):
    color_dict = {
        'White': '\033[39m',
        'Red': '\033[31m',
        'Blue': '\033[34m',
        "Cyan": '\033[36m',
        "Bold": '\033[1m',
        'Green': '\033[32m',
        'Orange': '\033[33m',
        'Magenta': '\033[35m',
        'Red_Background': '\033[41m',
    }

    color = color_dict[color.title()]
    print(color + text + color + color_dict['White'])
    # Trailing white prevents the color from staying applied


def convert_obj_col_to_float(df, col):
    if df.dtypes[col] not in ["int64", "float64"]:
        df[col] = df[col].str.replace(",", "")
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def validate_val_not_various_nulls(val):
    if pd.isnull(val):
        return 0
    null_values = ["", " ", "[]", "{}", [], {}, "0", np.nan, float('nan'), "nan", None]
    return 0 if val in null_values else 1

# 
def deduplicate_and_join(x):
    deduplicated_values_for_one_unique_key = {y for y in x if validate_val_not_various_nulls(y)}
    return concat_delimiter.join(deduplicated_values_for_one_unique_key)

 
# input is a series (not the full col) group'd-by the unique_key
def split_and_deduplicate_and_join(series):
    output_set = set()
    for cell in series:
        if not cell:
            continue
        for val in cell.split(concat_delimiter):
            if validate_val_not_various_nulls(val):
                output_set.add(val)

    return concat_delimiter.join(output_set)


def load_col_types_from_schema_json(config_args, df, concat_delimiter):
    operations_options = {
        "cat": concat_delimiter.join,
        "ddc": deduplicate_and_join,
        "sdc": split_and_deduplicate_and_join,
        "sum": np.sum,
        "avg": np.mean,
        "max": np.max,
        "min": np.min,
        "first": "first",

    }
    groupby_dict = {}

    if not (os.path.isfile(config_args['schema_json']) and '.json' in config_args['schema_json'] and os.path.getsize(config_args['schema_json']) != 0):
        raise IOError("Ensure your `schema_json` file is in the local dir, is named correctly, and has the .json suffix")

    with open(config_args['schema_json']) as f:
        schema_dict = json.load(f, strict=True)

    unique_keys = config_args["unique_key"].split(",") if "," in config_args["unique_key"] else [config_args["unique_key"]]

    if not (set(unique_keys + list(schema_dict.keys())) == set(df.columns.tolist())):
        raise IOError("Ensure the `schema_json` file has a k:v pair for each NON-unique_key column in the data")

    for col, v in schema_dict.items():
        if v in ['cat', 'ddc', 'sdc']:
            df[col] = df[col].astype(str)
        elif v in ['sum', 'avg', 'max', 'min']:
            df = convert_obj_col_to_float(df, col)
        elif v == 'drop':
            del df[col]

        if v != "drop":
            groupby_dict["Clean " + col] = (col, operations_options[v]) # the groupby requires this tuple structure

    print("These operations will be undertaken:\n")
    pprint(schema_dict)

    return df, groupby_dict

def prompt_user_for_col_types(df, col, groupby_dict, concat_delimiter):

    message = f"\nFor col: {col} - how would you like it to be merged?"
    questions = [inquirer.Text("col_merge_type", message=message)]
    answers = inquirer.prompt(questions)
    answer = answers["col_merge_type"].replace('"', '')

    if answer in ["cat", "Cat", "concat", "combine"]:
        print('Concatenating text')
        groupby_dict["Clean " + col] = (col, concat_delimiter.join)
        df[col] = df[col].astype(str)

    # FYI this is compute expensive
    elif answer in ["ddc", "DDC", "dd_cat", "dd_concat", "dedupe_cat", "deduplicate_concat"]:
        print('Deduplicating then concatenating text')
        groupby_dict["Clean " + col] = (col, deduplicate_and_join)
        df[col] = df[col].astype(str)

    elif answer in ["sdc", "split_dd_cat", "split_dupe_cat"]:
        print(f'Splitting each cell by "{concat_delimiter}", deduplicating, then concatenating text')
        groupby_dict["Clean " + col] = (col, split_and_deduplicate_and_join)
        df[col] = df[col].astype(str)

    elif answer in ["sum", "Sum", "total"]:
        print('Summing numbers')
        groupby_dict["Clean " + col] = (col, np.sum)
        df = convert_obj_col_to_float(df, col)

    elif answer in ["avg", "Avg", "mean", "Mean", "average"]:
        print('Averaging (arithmetic mean) numbers')
        groupby_dict["Clean " + col] = (col, np.mean)
        df = convert_obj_col_to_float(df, col)
    
    elif answer in ["max", "Max", "maximum", "most", "highest"]:
        print('Taking the highest value')
        groupby_dict["Clean " + col] = (col, np.max)
        df = convert_obj_col_to_float(df, col)
    
    elif answer in ["min", "Min", "minimum", "lowest"]:
        print('Taking the lowest value')
        groupby_dict["Clean " + col] = (col, np.min)
        df = convert_obj_col_to_float(df, col)

    elif answer in ["drop", "Drop", "skip"]:
        print(f"Dropping the column {col}")
        del df[col]

    else:
        print("The first value in any duplicate pair of rows will be applied")
        groupby_dict["Clean " + col] = (col, "first")

    return df, groupby_dict


def main_op(config_args):
    
    if "file" in config_args:
        df = config_args.get("file")
    else:
        filename = config_args.get('filename') if any(x for x in [".csv", ".xlsx"] if x in config_args.get('filename')) else config_args.get('filename') + ".csv"
        df = pd.read_csv(filename, sep=args["sep"], encoding=config_args['encoding'], engine=config_args['engine'], error_bad_lines=detect_boolean(config_args['break_on_errors']), escapechar='\\')

    # print(f"Your file has {df.shape[0]} rows and {df.shape[1]} columns.")
    # print(f"The columns are: {[f'{x}: {x.dtypes}' for x in df.columns.to_list()]}")
    print(f"Here's an overview of your file: {filename}")
    print(df.info())

    global concat_delimiter
    concat_delimiter = config_args['concat_delimiter']

    if config_args.get('schema_json'):
        df, groupby_dict = load_col_types_from_schema_json(config_args, df, concat_delimiter)

    else:
        print_in_color("\nFor each column, you decide how rows with same unique_key are merged. Your options are as follows:", "Bold")
        print_in_color('"cat" and Enter - Concatenate text', 'Cyan')
        print_in_color('"ddc" and Enter - Deduplicate, and then Concatenate text', 'Cyan')
        print_in_color(f'"sdc" and Enter - Split by "{concat_delimiter}", Deduplicate, and then Concatenate text', 'Cyan')
        print_in_color('"sum" and Enter - Sum numbers', 'Cyan')
        print_in_color('"avg" and Enter - Average (arithmetic mean) numbers', 'Cyan')
        print_in_color('"max" and Enter - Take the highest value', 'Cyan')
        print_in_color('"min" and Enter - Take the lowest value', 'Cyan')
        print_in_color('"drop" and Enter - Drop the column, so it wont be in the output', 'Cyan')
        print_in_color("Just Press Enter - The first value found will be used", 'Cyan')
        print("\n If you make a mistake, hit CONTROL+C to start over\n")
        print()

        groupby_dict = {}
        for col in df.columns:
            if col not in config_args["unique_key"]:
                df, groupby_dict = prompt_user_for_col_types(df, col, groupby_dict, concat_delimiter)


    print("\nBeginning the merge")

    if config_args['case_insensitive']:
        groupby_key = df[config_args['unique_key']].str.lower()
    else:
        groupby_key = [x.strip() for x in config_args['unique_key'].split(",")]


    df2 = df.groupby(groupby_key).agg(**groupby_dict).reset_index()
    
    df2 = df2.rename(columns=lambda x: x.strip().replace("Clean ", ""))

    print(f"After the merge, the file is of shape {df2.shape}, with columns {df2.columns.tolist()}")

    output_filename = config_args.get('output_filename', "MDD_" + config_args["filename"])

    df2.to_csv(output_filename, index=False, encoding='utf-8')

    print_in_color(f"Now finished. The output file has been written with name {config_args['output_filename']}", "Green")


"""
FYI -case_insensitive and -unique_key being a list are incompatible
if you pass a list unique_key, it will drop every row that doesn't have a value in each listed col
"""


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-filename', required=True, help="Name of your input file")
    argparser.add_argument('-output_filename', nargs='?', default="! Post_Deduplication.csv", help="If you want an output filename other than the default")
    argparser.add_argument('-unique_key', required=True, help="The column you want to deduplicate/merge on")
    # If you're getting "pandas.errors.ParserError: Error tokenizing data. C error" use -engine python
    argparser.add_argument('-engine', nargs='?', default="c", help="Set to 'c' if you're getting Error tokenizing data errors")
    argparser.add_argument('-encoding', nargs='?', default="utf-8", help="Usually ISO-8859-1 is a good alternative")
    argparser.add_argument('-sep', nargs='?', default=",", help="The cell delimiter (seperator). Default is ','")
    argparser.add_argument('-break_on_errors', nargs='?', default=True, help="Default behavior is to break on errors. Set to false if you dgaf")
    argparser.add_argument('-concat_delimiter', nargs='?', default=', ', help="What concatenated strings will be separated by. Default is ', '")
    argparser.add_argument('-case_insensitive', nargs='?', default=False, help="If you want the unique_key merge to be case insensitive. Default is 'a' != 'A'")
    argparser.add_argument('-schema_json', nargs='?', default=False, help="Name of a JSON file containing the col name and merge types you want run instead of the interactive CLI")

    args = argparser.parse_args()
    args = vars(args)
    main_op(args)



# maybe TODO a helper that detects "Unnamed: n" cols that have no values and drops them
