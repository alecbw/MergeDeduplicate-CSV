import sys
import os
import argparse
import itertools
import re

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
    df[col] = df[col].str.replace(",", "")
    df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def validate_val_not_various_nulls(val):
    if pd.isnull(val):
        return 0
    null_values = ["", " ", "[]", "{}", [], {}]
    return 0 if val in null_values else 1


def deduplicate_and_join(x):
    deduplicated_values_for_one_unique_key = {y for y in x if validate_val_not_various_nulls(y)}
    return concat_delimiter.join(deduplicated_values_for_one_unique_key)


def prompt_user_for_col_types(df, col, groupby_dict, concat_delimiter):

    message = f"For col: {col} - how would you like it to be merged?"
    questions = [inquirer.Text("col_merge_type", message=message)]
    answers = inquirer.prompt(questions)
    answer = answers["col_merge_type"].replace('"', '')

    if answer in ["c", "cat", "Cat", "concat", "combine"]:
        print('Concatenating text')
        groupby_dict["Clean " + col] = (col, concat_delimiter.join)
        df[col] = df[col].astype(str)

    # FYI this is compute expensive
    elif answer in ["ddc", "DDC", "dedupe_cat", "dd_cat", "deduplicate_concat"]:
        print('Deduplicating then concatenating text')
        groupby_dict["Clean " + col] = (col, deduplicate_and_join)
        df[col] = df[col].astype(str)

    elif answer in ["s", "sum", "Sum", "total"]:
        print('Summing numbers')
        groupby_dict["Clean " + col] = (col, np.sum)
        df = convert_obj_col_to_float(df, col)

    elif answer in ["mean", "Mean", "avg", "Avg", "average"]:
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

    elif answer in ["drop", "Drop"]:
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

    print(f"File is of shape {df.shape}, with columns {df.columns}")

    print_in_color("\nFor each column, you decide how rows with same unique_key are merged. Your options are as follows:", "Bold")
    print_in_color('"cat" and Enter - Concatenate text', 'Cyan')
    print_in_color('"ddc" and Enter - Deduplicate, and then Concatenate text', 'Cyan')
    print_in_color('"sum" and Enter - Sum numbers', 'Cyan')
    print_in_color('"avg" and Enter - Average (arithmetic mean) numbers', 'Cyan')
    print_in_color('"max" and Enter - Take the highest value', 'Cyan')
    print_in_color('"min" and Enter - Taking the lowest value', 'Cyan')
    print_in_color(f'"drop" and Enter - Drops the column, so it wont be in the output', 'Cyan')
    print_in_color("Just Press Enter - The first value found will be used", 'Cyan')

    global concat_delimiter
    concat_delimiter = config_args['concat_delimiter']

    groupby_dict = {}
    for col in df.columns:
        df, groupby_dict = prompt_user_for_col_types(df, col, groupby_dict, concat_delimiter)

    df2 = df.groupby(config_args['unique_key']).agg(**groupby_dict).reset_index()

    print(f"After the merge, the file is of shape {df2.shape}, with columns {df2.columns}")

    df2.to_csv(config_args['output_filename'], index=False)

    print_in_color(f"Now written the output file with name {config_args['output_filename']}", "Green")



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-filename', help="Name of your input file")
    argparser.add_argument('-output_filename', nargs='?', default="! Post_Deduplication.csv", help="If you want an output filename other than the default")
    argparser.add_argument('-unique_key', help="The column you want to deduplicate/merge on")
    # If you're getting "pandas.errors.ParserError: Error tokenizing data. C error" use -engine python
    argparser.add_argument('-engine', nargs='?', default="c", help="Set to 'c' if you're getting Error tokenizing data errors")
    argparser.add_argument('-encoding', nargs='?', default="utf-8", help="Usually ISO-8859-1 is a good alternative")
    argparser.add_argument('-sep', nargs='?', default=",", help="The cell delimiter (seperator). Default is ','")
    argparser.add_argument('-break_on_errors', nargs='?', default=True, help="Default behavior is to break on errors. Set to false if you dgaf")
    argparser.add_argument('-concat_delimiter', nargs='?', default=', ', help="What concatenated strings will be separated by. Default is ', '")

    args = argparser.parse_args()
    args = vars(args)
    main_op(args)
