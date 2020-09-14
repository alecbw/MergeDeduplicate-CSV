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



argparser = argparse.ArgumentParser()
argparser.add_argument('-filename', help="Name of your input file")
argparser.add_argument('-output_filename', nargs='?', default="! Post_Deduplication.csv", help="If you want an output filename other than the default")
argparser.add_argument('-unique_key', help="The column you want to deduplicate/merge on")
# If you're getting "pandas.errors.ParserError: Error tokenizing data. C error" use -engine python
argparser.add_argument('-engine', nargs='?', default="c", help="Set to 'c' if you're getting Error tokenizing data errors")
argparser.add_argument('-encoding', nargs='?', default="utf-8", help="Usually ISO-8859-1 is a good alternative")
argparser.add_argument('-break_on_errors', nargs='?', default=True, help="Default behavior is to break on errors. Set to false if you dgaf")
argparser.add_argument('-concat_delimiter', nargs='?', default=', ', help="What concatenated strings will be separated by. Default is ', '")

args = argparser.parse_args()


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


def clean_string_infested_float_col(df, col):
    df[col] = df[col].replace("[^0-9.]", "", regex=True)
    # df[col] = None if df[col] == "." else df[col]
    df = df[df[col].str.match(".") == None]
    df[col] = df[col].astype(float)
    return df[col]

def convert_obj_col_to_float(df, col):
    df[col] = df[col].str.replace(",", "")
    df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

### TOOD DROP maybe
def prompt_user_for_col_types(df, col, groupby_dict, concat_delimiter):

    message = f"Options: P: Concatenate text; L: Sum numbers; M: Average numbers ---- {col}"

    questions = [inquirer.Text("col_merge_type", message=message)]
    answers = inquirer.prompt(questions)
    if answers["col_merge_type"] in ["p", "P", "P:", 0, "O", "o"]:
        print('Concatenating text')
        groupby_dict["Clean " + col] = (col, concat_delimiter.join)
        df[col] = df[col].astype(str)
    elif answers["col_merge_type"] in ["l", "L", "L:", "k", "K"]:
        print('Summing numbers')
        groupby_dict["Clean " + col] = (col, np.sum)
        df = convert_obj_col_to_float(df, col)
        # df[col] = df[col].to_numeric(errors='coerce')
        # df[col] = pd.to_numeric(df[col], errors='coerce')
        # df[col]= clean_string_infested_float_col(df, col)
    elif answers["col_merge_type"] in ["n", "N", "m", "M", "M:"]:
        print('Averaging (arithmetic mean) numbers')
        groupby_dict["Clean " + col] = (col, np.mean)
        df = convert_obj_col_to_float(df, col)
        # df[col] = pd.to_numeric(df[col], errors='coerce')
        # df[col] = df[col].to_numeric(errors='coerce')
        # df[col]= df[col].replace("[^0-9.]", "", regex=True).astype(float)
    elif answers["col_merge_type"] in ["Q", "q", "a", "A", "w", "W"]:
        print(f"Dropping the column {col}")
        del df[col]

    else:
        print("The first value in any duplicate pair of rows will be applied")
        groupby_dict["Clean " + col] = (col, "first")

    return df, groupby_dict


if __name__ == "__main__":
    # list_of_files = [f for f in os.listdir('.') if (os.path.isfile(f) and f".{args.type.lower()}" in f and f != args.output_filename and os.path.getsize(f) != 0)]
    filename = args.filename if any(x for x in [".csv", ".xlsx"] if x in args.filename) else args.filename + ".csv"

    df = pd.read_csv(filename, encoding=args.encoding, engine=args.engine, error_bad_lines=detect_boolean(args.break_on_errors), escapechar='\\')

    print(f"File is of shape {df.shape}, with columns {df.columns}")
    print("\nFor each column, you decide how rows with same unique_key are merged. Your options are as follows:")
    print("Press P and Enter - Concatenate (combine) text")
    print("Press L and Enter - Sum numbers")
    print("Press M and Enter - Average (arithmetic mean) numbers")
    print("Press Q and Enter - The column will be removed from the output")
    print("Just Press Enter - The first value found will be used")
    groupby_dict = {}
    for col in df.columns:
        print(df[col].dtypes)
        df, groupby_dict = prompt_user_for_col_types(df, col, groupby_dict, args.concat_delimiter)

    print(groupby_dict)
    df2 = df.groupby(args.unique_key).agg(**groupby_dict).reset_index()
    # df = df.groupby(args.unique_key).agg(**groupby_dict).reset_index()
# max_height=('height', 'max'), min_weight=('weight', 'min'),)

    print(f"After the merge, the file is of shape {df.shape}, with columns {df.columns}")

    df2.to_csv(args.output_filename, index=False)

    print(f"Now written the output file with name {args.output_filename}")


# Print to the termnal in color!
def colorful_print(inputText, inputColor):
  color_dict = {
    'White': '\033[39m',
    'Red': '\033[31m',
    'Blue': '\033[34m',
    'Green': '\033[32m',
    'Orange': '\033[33m',
    'Magenta': '\033[35m',
    'Red_Background': '\033[41m',
  }

  color = color_dict[inputColor.title()]

  if isinstance(inputText, tuple) or isinstance(inputText, list):
    inputText = ', '.join(str(v) for v in inputText)

  print(color + inputText + color)
  print(color_dict['White'] + " " + color_dict['White'])

# df = df.groupby('Name').agg({'Sid':'first',
#                              'Use_Case': ', '.join,
#                              'Revenue':'first' }).reset_index()



        # if col_merge_type == "Concatenating text":
        #     groupby_dict[col] = concat_delimiter.join
        # elif col_merge_type == "Summing numbers":
        #     groupby_dict[col] = np.sum
        # else:
        #     continue