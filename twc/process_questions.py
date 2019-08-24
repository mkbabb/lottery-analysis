import pandas as pd
import sqlite3
from typing import List, Dict, Union, Any, Tuple

from sql_keywords import sql_keywords

conn = sqlite3.connect("twc.db")


def xlsx_to_df(file):
    xl = pd.ExcelFile(file)
    return [pd.read_excel(xl, i) for i in xl.sheet_names]


sheets = xlsx_to_df("twc/TWC_NC16_codebook_final for NCDLCN.xlsx")

for i in sheets:
    i.replace({r"NC\d+_": "NC18_"}, regex=True, inplace=True)


tag_group_mapping = \
    dict(
        zip(sheets[0]["Tag"], sheets[0]["Answer Group"])
    )

code_value_mapping = \
    dict(
        sheets[2]
        .groupby("Name")["Value"]
        .apply(lambda x:
               list(
                   zip(
                       range(1, len(x)), list(x)
                   )))
    )

base_template_command = lambda table_name, pivot_on, pivot_with, summarize_by, cases, pivot_output_columns: f'''
WITH toast AS ( SELECT * FROM {table_name} ), pancake AS ( SELECT {pivot_with} AS answerColumn, {summarize_by}, * FROM toast GROUP BY answerColumn, {pivot_on}), crepe AS ( SELECT {cases}, * FROM pancake), bacon AS ( SELECT {pivot_output_columns} FROM crepe GROUP BY {pivot_on} )SELECT * FROM bacon
'''
case_template_command = lambda answer_when, answer_then, answer_else, answer_as: f'''
(CASE WHEN answerColumn = {answer_when} THEN {answer_then} ELSE {answer_else} END) AS {answer_as}'''


def escape_sql_keyword(keyword: str) -> str:
    temp_k = keyword.strip().upper()
    if (temp_k in sql_keywords):
        keyword = "_" + keyword
    return keyword


def sanitize_pivot_predicates(pivot_predicates: List[List[str]]):
    for n, i in enumerate(pivot_predicates):
        if (len(i) == 2):
            pivot_predicates[n].append("NULL")
            pivot_predicates[n].append(f"pivoter_{n}")
        elif (len(i) == 3):
            pivot_predicates[n].append(f"pivoter_{n}")
        pivot_predicates[n][3] = escape_sql_keyword(
            str(pivot_predicates[n][3]))\
            .replace(" ", "_")
    return pivot_predicates


def pivot_by(table_name: str,
             pivot_on: str,
             pivot_with: str,
             pivot_predicates: List[List[Any]],
             pivot_output_columns: List[str],
             summarize_by: List[str],
             pivot_output_operation=None
             ):
    if not pivot_output_operation:
        pivot_output_operation = lambda x: ""

    pivot_maxes = [f"max(_{i[3]}) AS {i[3]}" for i in pivot_predicates]
    pivot_operations = pivot_output_operation(
        [f"max(_{i[3]})" for i in pivot_predicates])

    pivot_output_columns_string = \
        ", ".join(pivot_output_columns) + ", " \
        + ", ".join(pivot_maxes) + ", " \
        + pivot_operations

    cases = ", ".join(case_template_command(
        i[0], i[1], i[2] if i[2] != "" else "answerColumn", f"_{i[3]}") for i in pivot_predicates)

    summarize_by_string = ", ".join(summarize_by)

    return base_template_command(table_name, pivot_on, pivot_with, summarize_by_string, cases, pivot_output_columns_string)


def generate_command(question_column: str,
                     tag: str = None):
    if (not tag):
        tag = tag_group_mapping[question_column]

    codes = code_value_mapping[tag]

    table_name = "data"
    pivot_on = "MasterSiteID"
    pivot_with = question_column

    pivot_predicates = sanitize_pivot_predicates(
        [[i[0],
          "answerCount",
          "0",
          i[1]] for i in codes
         ]
    )

    pivot_output_columns = ["MasterSiteID", "orgName"]

    summarize_by = ["count(*) AS answerCount"]
    pivot_operation = lambda x: f"({'+'.join(x)}) AS _total"

    pivot_command = pivot_by(
        table_name, pivot_on, pivot_with, pivot_predicates, pivot_output_columns, summarize_by, pivot_operation)
    print(pivot_command)


generate_command("NC18_tmt046collabpln", "021agree")
