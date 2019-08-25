import pandas as pd
import sqlite3
from typing import List, Dict, Union, Any, Tuple, Callable

from sql_keywords import sql_keywords

conn = sqlite3.connect("twc.db")

# Flattens the twc xlsx file.


def xlsx_to_df(file):
    xl = pd.ExcelFile(file)
    return [pd.read_excel(xl, i) for i in xl.sheet_names]


sheets = xlsx_to_df("twc/TWC_NC16_codebook_final for NCDLCN.xlsx")
for i in sheets:
    i.replace({r"NC\d+_": "NC18_"}, regex=True, inplace=True)


'''
Creates two mappings of the TWC data: first for the tag data, then for the code data.
'''
tag_group_mapping = \
    dict(
        zip(
            sheets[0]["Tag"],
            sheets[0]["Answer Group"])
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

# Compressed and obfuscated subsequent command for generating the pivot tables.
base_template_command = lambda table_name, pivot_on, pivot_columns, pivot_columns_as, summarize_by, cases, pivot_output_columns: f'''
WITH toast AS ( SELECT * FROM {table_name} ), pancake AS ( SELECT {pivot_columns}, {summarize_by}, * FROM toast GROUP BY {pivot_columns_as}, {pivot_on}), crepe AS ( SELECT {cases}, * FROM pancake), bacon AS ( SELECT {pivot_output_columns} FROM crepe GROUP BY {pivot_on} )SELECT * FROM bacon
'''

case_template_command = lambda answer_when, answer_then, answer_else, answer_as, count: f'''
(CASE WHEN answerColumn_{count} = {answer_when} THEN {answer_then} ELSE {answer_else} END) AS {answer_as}'''


def escape_sql_keyword(keyword: str) -> str:
    '''
    Simply prepends a "_" upon an input keyword's existence in sql_keywords.

    @param keyword: input string keyword

    @returns: potentially escaped input string keyword.
    '''
    temp_k = keyword.strip().upper()
    if (temp_k in sql_keywords):
        keyword = "_" + keyword
    return keyword


def sanitize_pivot_predicates(
        pivot_predicates: List[List[str]]) -> List[List[str]]:
    '''
    Takes in a list of pivot_predicates, sanitizes them accordingly (from the below function):

    "The latter two elements of the aforesaid pivot_predicates may be elided by the user, whereupon they'll be inferred and or replaced by temporary or default values."

    @param pivot_predicates: 2-d list of pivot predicates containing four                                elements each.

    @returns: sanitized pivot_predicates.
    '''
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
             pivot_with: Dict[str, List[List[str]]],
             pivot_output_columns: List[str],
             summarize_by: List[str],
             pivot_output_operation: Callable[[List[str]], str] = None
             ) -> str:
    '''
    Takes a sql table, table_name, and pivots by pivot_on, reducing a column(s) specified pivot_with. For this method of pivoting to work, the bounds of the pivot table must be stipulated aforehand, betokened by the pivot_predicates which comprise each value therein pivot_with. pivot_predicates must be a 2-d list, wherein the inner-most dimension contains a list of four elements:

    [
        the predicate, what to compare for,
        the resultant if the predicate evaluates truthfully,
        the else-resultant if the predicate evaluates falsefully,
        the name of the pivot output column
    ]

    The latter two elements of the aforesaid pivot_predicates may be elided by the user, whereupon they'll be inferred and or replaced by temporary or default values.

    @param table_name: name of the desired table whereupon to pivot.
    @param pivot_on: column whereupon to pivot.
    @param pivot_with: dictionary of
                                    keys: columns appurtenant to pivot_on.
                                    values: pivot_predicates; hereinbefore              written predicate values of the             pivot table.
    @param pivot_output_columns: output columns, whereof are used for the                                     resultant pivot table.
    @param summarize_by: summarization operation, acted upon the pivots;                             typically a count(*).
    @param pivot_output_operation: functor that takes a list of pivot predicate                                 names and acts on the aggregate thereof:                                    e.g:
                                    pivot_output_operation = lambda x: f"({'+'.join(x)})",
                                    which spawns an summed aggregate of each pivoted predicate.

    @returns: base pivot template command, customized with the aforewritten and            script produced accoutrements.
    '''
    if not pivot_output_operation:
        pivot_output_operation = lambda x: ""

    pivot_maxes = ""
    cases = ""
    pivot_operations = ""
    pivot_columns = ""
    pivot_columns_as = ""

    n = 0
    for pivot, pivot_predicates in pivot_with.items():
        pivot_predicates = sanitize_pivot_predicates(pivot_predicates)
        # Each pivot predicate name is prefixed by the basal pivot name
        pivot_maxes = \
            ", ".join(
                [f"max(_{pivot}_{i[3]}) AS {pivot}_{i[3]}" for i in pivot_predicates])
        # Each case value is prefixed with both the basal pivot name, and an additional "_"
        cases += \
            ", ".join(
                [case_template_command(
                    i[0],
                    i[1],
                    f"{i[2]}" if i[2] != "" else f"answerColumn_{n}",
                    f"_{pivot}_{i[3]}",
                    n) for i in pivot_predicates])
        # Each aggregate pivot operation is named as op_"pivot".
        pivot_operations += ""\
            + "("\
            + pivot_output_operation(
                [f"max(_{pivot}_{i[3]})" for i in pivot_predicates])\
            + ")"\
            + f"AS op_{pivot}"
        # Unique pivot columns for each individual key "pivot".
        column_as = f"answerColumn_{n}, "
        pivot_columns += f"{pivot} AS {column_as}"
        pivot_columns_as += column_as
        n += 1

    pivot_output_columns_string = \
        ", ".join(pivot_output_columns) + ", " \
        + pivot_maxes + ", " \
        + pivot_operations

    summarize_by_string = ", ".join(summarize_by)

    return base_template_command(table_name,
                                 pivot_on,
                                 pivot_columns[:-2],
                                 pivot_columns_as[:-2],
                                 summarize_by_string,
                                 cases,
                                 pivot_output_columns_string)


def generate_pivot_command(question_column: str,
                           tag: str = None) -> str:
    '''
    Generates a pivot table based upon a question_column. Typically used with an additional "tag", as the likelyhood of a 2018 question existing in the  2016 mapping is rather low, but the likelyhood of its tag counterpart is high.

    @param question_column: question whereupon to pivot.
    @param tag: helper parameter to assist in finding the concomitant mapping               for question column.

    @returns: a begotten pivot table command.
    '''
    if (not tag):
        tag = tag_group_mapping[question_column]

    codes = code_value_mapping[tag]

    table_name = "data"
    pivot_on = "MasterSiteID"

    pivot_predicates = [[i[0],
                         "answerCount",
                         "0",
                         i[1]] for i in codes
                        ]

    pivot_with = {question_column: pivot_predicates}

    pivot_output_columns = ["MasterSiteID", "orgName"]

    summarize_by = ["count(*) AS answerCount"]
    pivot_operation = lambda x: f"({'+'.join(x)})"

    pivot_command = pivot_by(table_name,
                             pivot_on,
                             pivot_with,
                             pivot_output_columns,
                             summarize_by,
                             pivot_operation)
    return pivot_command


codebook = pd.read_csv("twc/mijn-NC18_codebook_main - Sheet1.csv")
priority_rows = \
    codebook[
        codebook["Priority"]
        .where(codebook["Priority"] == True)
        .notna()]

commands = []
priority_rows[["QuestionTag", "AnswerGroup"]]\
    .apply(
    lambda x: commands.append(generate_pivot_command(x[0], x[1])), 1)

for i in commands:
    print(i)
