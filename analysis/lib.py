import polars as pl
from IPython.display import display, display_markdown


def get_dataset():
    dataset = pl.read_ipc("../output/dataset.arrow")
    dataset = dataset.drop("patient_id")
    return dataset


descriptions = {
    "ethnicity_6_category": """
## 5 Category ethnicity
**This text describes the variable**
### It is defined using the following ehrQL:
```
ethnicity_6_category = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(ethnicity_6_category_codelist)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code.to_category(ethnicity_6_category_codelist)
)
```
""",
    "ethnicity_16_category": """
## 16 Category ethnicity
**This text describes the variable**
### It is defined using the following ehrQL:
```
ethnicity_16_category = (
    clinical_events.where(
        clinical_events.snomedct_code.is_in(ethnicity_16_category_codelist)
    )
    .sort_by(clinical_events.date)
    .last_for_patient()
    .snomedct_code.to_category(ethnicity_16_category_codelist)
)
```
""",
    "ethnicity_sus": """
## Ethnicity from SUS
**This text describes the variable**

### It is defined using the following ehrQL:
```
ethnicity_sus = ethnicity_from_sus.code
```
""",
    "ethnicity_gp_and_sus_5_category": """
## 6 Category ethnicity combined with SUS
**This text describes the variable**
### It is defined using the following ehrQL (which includes some variables defined above):
```
dataset.ethnicity_gp_and_sus_5_category = case(
    when(
        (ethnicity_6_category == "1")
        | ((ethnicity_6_category.is_null()) & (ethnicity_sus.is_in(["A", "B", "C"])))
    ).then("White"),
    when(
        (ethnicity_6_category == "2")
        | (
            (ethnicity_6_category.is_null())
            & (ethnicity_sus.is_in(["D", "E", "F", "G"]))
        )
    ).then("Mixed"),
    when(
        (ethnicity_6_category == "3")
        | (
            (ethnicity_6_category.is_null())
            & (ethnicity_sus.is_in(["H", "J", "K", "L"]))
        )
    ).then("Asian or Asian British"),
    when(
        (ethnicity_6_category == "4")
        | ((ethnicity_6_category.is_null()) & (ethnicity_sus.is_in(["M", "N", "P"])))
    ).then("Black or Black British"),
    when(
        (ethnicity_6_category == "5")
        | ((ethnicity_6_category.is_null()) & (ethnicity_sus.is_in(["R", "S"])))
    ).then("Chinese or Other Ethnic Groups"),
    otherwise="Missing",
)
```
""",
    "ethnicity_gp_and_sus_16_category": """
## 6 Category ethnicity combined with SUS
**This text describes the variable**
### It is defined using the following ehrQL (which includes some variables defined above):
```
dataset.ethnicity_gp_and_sus_16_category = case(
    when(
        (ethnicity_16_category == "1")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["A"])))
    ).then("White - British"),
    when(
        (ethnicity_16_category == "2")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["B"])))
    ).then("White - Irish"),
    when(
        (ethnicity_16_category == "3")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["C"])))
    ).then("White - Any other White background"),
    when(
        (ethnicity_16_category == "4")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["D"])))
    ).then("Mixed - White and Black Caribbean"),
    when(
        (ethnicity_16_category == "5")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["E"])))
    ).then("Mixed - White and Black African"),
    when(
        (ethnicity_16_category == "6")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["F"])))
    ).then("Mixed - White and Asian"),
    when(
        (ethnicity_16_category == "7")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["G"])))
    ).then("Mixed - Any other mixed background"),
    when(
        (ethnicity_16_category == "8")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["H"])))
    ).then("Asian or Asian British - Indian"),
    when(
        (ethnicity_16_category == "9")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["J"])))
    ).then("Asian or Asian British - Pakistani"),
    when(
        (ethnicity_16_category == "10")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["K"])))
    ).then("Asian or Asian British - Bangladeshi"),
    when(
        (ethnicity_16_category == "11")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["L"])))
    ).then("Asian or Asian British - Any other Asian background"),
    when(
        (ethnicity_16_category == "12")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["M"])))
    ).then("Black or Black British - Caribbean"),
    when(
        (ethnicity_16_category == "13")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["N"])))
    ).then("Black or Black British - African"),
    when(
        (ethnicity_16_category == "14")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["P"])))
    ).then("Black or Black British - Any other Black background"),
    when(
        (ethnicity_16_category == "15")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["R"])))
    ).then("Other Ethnic Groups - Chinese"),
    when(
        (ethnicity_16_category == "16")
        | ((ethnicity_16_category.is_null()) & (ethnicity_sus.is_in(["S"])))
    ).then("Other Ethnic Groups - Any other ethnic group"),
    otherwise="Missing",
)
```
""",
}

pl.Config(fmt_str_lengths=60)


def count_by_category(df):
    for col in df.columns:
        display_markdown(descriptions[col], raw=True)
        count_table = df[col].value_counts().sort(col)
        with pl.Config(tbl_rows=len(count_table)):
            display(count_table)


def crosstab(df, index, column):
    df = df.with_columns(count=pl.lit(1))
    crosstab = df.pivot(
        index=index,
        columns=column,
        values="count",
        aggregate_function="sum",
    )
    display(crosstab)


def do_all_crosstabs(df):
    x_list = df.columns
    y_list = df.columns
    for x in x_list:
        for y in y_list:
            if x != y:
                display_markdown(f"## Comparing {x} with {y}", raw=True)
                crosstab(df, x, y)
        y_list.remove(x)
