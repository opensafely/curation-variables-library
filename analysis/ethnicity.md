# Defining ethnicity in OpenSAFELY
Ethnicity is most commonly defined using primary care data, but can also be defined using secondary care data. These can then be combined as needed. It might be advantageous to supplement the primary care definition with secondary care data because primary care alone tends to lead to 20-30% of people with a missing ethnicity. **However**, it is important to carefully consider the implications of this for your study. For example, the remaining people with a missing ethnicity are likely to be extremely unrepresentative of the population as a whole, in that they are much less likely to have frequently visited a GP or hospital.

## Codelists used

The ethnicity variable uses only [this codelist](https://www.opencodelists.org/codelist/opensafely/ethnicity-snomed-0removed/2e641f61/) for defining ethnicity in primary care. This was determined to be the best codelist to use in work published in [this paper](https://www.medrxiv.org/content/10.1101/2023.11.21.23298690v1). The codelist can be added to your study by adding this to your `codelists.txt` file:
```
opensafely/ethnicity-snomed-0removed/2e641f61
```
As described in the [codelist description](https://www.opencodelists.org/codelist/opensafely/ethnicity-snomed-0removed/2e641f61/), it cotains 2 levels of categorisation, one containing 5 categories, and another containing 16 categories.

Depending on what level of detail your study requires, you can use the 5 level categorisation by importing the codelist into to your dataset defintion like this:
```
ethnicity_6_category_codelist = codelist_from_csv(
    "codelists/opensafely-ethnicity-snomed-0removed.csv",
    column="snomedcode",
    category_column="Grouping_6",
)
```
...or the 16 category like this:
```
ethnicity_16_category_codelist = codelist_from_csv(
    "codelists/opensafely-ethnicity-snomed-0removed.csv",
    column="snomedcode",
    category_column="Grouping_16",
)
```

## List of ethnicity definitions
- [Primary care ethnicity 6 category](https://github.com/opensafely/curation-variables-library/blob/mvp-2/analysis/data/html/ethnicity_6_category.html)
- [Primary care ethnicity 16 category](https://github.com/opensafely/curation-variables-library/blob/mvp-2/analysis/data/html/ethnicity_16_category.html)
- [SUS ethnicity](https://github.com/opensafely/curation-variables-library/blob/mvp-2/analysis/data/html/ethnicity_sus.html)
- [Primary care ethnicity 6 category combined with SUS](https://github.com/opensafely/curation-variables-library/blob/mvp-2/analysis/data/html/ethnicity_gp_and_sus_5_category.html)
- [Primary care ethnicity 16 category combined with SUS](https://github.com/opensafely/curation-variables-library/blob/mvp-2/analysis/data/html/ethnicity_gp_and_sus_16_category.html)
