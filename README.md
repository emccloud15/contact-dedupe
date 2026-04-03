## CONTACT DEDUPE
Deduplication tool that combines exact & fuzzy matching for any contact record file

The contact dedupe tool helps deduplicate messy contact record files. This was specifically designed to help non-profits deduplicate donor contact files. Non-profits can have vastly different tracking setups for donors with unique field names and unique levels of importance for each field. This allows the user to deduplicate most contact files with any combination of field names and field level of importance

## Installation
```bash
git clone https://github.com/emccloud15/contact-dedupe
cd contact-dedupe
python -m .venv/bin/activate
pip install -r requirements.txt
```
## Configuration
A specific yaml configuration is required to run the tool. The config file is used to specify field names, weights, and blocking as well as some other customizable settings.  

[Example configuration](client_template.yaml)

## Usage
```bash
python run.py --yaml path/to/config.yaml --input path/to/duplicated_contacts.csv --output path/to/output/
```

## Output
The output is three files. The first is a master file which contains every original record with T/F fields to indicate what field a record is duplicated on, an assigned root id for that record, as well as the fuzzy score for each record. The second file is a deduplicated version of the original contact file. The last file contains fuzzy matches that were possible duplicates, but need manual checking to confirm. The records that need to be checked returned fuzzy scores within a configurable score range (default between: 75-90%).

## HOW IT WORKS
This tool deduplicates in two main phases. 
-	Find duplicates based on exact matches
-	Identify duplicates using a fuzzy algorithm to find non exact duplicates. 

The strict deduplication phase uses a simple pandas deduplication method to find exact string matches. 
The fuzzy phase uses blocking to dramatically reduce the number of record to record comparisons, then isolates and fuzzy matches on each field. Scores for each field are weighted, with the final score meeting two conditions. First, two fields need to score above a configurable threshold (default: 95%) to be considered a potential match, as well as one specific field (determined in the yaml file) scoring above a configurable threshold (default: 95%). 
After each phase matching records are grouped and assigned a master record by implementing the Disjoint Set Union data structure and algorithm. DSU creates and stores a collection of disjoint (non-overlapping) sets. This allows multiple duplicated records to tie to one parent record. 


## LIMITATIONS
The weight for each field is manually set in the yaml file. It can be difficult to set an accurate weight given how differently each field contributes to determining a duplicate.

The weight specified will be equally divided amongst that contact type’s columns, so the total column’s weights add to 1.0. Only the name fields currently have the option to specify different weights between each specific name field. This will be updated in v2.

If a record has a null value for a field, that field is scored as a 0 percent match and potentially lowers the chance for correctly identifying a duplicate. In future versions the weights will be auto adjusted to account for this. 

Sometimes if contact data has contact fields but no names “Default Contact” is used as the name. The tool does not currently account for this case and will potentially miss duplicates. This will be fixed in v2

Another potential limitation is first names can be very similar for two separate people, but also very different for the same person. For example, 
| ID | FIRST   | LAST  | EMAIL           |
|----|---------|-------|-----------------|
| 1  | William | Smith | wsmith@gmail.com |
| 2  | Bill    | Smith | wsmith@gmail.com |
| 3  | Julian  | Jones | jjones@gmail.com |
| 4  | Julius  | Jones | jjones@gmail.com |

Record ID 1 & 2 return approximately as a 67 percent match as well as records 3 & 4 with the WRatio and JaroWinkler algorithms. However, we know Bill is a classic nickname for William which make us believe records 1 & 2 are duplicated records. But we can see Julian and Julius are two unique unrelated names and are therefore most likely not duplicates. This is mitigated with the use of the python library [nicknames](https://pypi.org/project/nicknames/). At the start of the fuzzy matching phase a nickname cache is created. When the first name field is compared, each name is first checked in the nickname cache and a set of related nicknames is assigned to both names being compared. If the intersecting set between the related nickname sets is not null then the name field on the two records is treated as a match, thus helping boost the score of a potential duplicate. 
