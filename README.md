CLOUDY DEDUPE
Strict & fuzzy combined deduplication tool for any contact record file

The cloudy dedupe tool helps deduplicate messy contact record files. This was specifically designed to help non-profits deduplicate donor contact files. Non-profits can have vastly different tracking setups for donors with unique field names and unique levels of importance for each field. This allows the user to deduplicate most files with any combination of field names and field level of importance

HOW IT WORKS
This tool deduplicates in two main phases. 
-	Find duplicates based on exact matches, 
-	Identify duplicates using a fuzzy algorithm to find non exact duplicates. 
The strict deduplication phase uses a simple pandas deduplication method to find exact string matches. 
The fuzzy phase first uses blocking, then isolates and fuzzy matches on each field. Scores for each field are weighted, with the final score passing a gate where a record needs two fields to score above a default value of 95 percent to be considered a potential match as well as one specific field scoring above a default 95 percent. 
After each phase matching records are grouped and assigned a master record by implementing the Disjoint Set Union data structure and algorithm. DSU creates and stores a collection of disjoint (non-overlapping) sets. This allows multiple duplicated records to tie to one parent record. 

INPUT
 The input is a yaml file which includes the field names to be used to find duplicates, as well as some other customizable settings. The output is three files. The first is a master file which contains every original record, T/F fields to indicate what field a record was duplicated on, the root id for that record, as well as the fuzzy score for each record. The second file is a cleaned file and only contains the master record for each duplicate found, as well as only the original fields. The third and final file is a file with fuzzy matches that fall within a configurable score range (default: 75-90%) 




LIMITATIONS
The weight for each field is manually set in the yaml file, this can be difficult to set an accurate weight given how differently each field contributes to determining a duplicate.
The weight specified will be equally divided amongst that contact type’s columns, so the total column’s weights add to 1.0. Only the name fields currently have the option to specify different weights between each specific name field. This will be updated in v2.
If a record has a null value for a field, that field is scored as a 0 percent match and potentially lowers the chance for correctly identifying a duplicate. In future versions the weights will be auto adjusted to account for this. 

Sometimes if contact data has contact fields but no names “Default Contact” is used as the name. The tool does not currently account for this case and will potentially miss duplicates. This will be fixed in v2

Another potential limitation is first names can be very similar for two separate people, but also very different for the same person. For example, 
ID	FIRST	LAST	EMAIL
1	Willaim	Smith	wsmith@gmail.com
2	Bill	Smith	wsmith@gmail.com
3	Julian	Jones	jjones@gmail.com
4	Julius	Jones	jjones@gmail.com

These four records each score about 67 percent match with the WRatio and JaroWinkler algorithms. However, we know Bill is a classic nickname for William which makes us believe records 1 & 2 are duplicated records. But we can see Julian and Julius are two different names and are therefore most likely not duplicates. This is mitigated with the use of the python library nicknames. At the start of the fuzzy phase a nickname cache is created, when the first name field is compared using fuzzy, each name is checked in the nickname cache then an intersection of the set of nicknames related to both names is created. If there is a name in the intersecting set the name field on the two records is treated as a match, thus helping boost the score of a potential duplicate. 
