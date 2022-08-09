[![Codacy Badge](https://api.codacy.com/project/badge/Grade/339360ac7546410c820a71c3113c0990)](https://www.codacy.com/manual/FNLCR_2/ctdc-data-processing?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CBIIT/ctdc-data-processing&amp;utm_campaign=Badge_Grade)

# ctdc-data-processing
CodeBase to Store Data Processing Scripts for the Clinical Trial Data Commons

- ````blindIDMappingValidation.py````is a script to compare two input files, one raw MATCH case file (with PSN column) and one PSN to blind ID mapping file. The purpose of this script is to make sure all PSNs from case file has one and only one entry in the mapping file. To run this script, the user can user the commandbelow:

	````python3 blindIDMappingValidation.py psn_file blindID_file````
		
	````psn_file````: Raw MATCH case file
		
	````blindID_file````: PSN to blind ID mapping file