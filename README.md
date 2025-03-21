# Description
This repository contains data documentation and code for the analysis in the manuscript titled "Identifying observable medication time for US nursing home residents using Medicare claims: A tutorial".
## Repository Contents
- `data_documentation/` - Contains files describing the data sources, key source variables, output variables, synthetic data to demonstrate data structure.
- `code/` - The programs used apply the drug observable NH time algorithm.
- `LICENSE` - The license under which this repository is shared.
- `README.md` - This file, providing an overview of the repository.
## Data Documentation
The `data_documentation/` directory contains the following files:
- `Metadata_nhddi_tutorial.github.xlsx` - describes the data sources, key source variables, and output variables for the 4 datasets this algorithm produces. 
- `Synthetic_data_nhddi_tutorial_github.xlsx` - contains synthetic data to demonstrate the structure of the 4 datasets this algorithm produces.
## Code
The `code/` directory contains the following programs:
- `1_tutorial_constructing_nh_episodes_github.sas` - Constructs NH episodes from MDS data
- `2_tutorial_data_prep_to_remove_unobs_time_github.sas` - Preps NH episode data, as well as MedPAR SNF and hospitalization data, and MBSF data for day level processing.
- `3_tutorial_day_level_processing_code_to_iterate_github.sas` - Contains macro which processes NH episode data at the day level to remove unenrolled time, as well as time spent in SNF or hospital care.
- `4_tutorial_parallel_processing_macro_to_run_github.sas` - Implements a parallel processing procedure to run the code from program 3 in smaller partitions to reduce processing time and avoid disk space problems.
- `5_tutorial_post_processing_data_concatenation_github.sas` - Concatonates partitions and cleans up final datasets.

Programs were run in sequence.
Programs only contain code to run algorithm, and do not contain code used to populate tables included in the results of the manuscript.
