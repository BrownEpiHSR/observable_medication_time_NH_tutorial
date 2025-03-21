/*
Overall Project: Drug Observable Nursing Home Time algorithm

	Description: To identify episodes of person-time in the Nursing Home population 
	in which Medicare Part D Fee-for-Service drug dispensings are observable. Nursing Home time is 
	defined using MDS data. Drug observable time is defined by enrollment in Parts A, B, and D FFS, 
	and outside of periods of hospitalization and post-acute care (SNF), both of which are covered
	by bundled payments under Part A and cannot be observed in Part D claims data.

Program: 4_tutorial_parallel_processing_macro_to_run

	Desciption: This code opens 10 distinct SAS sessions and assigns each of the 10 partitions
	of data to process sequentially using the code in Program 3 (3_tutorial_day_level_processing_code_to_iterate).

Programmer: Adam DAmico

Date: 19Dec2024

Version History:
*/


/* Configure debugging options and output settings:
   - Enable macro debugging: mprint, mtrace, mlogic, macrogen, symbolgen
   - Format output: linesize, pagesize, nocenter
   - Set variable naming convention: validvarname = upcase
   - Adjust message detail level: msglevel=I */
options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

/*Define macros*/
%let max_runs 	= 10; *Number of SAS windows that will be running at once - any more than 10 might be too much;
%let saspath 	= C:\SAS94\SASFoundation\9.4\sas.exe; *Path to SAS executable;
%let sas_code 	= P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\code\Publication_Version\3_tutorial_day_level_processing_code_to_iterate.sas; *The SAS program that will be run using the partitions of data;
%let logpath 	= P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\output\Publication_version\parallel_processing_logs; *Path to the log file;
%let run_date 	= &sysdate.; *Run date;
%let log_name 	= output_partition_log; *Log file name;
%let max_partitions = 10; *Number of data partitions each SAS window will run (Note: Does not need to be the same number as max_runs, but we just happen to be using the same number);


%macro pp;

	*Loop to launch &max_runs. parallel tasks;
	%do g = 1 %to &max_runs.;

		*Use the systask command to launch a new SAS process asynchronously;
		systask command " ""&saspath.""  ""&sas_code."" 
		-log ""&logpath.\&run_date._&log_name._&g..log""
		-nosplash -nologo -icon
		-bufno 1000 -bufsize 16k -threads -sgio
		-initstmt ""%nrquote(%)global iter; 
					%nrquote(%)let iter=&g.;
					%nrquote(%)let run_date=&run_date.;
					%nrquote(%)let max_partitions=&max_partitions.;
					%nrquote(%)let partition_mode=0;
					%nrquote(%)let max_runs=&max_runs.; "" "
			/*""&saspath.""  ""&sas_code."": Path to the SAS executable and code*/
			/*-log ""&logpath.\&run_date._&log_name._&g..log": Specify unique log file for each task*/
			/*-nosplash, -nologo, -icon: Suppress SAS splash screen, logo, and use icon for minimized window */
			/*-bufno 1000 -bufsize 16k -threads -sgio: Performance tuning options*/
			/*initstmt: Initial statements to set task-specific macro variables*/
					/*%nrquote(%)global iter;: Declare iter as a macro variable.
					/*%nrquote(%)let iter=&g.;: Assign the loop iteration number to 'iter' (e.g., 1, 2, 3)*/
					/*%nrquote(%)let run_date=&run_date.;: Pass the run date to the task*/
					/*%nrquote(%)let max_partitions=&max_partitions.;: Pass partition details (i.e., the number of partitions)*/
					/*%nrquote(%)let partition_mode=0;: Set the partition mode (e.g., off)*/
					/*%nrquote(%)let max_runs=&max_runs.;: Pass the total number of runs to the task*/

		taskname=task_&g. 	/*Assign a unique task name (e.g., task_1, task_2)*/
		status=rc_&g.; 		/*Store the return code (i.e., status code) of the task in a macro variable (e.g., rc_1, rc_2). A value of 0 indicates that the task completed successfully*/
	%end; 

	*Wait for all launched tasks to finish;
	waitfor _all_
		%do g = 1 %to &max_runs.;
			task_&g.
		%end;
	;

	*Log the return code for each task;
	%do g = 1 %to &max_runs.;
		%put The SAS return code for task run &g. is &&rc_&g..;
	%end;

%mend;

%pp;
	
/*END OF PROGRAM*/
