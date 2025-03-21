/*
Overall Project: Drug Observable Nursing Home (NH) Time algorithm

	Description: To identify episodes of person-time in the Nursing Home population 
	in which Medicare Part D Fee-for-Service drug dispensings are observable. Nursing Home time is 
	defined using MDS data. Drug observable time is defined by enrollment in Parts A, B, and D FFS, 
	and outside of periods of hospitalization and post-acute care (SNF), both of which are covered
	by bundled payments under Part A and cannot be observed in Part D claims data.

Program: 3_tutorial_day_level_processing_code_to_iterate

	Description: This code is invoked in Program 4 (4_tutorial_parallel_processing_macro_to_run). The first portion of the program 
	identifies which 10 partitions of the Nursing Home episode data (out of 100) to process. Partitions
	are processed sequentially. The second portion expands the selected partitions to the day level, along with enrollment,
	SNF and hospital data, merges them all together, and generates episodes of drug observable Nursing Home 
	time for the selected partition.

Programmer: Adam DAmico

Date: 19Dec2024

Version History:
*/

/*Configure debugging options, define directories, and assign library names*/
options mprint mlogic;

%let version=20241111; *date of data pull from MDS;
libname fresh "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\fresh_pull_&version.";
libname mdstime "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\mdstime"; 
libname prepmedp "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\prepmedp"; 
libname enroll "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\enroll";  
libname partin  "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\partin"; 
libname partout "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\partout"; 
libname final "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\final";


/*STEP 1: DETERMINE WHICH PARTITIONS OF DATA TO PROCESS*/

%macro getrawdata(x);
*x refers to the prefix designating the datasets/variables as admission-anchored (adm) or entry-anchored (ent);

	*Note: &iter. tracks the iteration number and is passed in Program 4 (4_tutorial_parallel_processing_macro_to_run);

	%if &iter. = 1 %then %do; 
		%let start_partition = 1;
		%let end_partition = 10;
	%end;

	%if &iter. = 2 %then %do;
		%let start_partition = 11;
		%let end_partition = 20;
	%end;

	%if &iter. = 3 %then %do;
		%let start_partition = 21;
		%let end_partition = 30;
	%end;

	%if &iter. = 4 %then %do;
		%let start_partition = 31;
		%let end_partition = 40;
	%end;

	%if &iter. = 5 %then %do;
		%let start_partition = 41;
		%let end_partition = 50;
	%end;

	%if &iter. = 6 %then %do;
		%let start_partition = 51;
		%let end_partition = 60;
	%end;

	%if &iter. = 7 %then %do;
		%let start_partition = 61;
		%let end_partition = 70;
	%end;

	%if &iter. = 8 %then %do;
		%let start_partition = 71;
		%let end_partition = 80;
	%end;

	%if &iter. = 9 %then %do;
		%let start_partition = 81;
		%let end_partition = 90;
	%end;

	%if &iter. = 10 %then %do;
		%let start_partition = 91;
		%let end_partition = 100;
	%end;


*******************************************************************;
/*CODE BELOW IS PARSED THROUGH USING PARALLEL PROCESSING*/

%do p = &start_partition. %to &end_partition.;


/*STEP 2: BRING DATA TO DAY LEVEL*/

%macro dayprocessing(x);

*Copy partition dataset;
data &x.NHs_this_part;
	set partin.&x.NH_partition_&p.;
run;

*Reduce SNF data to beneficiary IDs in the partitioned NH episode dataset;
proc sql;
	create table snf_this_part as
		select bene_id_18900, snf_clmid, snf_entry_date, snf_discharge_date
		from prepmedp.snf_restrict3
		where bene_id_18900 in (select bene_id_18900 from &x.NHs_this_part);
quit;

*Reduce hospital data to beneficiaries in the partitioned NH episode dataset;
proc sql;
	create table hosp_this_part as
		select bene_id_18900, hosp_clmid, hosp_entry_date, hosp_discharge_date
		from prepmedp.hosp_restrict3
		where bene_id_18900 in (select bene_id_18900 from &x.NHs_this_part);
quit;

*Reduce enrollment episodes to relevant NH episodes in the partitioned dataset;
	*Note: We only need to do this for NH episodes with partial enrollment;
proc sql;
	create table enroll_this_part as
		select bene_id_18900, &x.NH_id, enroll_id, enroll_start, enroll_end
		from mdstime.&x.NH_partial_enrollment /*Partial enrollment dataset*/
		where &x.NH_id in (select &x.NH_id from &x.NHs_this_part);
quit;

*Bring the 4 data sets (NH episodes, SNF, hospital, and enrollment) to the day level;
	*Process NH episode dataset;
	data _&x.NHdaily ;
		set &x.NHs_this_part ;

	    &x.NH_day_num = 1; 				/*Initialize a NH episode day number*/

	    do daily = &x.NH_entry_date to &x.NH_discharge_date ;
	        output ; 					/*Output one record for each day in the NH*/
	        &x.NH_day_num + 1; 			/*Increment the day number*/
	    end;

		format daily mmddyy10.;
	run; 

	*Process SNF dataset;
	data _snfdaily ;
		set snf_this_part ;

		do daily = snf_entry_date to snf_discharge_date ;
			output ; /*Output one record for each day in the SNF*/
		end;

		format daily mmddyy10. ;
	run; 

	*Process hospital dataset;
	data _hospdaily ;
		set hosp_this_part ;

		do daily = hosp_entry_date to hosp_discharge_date ;
			output ; /*Output one record for each day in the hospital*/
		end;

		format daily mmddyy10. ;
	run; 

	*Process enrollment dataset;
	data _enrolldaily ;
		set enroll_this_part ;

		do daily = enroll_start to enroll_end ;
			output ; /*Output one record for each day in the enrollment episode*/
		end;

		format daily mmddyy10. ;
	run; 

*Deduplicate daily records in the 4 day-level datasets (NH episodes, SNF, hospital, and enrollment);
	*Note:  Duplicates arise from overlapping stays (SNF/Hospital data) or because a SNF/Hospital/Enrollment episode 
			is associated with multiple NH episodes for a single person.
			Duplicates should already be dealt with at the &x.NH episode level, but are included here for completeness;

	*NH episodes;
	proc sort data = _&x.NHdaily nodupkey out = _&x.NHdaily_dedupe;
		by bene_id_18900 daily;
	run; 

	*SNF;
	proc sort data = _snfdaily nodupkey out = _snfdaily_dedupe;
		by bene_id_18900 daily;
	run; 

	*Hospital;
	proc sort data = _hospdaily nodupkey out = _hospdaily_dedupe;
		by bene_id_18900 daily;
	run; 

	*Enrollment;
	proc sort data = _enrolldaily nodupkey out = _enrolldaily_dedupe;
		by bene_id_18900 daily;
	run; 


/*STEP 3: MERGE DAY-LEVEL DATA FROM THE 4 DATASETS (NH EPISODES, SNF, HOSPITAL, ENROLLMENT) BY BENEFICIARY ID AND DATE */

proc sql;
	create table _daily_alldata as
		SELECT 
		a.bene_id_18900, 
		a.&x.NH_id, 
		a.&x.NH_entry_date, 
		a.&x.NH_discharge_date, 
		a.&x.NH_discharge_type, 
		a.&x.NH_return_anticipated, 
		a.&x.NH_no_discharge, 
		a.&x.NH_admiss_id, 
		a.&x.NH_admiss_assess_date, 
		a.hkdod, 
		a.mbsf_prs_match,
		a.partition_num, 
		a.&x.NH_enrollcat, 
		a.&x.NH_day_num,
		a.daily, /*Specific date*/
		COALESCE(a.enroll_id, d.enroll_id) 							as enroll_id,     /*Assign enroll_id: use a.enroll_id if available; otherwise, use d.enroll_id*/
		COALESCE(a.enroll_start, d.enroll_start) format=mmddyy10. 	as enroll_start,  /*Assign enroll_start: use a.enroll_start if available; otherwise, use d.enroll_start*/
		COALESCE(a.enroll_end, d.enroll_end) format=mmddyy10. 		as enroll_end,    /*Assign enroll_end: use a.enroll_end if available; otherwise, use d.enroll_end*/
	    CASE WHEN b.bene_id_18900 IS NOT NULL AND b.daily IS NOT NULL THEN 1 ELSE 0 END AS snf_day,   	/*1 if day spent in SNF, 0 otherwise*/
	    CASE WHEN c.bene_id_18900 IS NOT NULL AND c.daily IS NOT NULL THEN 1 ELSE 0 END AS hosp_day,  	/*1 if day spent in hospital, 0 otherwise*/
	    CASE WHEN d.bene_id_18900 IS NOT NULL AND d.daily IS NOT NULL THEN 1 ELSE 0 END AS enroll_day 	/*1 if day is spent during enrollment episode, 0 otherwise*/
	FROM  _&x.NHdaily_dedupe  as a 																	    /*NH episode day-level dataset*/
		left JOIN _snfdaily_dedupe as b 	ON  a.bene_id_18900 = b.bene_id_18900 AND a.daily = b.daily /*SNF day-level dataset*/
		left JOIN _hospdaily_dedupe as c 	ON  a.bene_id_18900 = c.bene_id_18900 AND a.daily = c.daily /*Hospital day-level dataset*/
		left JOIN _enrolldaily_dedupe as d 	ON  a.bene_id_18900 = d.bene_id_18900 AND a.daily = d.daily /*Enrollment period day-level dataset*/
	order by bene_id_18900, &x.NH_id, daily;
quit; 

*Clean merged data for NH episodes with full or no enrollment time (i.e., cases that did not require day-level processing);
data _daily_alldata2; set _daily_alldata;
	if &x.NH_enrollcat=0 then do; 	/*No enrollment during the NH episode*/
		enroll_day		=0; 		/*Enrollment criteria not met for the given day*/
		enroll_id		=.;
		enroll_start	=.;
		enroll_end		=.; 
		end;
	else if &x.NH_enrollcat=1 then do; 	/*Full enrollment during NH episode*/
		enroll_day		=1; 			/*Enrollment criteria met for the given day*/
		end;
run;


/*STEP 4: LIMIT TO DRUG OBSERVABLE DAYS AND ROLL DATA UP TO THE EPISODE LEVEL*/

*Clean up work directory to avoid space issues;
proc datasets library=work nolist;
    save _daily_alldata2;
	run;
quit;

*Limit to drug observable days: days where beneficiary is enrolled, not in SNF, and not in the hospital;
data _daily_drugobs_only;
	set _daily_alldata2;
	where enroll_day=1 & hosp_day=0 & snf_day=0; /*Enrollment criteria met, no SNF or hospital stay*/
run;

*Clean up work directory to avoid space issues;
proc datasets library=work nolist;
    save _daily_drugobs_only;
	run;
quit;

*Sort by beneficiary ID, NH episode ID, and date;
proc sort data = _daily_drugobs_only;
	by bene_id_18900 &x.NH_id daily; 
run;

*Create drug observable episode start and end dates that will be accurate on the last day 
 of each drug observable NH episode;
data _rollupA;
	set _daily_drugobs_only ;
	by bene_id_18900 &x.NH_id;

	retain &x.DONH_start &x.DONH_end;

	*For the first record of each NH episode, set the drug observable start and end dates to the record's date;
	if first.&x.NH_id then do;
		&x.DONH_start = daily;
		&x.DONH_end = daily ;
	end;

	*For subsequent records, update drug observable dates based on the record's date;
	if first.&x.NH_id ne 1 then do; 
		*If the record's date is 1 day after the previous record's drug observable end date, extend the drug observable end date to this record's date;
		if daily = &x.DONH_end + 1 then 
			&x.DONH_end = daily ;
		*Otherwise, start a new drug observable NH episode using the record's date;
		else do ;
			&x.DONH_start = daily;
			&x.DONH_end = daily ;
		end;
	end;

	format &x.DONH_start &x.DONH_end mmddyy10. ;
run;

*Sort by beneficiary ID, NH episode ID, drug observable NH episode start date, and drug observable NH episode end date;
proc sort data = _rollupa;
	by bene_id_18900 &x.NH_id &x.DONH_start &x.DONH_end; 
run; 

*Take the last day of each drug observable NH episode;
data partout.&x.DONH_eps_&p. ;
	set _rollupa ;
	by bene_id_18900 &x.NH_id &x.DONH_start ;
	
	if last.&x.DONH_start;
	
	drop daily &x.NH_day_num enroll_day snf_day hosp_day;
run; 

*Clear the work directory before the next iteration;
proc datasets lib=work nolist kill;
	run;
quit;

%mend dayprocessing;

*Run day-level processing on this partition for both
admission-anchored and entry-anchored NH episodes;
%dayprocessing(x=adm);
%dayprocessing(x=ent);

%end;

%mend getrawdata;

*Invoke the macro;
%getrawdata;

/*PARALLEL PROCESSING END*/














