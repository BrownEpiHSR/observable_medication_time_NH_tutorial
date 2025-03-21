/*
Overall Project: Drug Observable Nursing Home Time algorithm

	Description: To identify episodes of person-time in the Nursing Home (NH) population 
	in which Medicare Part D Fee-for-Service drug dispensings are observable. Nursing Home time is 
	defined using MDS data. Drug observable time is defined by enrollment in Parts A, B, and D FFS, 
	and outside of periods of hospitalization and post-acute care (SNF), both of which are covered
	by bundled payments under Part A and cannot be observed in Part D claims data.

Program: 1_tutorial_constructing_NH_episodes

	Description: This code constructs Nursing Home episodes from MDS data. This is done by first identifying matched 
	pairs of entry and discharge tracking records. Instances of overlapping time within a person are collapsed 
	to form the basis of the entry-anchored Nursing Home episodes (entNH). Additional processing is done 
	to generate admission-anchored Nursing Home episodes (admNH). Both entry-anchored and admission-anchored 
	episodes have distinct use cases and undergo the same day-level processing to remove drug-unobservable time 
	in subsequent programs.

Programmer: Adam DAmico

Date: 19Dec2024

Version History:

*/

/*Set location to save log file*/
proc printto log="P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\output\Publication_version\Program_1_log_file_&sysdate..log" new;
run;

/*Define directories and assign library names*/
%let version=20241111; *date of data pull from MDS;
libname fresh "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\fresh_pull_&version.";
libname mdstime "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\mdstime"; 
libname prepmedp "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\prepmedp"; 
libname enroll "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\enroll";  
libname partin  "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\partin"; 
libname partout "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\partout"; 
libname final "P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\data\version_date_&version.\final";

/*Assign study start and end dates*/
%let startofstudy = '01jan2013'd;
%let endofstudy = '31dec2020'd;

/*STEP 1: LIMIT AND CATEGORIZE MDS RECORDS*/

	*****************************************************************************************************
	Note:
	-Researchers often pull MDS data by target date.The Target Date reflects the time frame in which the 
	 an assessment or record was to be completed. The exact date varies by the record time. 
		• For an entry record (A0310F = [01]), the target date is equal to the entry date (A1600).
		• For a discharge record (A0310F = [10, 11]) or death-in-facility record (A0310F = [12]),
			the target date is equal to the discharge date (A2000).
		• For all other records, the target date is equal to the assessment reference date (A2300).

	-We needed to regenerate the target date variable because the one in our dataset (m3_trgt_dt) is corrupted.

	-The admission date (M3A1900) is missing in many entry and discharge records, making it less than ideal
	for identifying admission-anchored Nursing Home time.
	*****************************************************************************************************;

*Create a full copy of MDS data, so you can refer to it without the data changing;
data fresh.all_mds3_project_copy; set mds.all_mds3;
run;

*Limit the MDS dataset to variables of interest and entry dates that fall within the study period;
data fresh.mds_limited;
    set fresh.all_mds3_project_copy (
		keep=DMRECID bene_id_18900 M3A0100B M3A0310A M3A0310F M3A1600 M3A1700 M3A1800 M3A1900 M3A2000 M3A2100 M3A2300 
	    where=(&startofstudy. <= M3A1600 <= &endofstudy.)
		);
run; 

*Further limit the MDS dataset by target dates of interest;
data  fresh.study_recs;
	set fresh.mds_limited; 

	*If the Entry/Discharge Reporting code is 10 ("Discharge assessment - return not anticipated"), 
											  11 ("Discharge assessment - return anticipated"), or
	                                          12 ("Death in facility record tracking record"),
	 set the target date to the discharge date;
	if M3A0310F in (10, 11, 12) then  target_date = M3A2000;

	*Otherwise, if the Entry/Discharge Reporting code is 1 ("Entry tracking record"),
	 set the target date to the entry date;
	else if M3A0310F in (1) then target_date = M3A1600;

	*Otherwise, set the target date to the assessment reference date (i.e., the end date of the observation period of the assessment);
	else target_date = M3A2300;

	*Limit the dataset to target dates that fall within the study period;
	if &startofstudy. <= target_date <=&endofstudy.;

	drop target_date;
run; ;

*Split the MDS dataset by assessment type (i.e., discharge, entry, and admission);
data discharge_recs entry_recs admiss_recs;
	set fresh.study_recs;  

	*Discharge: if the Entry/Discharge Reporting code is: 
		10 ("Discharge assessment - return not anticipated"), 
		11 ("Discharge assessment - return anticipated"), or
	    12 ("Death in facility record tracking record"), output record to the discharge records dataset;
	if M3A0310F in (10, 11, 12) then output discharge_recs; 

	*Entry: otherwise, if the Entry/Discharge Reporting code is: 
		1 ("Entry tracking record"), then output the record to the entry records dataset; 
		else if M3A0310F =1 then output entry_recs;

	*Admission: if the record is an admission assessment, output to the admission records dataset;
	if M3A0310A =1 then output admiss_recs;

run;


/*STEP 2: CLEAN ENTRY RECORDS*/

*Clean up variables in the entry records dataset;
data entry_recs2;
    set entry_recs(rename=(
        M3A1600=entry_date 
        M3A0100B=entry_ccn
    ));
    keep bene_id_18900 entry_date entry_ccn  ;
run;

*Deduplicate: Remove records with the same beneficiary ID, entry date, and CMS certification number (CCN);
	proc sort data = entry_recs2 nodupkey;
		by bene_id_18900 entry_date entry_ccn; 
	run; 

*Remove record if missing CCN;
data entry_recs3; set entry_recs2;
	if entry_ccn='' then delete;
run; 


/*STEP 3: CLEAN DISCHARGE RECORDS*/

*Clean up variables in the discharge records dataset;
proc format;
    value return_anticipatedf
        0 = "0. No return anticipated (or death)"
        1 = "1. Return anticipated"
        2 = "2. No discharge record";
run;

data discharge_recs2;
	set discharge_recs(rename=(
		M3A2000=discharge_date     
		M3A0310F=discharge_type
		M3A0100B=discharge_ccn
		M3A1600=discharge_entry_date));

	*Create return_anticipated variable;

		*If the Entry/Discharge Reporting code is 
		 -10 ("Discharge assessment - return not anticipated") OR
		 -12 ("Death in facility record tracking record"),
		 the resident is not anticipated to return: return_anticipated = 0;
		if discharge_type in (10, 12) then return_anticipated = 0; 

		*Otherwise, the resident is anticipated to return: return_anticipated = 1;
		else return_anticipated = 1;

		*Delete record if the discharge date is prior to the entry date;
		if discharge_date < discharge_entry_date then delete;

	keep bene_id_18900 discharge_entry_date discharge_ccn discharge_date discharge_type return_anticipated; 
	format return_anticipated return_anticipatedf.
run; 

*Deduplicate: if a resident has multiple records with the same discharge date, prioritize the record where return is not anticipated;
	*Sort by beneficiary ID, entry date from the discharge dataset, CCN from the discharge dataset, discharge date, and return anticipated;
	proc sort data = discharge_recs2;
		by bene_id_18900 discharge_entry_date discharge_ccn discharge_date return_anticipated; 
	run; 
	*Keep only the first record for each unique combination of discharge date, prioritizing the record where return is not anticipated;
	data discharge_recs3; set discharge_recs2;
		by bene_id_18900 discharge_entry_date discharge_ccn discharge_date return_anticipated;
		if first.discharge_date; 
	run; 

*Deduplicate: take the earliest discharge within each person-entry-CCN combination;
	*Sort by beneficiary ID, entry date from the discharge dataset, CCN from the discharge dataset, 
	discharge date, and return anticipated;
	proc sort data = discharge_recs3;
		by bene_id_18900 discharge_entry_date discharge_ccn discharge_date return_anticipated;
	run;

	*Separate the earliest discharge record and all others into distinct datasets;
	data mdstime.first_discharges mdstime.weird_discharges; set discharge_recs3;
		by bene_id_18900 discharge_entry_date discharge_ccn discharge_date return_anticipated;

		if      first.discharge_ccn = 0 then output mdstime.weird_discharges; /*If this is not the earliest discharge record for the person-entry-CCN, output to weird_discharges dataset*/
		else if first.discharge_ccn = 1 then output mdstime.first_discharges; /*If this is the earliest discharge record for the person-entry-CCN, output to first_discharges dataset*/
	run; 


/*STEP 4: CLEAN ADMISSION ASSESSMENT RECORDS*/

*Clean up variables in the admission assessment records dataset;
data mdstime.admissions; set admiss_recs (rename=(
            M3A1600=admiss_entry_date
            M3A0100B=admiss_ccn
            M3A2300=admiss_assess_date
			DMRECID=admiss_id
			));
	*Delete record if the admission assessment date is before the admission entry date;
	if admiss_assess_date<admiss_entry_date then delete;

	keep bene_id_18900 admiss_id admiss_entry_date admiss_ccn admiss_assess_date;
run;

*Deduplicate: remove absolute duplicate records (using 4 variables - beneficiary ID, entry date, CCN, and admission assessment date);
proc sort data = mdstime.admissions nodupkey out = mdstime.admissions2;
	by bene_id_18900 admiss_entry_date admiss_ccn admiss_assess_date; 
run; 

*Deduplicate: if records have the same beneficiary ID, entry date, and CCN, keep the earliest admission assessment date;
data mdstime.admissions3; set mdstime.admissions2;
	by bene_id_18900 admiss_entry_date admiss_ccn admiss_assess_date; 
	if first.admiss_ccn;
run; 


/*STEP 5: CONSTRUCT ENTRY-DISCHARGE (ED) PAIRS*/

*Join the earliest discharge record to its corresponding entry record by beneficiary ID and entry date, but not CCN
 Note: Some discharge records do not have a matching entry record. These are relatively rare and are disregarded;
proc sql ;
	create table mdstime.entry_to_dis as 
		select 
			a.*, 
			b.discharge_ccn, 
			b.discharge_date, 
			b.discharge_type,
			b.return_anticipated
		from entry_recs3 as a 
		left join mdstime.first_discharges as b
		on a.bene_id_18900 = b.bene_id_18900 and a.entry_date = b.discharge_entry_date; /*discharge_entry_date is the entry date from the discharge dataset*/
quit; 


*Categorize ED pairs into datasets based on CCN status: matching CCN, missing discharge date, missing discharge CCN, or non-matching CCN
 Note: Those missing CCN in the entry record have already been excluded;
data mdstime.dis_ccn_match mdstime.entry_to_nodis mdstime.dis_ccn_missing mdstime.dis_ccn_different temp_all_else; 
set mdstime.entry_to_dis; 
	if      entry_ccn =  discharge_ccn                       	 then output mdstime.dis_ccn_match;     *Entry and discharge CCN match;
	else if discharge_date = .								  	 then output mdstime.entry_to_nodis;    *No discharge date;
	else if (entry_ccn ne discharge_ccn) & (discharge_ccn =  "") then output mdstime.dis_ccn_missing;   *Missing discharge CCN;
	else if (entry_ccn ne discharge_ccn) & (discharge_ccn ne "") then output mdstime.dis_ccn_different; *Entry and discharge CCN differ;
	else output temp_all_else;                                                                          *All other cases;
run;

*Concatenate ED pairs with matching CCN and entries that do not have a discharge record. 
 For entries without a discharge record, imput the discharge date as the study end date;
data mdstime.ED_pairs_a; set mdstime.dis_ccn_match mdstime.entry_to_nodis; 
	no_discharge = 0;

	*If there is no discharge date, mark the entry as not discharged and set the discharge date to the study end date;
	if discharge_date = . then do;
		no_discharge = 1;
		discharge_date = &endofstudy.; 
		return_anticipated=2; /*Set to 2 instead of missing to ensure it sorts last*/
	end;
run; 

*Join admission information onto ED pairs by beneficiary ID, entry date, and CCN;
proc sql;
	create table mdstime.ED_pairs_b as
		select 
			a.*, 
			b.admiss_id, 
			b.admiss_entry_date, 
			b.admiss_assess_date
		from mdstime.ED_pairs_a  as a
		left join mdstime.admissions3 as b
		on 	a.bene_id_18900=b.bene_id_18900 and 
			a.entry_date = b.admiss_entry_date and 
			a.entry_ccn=b.admiss_ccn;
quit; 


/*STEP 6: IDENTIFY ENTRY-DISCHARGE (ED) PAIRS THAT OVERLAP AND BELONG TO BROADER ENTRY-ANCHORED NH EPISODES*/

	**************************************************************************************
	Description:
	Entry-Anchored NH episodes begin at an entry record and end with the first 
	discharge of any kind.
	Discharge records that occur within spans of continuous NH residence are ignored.
	**************************************************************************************;

*Sort by beneficiary ID, entry date, and discharge date;
proc sort data = mdstime.ED_pairs_b; 
	by bene_id_18900 entry_date discharge_date;
run;

*Identify overlapping ED pairs and assign them a common entry-anchored NH episode ID (entNH_id)
 Note: Overlapping ED pairs can have different CCNs;
data mdstime.ED_pairs_c; set mdstime.ED_pairs_b;
	by bene_id_18900 entry_date discharge_date;

	*Increment the entry-anchored NH episode ID (entNH_id) for the first record of a beneficiary OR
	when the current entry date is after the previous record's discharge date;
	if first.bene_id_18900=1 
		or (first.bene_id_18900 = 0 and entry_date > lag(discharge_date))
	then entNH_id + 1;
run;

*Identify the start and end dates (i.e., entNH_entry_date and entNH_discharge_date) of each entry-anchored NH episode and apply these dates to all rows with that entNH_id;
proc sql;
	create table mdstime.ED_pairs_d as
		select 
			*, 
			min(entry_date) 	as entNH_entry_date     format=mmddyy10., /*The earliest entry date for a given entNH_id is the start date of the entry-anchored NH episode*/
			max(discharge_date) as entNH_discharge_date format=mmddyy10.  /*The latest discharge date for a given entNH_id is the end date of the entry-anchored NH episode*/
		from mdstime.ED_pairs_c
		group by entNH_id
		order by bene_id_18900, entry_date, discharge_date;
quit; 


/* STEP 7: IDENTIFY PEOPLE FOR WHOM ENTRY-ANCHORED NH EPISODES STILL OVERLAP AND REMOVE THEM */

	**************************************************************************************
	Note: 
	While we could plan to collapse these records, we have decided to remove the residents 
	entirely due to concerns about the integrity of the data;
	**************************************************************************************;

*Identify additional overlaps in residents' entry-anchored NH episodes;
	*Sort by beneficiary ID, entry date, and discharge date;
	proc sort data=mdstime.ED_pairs_d; 
		by bene_id_18900 entry_date discharge_date; 
	run;

	*Create entNH_id2 (entry-anchored NH episode ID 2) to identify additional overlaps;
	data mdstime.ED_pairs_e; set mdstime.ED_pairs_d;
		by bene_id_18900 entry_date discharge_date;

		*Increment the entry-anchored NH episode ID 2 (entNH_id2) for the first record of a beneficiary OR
		when the current entry date is after the previous record's discharge date;
		if first.bene_id_18900=1 
			or (first.bene_id_18900 = 0 and entNH_entry_date > lag(entNH_discharge_date))
		then entNH_id2 + 1;

		*Create variables to save the previous record's entNH_id and entNH_id2;
		lag_entNH_id = lag(entNH_id); 
		lag_entNH_id2 = lag(entNH_id2);
	run;

	*Sort updated dataset by beneficiary ID, entry date, and discharge date;
	proc sort data=mdstime.ED_pairs_e; 
		by bene_id_18900 entry_date discharge_date; 
	run;

	*Identify people with additional entry-anchored NH episode overlaps;
	proc sql;
		create table mdstime.double_collapse_ids as
			select distinct bene_id_18900
			from mdstime.ED_pairs_e
			where (entNH_id ne lag_entNH_id) & (entNH_id2 = lag_entNH_id2); /*Select beneficiaries with records where the entry-anchored NH episode ID differs from the previous record, 
																			  but the entry-anchored NH episode ID 2 matches the previous record*/
	quit; 

	*Remove people associated with additional entry-anchored NH episode overlaps from the cohort;
	proc sql;
		create table mdstime.ED_pairs_f as 
			select *
			from mdstime.ED_pairs_e
			where bene_id_18900 not in (select bene_id_18900 from mdstime.double_collapse_ids);
	quit;


/*STEP 8: CREATE ENTRY-ANCHORED NH EPISODES DATASET*/

*Extract the first admissions record for each entry-anchored NH episode;

	*Limit to records where admission assessment ID is not missing;
	data admiss_only; set mdstime.ED_pairs_f;
		if not missing(admiss_id);
		keep entNH_id admiss_:;
	run; 

	*Sort admission records by entry-anchored NH episode and admission entry date;
	proc sort data = admiss_only;
		by entNH_id admiss_entry_date;
	run;

	*Select the first admission record for each entry-anchored NH episode;
	data first_admiss_only; set admiss_only;
		by entNH_id admiss_entry_date;
		if first.entNH_id; *Keep the first record for each episode;
	run; 

*Add the first admission details to the ED pairs dataset based on entNH_id;
proc sql;
	create table mdstime.ED_pairs_g as
		select a.*, 
		b.admiss_id 			as entNH_admiss_id,
		b.admiss_entry_date 	as entNH_admiss_entry_date,
		b.admiss_assess_date 	as entNH_admiss_assess_date

	from mdstime.ED_pairs_f as a
	left join first_admiss_only as b
		on a.entNH_id=b.entNH_id
	order by bene_id_18900, entry_date, discharge_date;
quit; 

*Limit to records within an entry-anchored NH episode where the discharge date (discharge_date) equals the entry-anchored NH episode end date (entNH_discharge_date);
data entry_anchored_NHeps_1; set mdstime.ED_pairs_g;
	if discharge_date=entNH_discharge_date;
run; 

*Rename variables to reflect the entNH-level and remove any that are no longer relevant.
 In the event that there are multiple records with the same discharge date (and therefore the same entry-anchored NH episode ID), 
 prioritize the record where return is not anticipated;
	
	*Sort by entry-anchored NH episode ID and return anticipated;
	proc sort data = entry_anchored_NHeps_1;
		by entNH_id return_anticipated;
	run;

	*Rename variables and keep the first entry-anchored NH episode (prioritizing those where return is not anticipated);
	data mdstime.entry_anchored_NHeps_2; set entry_anchored_NHeps_1 (rename=(
		discharge_type=entNH_discharge_type
		return_anticipated = entNH_return_anticipated
		no_discharge = entNH_no_discharge
		));

		by entNH_id entNH_return_anticipated;

		if first.entNH_id;
		keep bene_id_18900 entNH_: ;
	run; 

*Remove person-time after the gold standard date of death (obtained from Medicare records);

	*Join date of death variable (hkdod) from Master Beneficiary Summary File (MBSF) to the entry-anchored NH episode dataset;
	proc sql ;
		create table mdstime.entry_anchored_NHeps_3 as 
			select 
				a.*, 
				b.hkdod,
				case when b.bene_id_18900 is not null then 1 else 0 end as mbsf_prs_match /*Flag if there's a match between MDS and MBSF beneficiary ID*/

			from mdstime.entry_anchored_NHeps_2 as a 
			left join claims.Hk100prs_bid as b
				on a.bene_id_18900 = b.bene_id_18900

			order by bene_id_18900, entNH_entry_date;
	quit; 

	*Apply date of death correction;
	data mdstime.entry_anchored_NHeps_4;
	set mdstime.entry_anchored_NHeps_3;

		*Remove records with a date of death prior to the entry-anchored NH episode start date;
		if 		not missing(hkdod) and hkdod < entNH_entry_date 		then delete; 

		*Adjust records with a date of death prior to the episode end date;
		else if not missing(hkdod) and hkdod < entNH_discharge_date 	then do;
			entNH_death_error=1; 			*Flag death error;
			entNH_discharge_date = hkdod; 	*Set discharge date to the date of death;
			entNH_discharge_type = .; 		*Set discharge type to missing;
			entNH_return_anticipated=0; 	*Mark return as not anticipated;
		end;
	run;


/*STEP 9: IDENTIFY ENTRY-ANCHORED NH EPISODES THAT BELONG TO BROADER ADMISSION-ANCHORED NH EPISODES*/

	****************************************************************************************************
	Description:
	Admission-Anchored NH episodes begin at the entry date associated with an admission assessment, 
	and end with the first of:
	 - discharge for which return is not anticipated
	 - discharge for which return is anticipated but no subsequent entries are observed within 30 days
	 - death
	Discharge records that occur within spans of continuous NH residence are ignored.
	****************************************************************************************************;

*Sort the entry-anchored NH episode data set by beneficiary ID and episode start date;
proc sort data = mdstime.entry_anchored_NHeps_4; 
	by bene_id_18900 entNH_entry_date; 
run;

*Calculate gap lengths between entry-anchored NH episodes belonging to the same person;
data mdstime.entry_anchored_NHeps_5; set mdstime.entry_anchored_NHeps_4; 
	by bene_id_18900 entNH_entry_date; 

	*Calculate the gap between the current episode's start date and the previous episode's end date;
	gap = entNH_entry_date - lag(entNH_discharge_date);

	*Set gap to missing for the first episode of each person;
	if first.bene_id_18900 then gap=.; 

run;

*Create an admission-anchored NH episode ID, applying discharge rules in order to group entry-anchored NH episodes (entry rules applied later);
data e_and_a_NHeps_a; set mdstime.entry_anchored_NHeps_5;
	by bene_id_18900;

	*Update the admission-anchored NH episode ID if:
		- This is the first record for a beneficiary,
		- The previous episode ended with no anticipated return,
		- The previous episode ended with anticipated return and the gap is more than 30 days;
	if 	(first.bene_id_18900=1) or 
		(lag(entNH_return_anticipated)=0) or 
		(lag(entNH_return_anticipated)=1 and gap >30) 
		then admNH_id + 1; 

	*Flag if an entry-anchored NH episode has an associated admission assessment;
	if missing(entNH_admiss_id) then entNH_has_admission = 0; 
		else entNH_has_admission=1;
run;

*Create an indictor variable to identify if an entry-anchored NH episode occurs before or after the first admission record within an admission-anchored NH episode;

	*admNH_had_first_admiss = 0 if entry-anchored NH episode occurs before the first admission record;
	*admNH_had_first_admiss = 1 if entry-anchored NH episode occurs after the first admission record;

	*Sort by beneficiary ID, admission-anchored NH episode ID, and entry-anchored NH episode start date;
	proc sort data = e_and_a_NHeps_a; 
		by bene_id_18900 admNH_id entNH_entry_date; 
	run;

	data mdstime.e_and_a_NHeps_b;
	    set e_and_a_NHeps_a;
		by bene_id_18900 admNH_id;

	    retain admNH_had_first_admiss;  

		*Set the indicator variable to 1 if the entry-anchored NH episode has an admission assessment;
		if entNH_has_admission=1 
			then admNH_had_first_admiss = 1; 

		*For the first record in an admission-anchored NH episode, 
		set the indicator based on whether the entry-anchored NH episode has an admission assessment;
		if first.admNH_id 
			then admNH_had_first_admiss = entNH_has_admission;	
	run;

*Remove any entry-anchored NH episode rows that occur before the first admission record within a given admission-anchored NH episode;
data mdstime.e_and_a_NHeps_c; set mdstime.e_and_a_NHeps_b;
	if admNH_had_first_admiss = 1; 
run;


/*STEP 10: CREATE AN ADMISSION-ANCHORED NURSING HOME-EPISODE LEVEL DATASET*/

*Identify the discharge dates for each admission-anchored NH episode;
proc sql;
	create table mdstime.e_and_a_NHeps_d as
		select 
			*, 
			max(entNH_discharge_date) as admNH_discharge_date format=mmddyy10. /*The latest discharge date for a given admNH_id is the end date of the admission-anchored NH episode*/
		from mdstime.e_and_a_NHeps_c
		group by admNH_id
		order by bene_id_18900, entNH_entry_date, entNH_discharge_date;
quit; 

*Identify the first admission record within each admission-anchored NH episode;
	data e_and_a_admiss_only; set mdstime.e_and_a_NHeps_d;

		*Include records where the admission assessment ID is not missing;
		if not missing(entNH_admiss_id);

		keep admNH_id entNH_admiss_:;
	run; 

	*Sort by admission-anchored NH episode ID and admission entry date;
	proc sort data = e_and_a_admiss_only;
		by admNH_id entNH_admiss_entry_date;
	run;

	data e_and_a_first_admiss_only; set e_and_a_admiss_only;
		by admNH_id entNH_admiss_entry_date;

		*Keep the first admission record associated within each admission-anchored NH episode;
		if first.admNH_id;
	run;

*Merge first admission details to all records within the admission-anchored NH episode,
 setting the admission entry date to be the admission-anchored NH entry date
 Note: This step is necessary because no admission requirements
 were applied when we constructed the entry-anchored NH episodes;
proc sql;
	create table mdstime.e_and_a_NHeps_e as
		select 
			a.*, 
			b.entNH_admiss_id 			as admNH_admiss_id,
			b.entNH_admiss_entry_date 	as admNH_entry_date,
			b.entNH_admiss_assess_date 	as admNH_admiss_assess_date
		from mdstime.e_and_a_NHeps_d as a
		left join e_and_a_first_admiss_only as b
		on a.admNH_id=b.admNH_id
		order by bene_id_18900, entNH_entry_date, entNH_discharge_date;
quit; 

*Reduce dataset to the admission-anchored NH-level;

	*Sort by beneficiary ID, admission-anchored NH episode ID, and entry-anchored NH episode ID;
	proc sort data = mdstime.e_and_a_NHeps_e;
		by bene_id_18900 admNH_id entNH_entry_date;
	run; 

	*Take only the last record in each admission-anchored NH episode, renaming and keeping only variables that remain accurate at this level; 
	data mdstime.admiss_anchored_NHeps_a; set mdstime.e_and_a_NHeps_e
		(keep=bene_id_18900 hkdod mbsf_prs_match admNH: entNH_discharge_type entNH_return_anticipated entNH_no_discharge entNH_death_error);
		by bene_id_18900 admNH_id;

		if last.admNH_id;

		rename 
			entNH_discharge_type=admNH_discharge_type 
			entNH_return_anticipated=admNH_return_anticipated
			entNH_no_discharge =admNH_no_discharge
			entNH_death_error=admNH_death_error;

		drop admNH_had_first_admiss; 
	run;

*Clean admission-anchored NH episodes using date of death;

*Note: Because we moved the entry date to the admission entry date in the admission-anchored NH episodes,
	   there may be records that now need to be deleted, even though we cleaned
	   the entry-anchored NH data previously;

data mdstime.admiss_anchored_NHeps_b;
	set mdstime.admiss_anchored_NHeps_a;

	*Apply date of death correction;

		*Remove records with a date of death prior to the admission-anchored NH episode start date;
		if 		not missing(hkdod) and hkdod < admNH_entry_date 		then delete;

		*Adjust records with a date of death prior to the episode end date;
		else if not missing(hkdod) and hkdod < admNH_discharge_date 	then do;
			admNH_death_error=1; *Flag death error;
			admNH_discharge_date = hkdod; *Set discharge date to the date of death;
			admNH_discharge_type = .; *Set discharge type to missing;
			admNH_return_anticipated=0; *Mark return as not anticipated;
			end;

	*Calculate the length of the admission-anchored NH episode;
	admNH_length = admNH_discharge_date - admNH_entry_date +1;

run; 


/*STEP 11: REMOVE ANY EPISODES FROM THE ENTRY-ANCHORED AND ADMISSION-ANCHORED NH DATASETS IN WHICH ENTRY_DATE = DISCHARGE_DATE*/

	********************************************************************************
	*Note:
	 	- These residents may have only been in the home for a matter of hours
	   	- Need to do this last for entNH dataset after admNH dataset is constructed 
		  because we do not want to exclude any records with a discharge
	   	  not anticipated to return when constructing admission-anchored episodes;
	********************************************************************************;

	*Entry-anchored NH episode dataset;
	data mdstime.entry_anchored_NHeps_6; set mdstime.entry_anchored_NHeps_5; 
		if entNH_entry_date = entNH_discharge_date then delete;
		drop gap;
	run;

	*Admisson-anchored NH episode dataset;
	data mdstime.admiss_anchored_NHeps_c; set mdstime.admiss_anchored_NHeps_b;
		if admNH_entry_date=admNH_discharge_date then delete;
	run;


/*STEP 12: SAVE NEAR FINAL VERSIONS OF DATASETS FOR EASY REFERENCE AND APPLY LABELS*/

	*Entry-anchored NH episode dataset;
	data mdstime.entry_anchored_NHeps_nearfinal; set mdstime.entry_anchored_NHeps_6; 
		label 
	        entNH_admiss_assess_date = "Entry-Anchored NH Episode: Assessment Reference Date for Admission assessment (not admission date) (if one associated with entNH episode)"
	        entNH_admiss_id          = "Entry-Anchored NH Episode: MDS repository record ID for admission assessment (if one associated with entNH episode)"
	        entNH_discharge_date     = "Entry-Anchored NH Episode: Discharge Date"
	        entNH_discharge_type     = "Entry-Anchored NH Episode: Discharge Type (based on MDS variable M3A0310F)"
	        entNH_entry_date         = "Entry-Anchored NH Episode: Entry Date"
	        entNH_id                 = "Entry-Anchored NH Episode: ID [temporary variable - delete in program 5 - not compatible outside of version &version.]"
	        entNH_no_discharge       = "Entry-Anchored NH episode: Flag - no discharge record, discharge date imputed with study end date (or hkdod if earlier)"
	        entNH_return_anticipated = "Entry-Anchored NH episode: Discharge return anticipated category (0=not anticipated or death, 1=anticipated, 2=no discharge or death record)"
	        hkdod                    = "Beneficiary date of death (from MBSF)"
	        mbsf_prs_match           = "Flag: bene_id from MDS has matching record in MBSF";
		keep 
			bene_id_18900
			entNH_admiss_assess_date
			entNH_admiss_id
			entNH_discharge_date
			entNH_discharge_type
			entNH_entry_date
			entNH_id
			entNH_no_discharge
			entNH_return_anticipated
			hkdod
			mbsf_prs_match;
	run;

	*Admission-anchored NH episode dataset;
	data mdstime.admiss_anchored_NHeps_nearfinal; set mdstime.admiss_anchored_NHeps_c; 
	    label 
	        admNH_admiss_assess_date = "Admission-Anchored NH Episode: Assessment Reference Date for Admission assessment (not admission date)"
	        admNH_admiss_id          = "Admission-Anchored NH Episode: MDS repository record ID for admission assessment"
	        admNH_discharge_date     = "Admission-Anchored NH Episode: Discharge Date"
	        admNH_discharge_type     = "Admission-Anchored NH Episode: Discharge Type (based on MDS variable M3A0310F)"
	        admNH_entry_date         = "Admission-Anchored NH Episode: Admission Date"
	        admNH_id                 = "Admission-Anchored NH Episode: ID [temporary variable - delete in program 5 - not compatible outside of version &version.]"
	        admNH_no_discharge       = "Admission-Anchored NH episode: Flag - no discharge record, discharge date imputed with study end date (or hkdod if earlier)"
	        admNH_return_anticipated = "Admission-Anchored NH episode: Discharge return anticipated category (0=not anticipated or death, 1=anticipated, 2=no discharge or death record)"
	        hkdod                    = "Beneficiary date of death (from MBSF)"
	        mbsf_prs_match           = "Flag: bene_id from MDS has matching record in MBSF";
		keep 
			bene_id_18900
			admNH_admiss_assess_date
			admNH_admiss_id
			admNH_discharge_date
			admNH_discharge_type
			admNH_entry_date
			admNH_id
			admNH_no_discharge
			admNH_return_anticipated
			hkdod
			mbsf_prs_match;
	run;

/*END OF PROGRAM*/
