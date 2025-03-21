/*
Overall Project: Drug Observable Nursing Home (NH) Time algorithm

	Description: To identify episodes of person-time in the Nursing Home population 
	in which Medicare Part D Fee-for-Service drug dispensings are observable. Nursing Home time is 
	defined using MDS data. Drug observable time is defined by enrollment in Parts A, B, and D FFS, 
	and outside of periods of hospitalization and post-acute care (SNF), both of which are covered
	by bundled payments under Part A and cannot be observed in Part D claims data.

Program: 2_tutorial_data_prep_to_remove_unobs_time

	Desciption: 
		1. Prepare Medicare Beneficiary Summary File (MBSF), Medicare Provider Analysis and Review (MEDPAR) SNF, and MEDPAR Hospitalization data for day-level processing. 
		2. Prepare Nursing Home episode datasets from program 1 (i.e., 1_tutorial_constructing_nh_episodes) for day-level processing.

Programmer: Adam DAmico

Date: 19Dec2024

Version History:

*/

/*Set location to save log file*/
proc printto log="P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\output\Publication_version\Program_2_log_file_&sysdate..log" new;
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

/*Assign study dates, enrollment lookback period date, and dataset names for admission- and entry-anchored NH episodes*/
%let startofstudy	= '01jan2013'd;
%let endofstudy		= '31dec2020'd;
%let e_lookback 	= '01jan2012'd; *enrollment lookback period;
%let AdmissAnchCohort = mdstime.admiss_anchored_NHeps_nearfinal; 
%let EntryAnchCohort = mdstime.entry_anchored_NHeps_nearfinal;

/*Note: Some macro invocations and datasets are hardcoded for specific years.
		If you are adapting code for another study period, make sure to update
		year-specific references in code below*/

/*STEP 1: RESTRICT SNF AND HOSPITAL DATA*/

*Restrict SNF records to those with admission dates within the study period;
data prepmedp.SNF_restrict1; set claims.hnallmed;
	if &startofstudy. <= hnadmdt <= &endofstudy.;
run; 

*Restrict SNF records to persons in the entry-anchored NH episode dataset;
	*Note: We use the entry-anchored NH episode dataset here because it has all of the beneficiary IDs we need;
proc sql;
	create table prepmedp.SNF_restrict2 as 
		select *
		from prepmedp.SNF_restrict1
		where bene_id_18900 in (select bene_id_18900 from &EntryAnchCohort.); 
quit;

*Restrict hospital records to those with admission dates within the study period;
data prepmedp.hosp_restrict1; set claims.hiallmed;
	if &startofstudy. <= hiadmdt <= &endofstudy.;
run;

*Restrict hospital records to persons in the entry-anchored NH episode dataset;
	*Note: We use the entry-anchored NH episode dataset here because it has all of the beneficiary IDs we need;
proc sql;
	create table prepmedp.hosp_restrict2 as 
		select *
		from prepmedp.hosp_restrict1
		where bene_id_18900 in (select bene_id_18900 from &EntryAnchCohort.);
quit;


/*STEP 2: CLEAN SNF AND HOSPITAL DATA*/

*Clean SNF records;
data prepmedp.SNF_restrict3; 
	set prepmedp.SNF_restrict2 (rename=(
		hnclmid =snf_clmid
		hnadmdt=snf_entry_date
		hndisch=snf_discharge_date
		hnlos=snf_los));

	*Impute SNF discharge date if missing by adding the length of stay to the entry date;
	if snf_discharge_date=. then do;
		snf_discharge_missing = 1;
		snf_discharge_date=intnx('day', snf_entry_date, snf_los); 
	end;
	else snf_discharge_missing=0;

	*Delete records where the SNF discharge date is earlier than the entry date;
	if . < snf_discharge_date < snf_entry_date then delete;

	*Cap SNF discharge date at the study end date if it occurs after the study period;
		*Note: This reduces the computing requirements of day-level analysis;
	if snf_discharge_date > &endofstudy. then snf_discharge_date = &endofstudy.;

	keep  bene_id_18900 snf_clmid  snf_entry_date  snf_discharge_date snf_discharge_missing snf_los;
run; 

*Clean hospital records;
data prepmedp.hosp_restrict3; 
	set prepmedp.hosp_restrict2 (rename=(
		hiclmid =hosp_clmid
		hiadmdt=hosp_entry_date
		hidisch=hosp_discharge_date
		hilos=hosp_los));

	*Impute hospital discharge date if missing by adding the length of stay to the entry date;
	if hosp_discharge_date=. then do;
		hosp_discharge_missing = 1;
		hosp_discharge_date=intnx('day', hosp_entry_date, hosp_los); 
		end;
		else hosp_discharge_missing=0;

	*Delete records where the hospital discharge date is earlier than the entry date;
	if . < hosp_discharge_date < hosp_entry_date then delete;

	*Cap hospital discharge date at the study end date if it occurs after the study period;
		*Note: This reduces the computing requirements of day-level analysis;
	if hosp_discharge_date > &endofstudy. then hosp_discharge_date = &endofstudy.;

	keep  bene_id_18900 hosp_clmid  hosp_entry_date  hosp_discharge_date hosp_discharge_missing hosp_los;
run; 


/*STEP 3: RESTRICT ENROLLMENT DATASETS TO THE BENEFICIARIESS IN OUR COHORT*/

%macro medicare (mediyr, bene_dataset);

*Subset annual MBSF files to beneficiaries and variables of interest;
proc sql;
    create table enroll.mediann_&mediyr as
    select 
        bene_id_18900,
		hkcuren label = "Reason for current Medicare entitlement",
        hkorgen label = "Reason for original Medicare entitlement",
        hkyear label = "Year of Medicare coverage (from annual file)",
        hkdob label = "Date of birth from Medicare",
        hkdod label = "Date of death from Medicare (annual-level files)",
        hkhmo1 label = "Fee-for-service enrollment monthly indicator (January)",
        hkhmo2 label = "Fee-for-service enrollment monthly indicator (February)",
        hkhmo3 label = "Fee-for-service enrollment monthly indicator (March)",
        hkhmo4 label = "Fee-for-service enrollment monthly indicator (April)",
        hkhmo5 label = "Fee-for-service enrollment monthly indicator (May)",
        hkhmo6 label = "Fee-for-service enrollment monthly indicator (June)",
        hkhmo7 label = "Fee-for-service enrollment monthly indicator (July)",
        hkhmo8 label = "Fee-for-service enrollment monthly indicator (August)",
        hkhmo9 label = "Fee-for-service enrollment monthly indicator (September)",
        hkhmo10 label = "Fee-for-service enrollment monthly indicator (October)",
        hkhmo11 label = "Fee-for-service enrollment monthly indicator (November)",
        hkhmo12 label = "Fee-for-service enrollment monthly indicator (December)",
        hkebi1 label = "Part A and B enrollment monthly indicator (January)",
        hkebi2 label = "Part A and B enrollment monthly indicator (February)",
        hkebi3 label = "Part A and B enrollment monthly indicator (March)",
        hkebi4 label = "Part A and B enrollment monthly indicator (April)",
        hkebi5 label = "Part A and B enrollment monthly indicator (May)",
        hkebi6 label = "Part A and B enrollment monthly indicator (June)",
        hkebi7 label = "Part A and B enrollment monthly indicator (July)",
        hkebi8 label = "Part A and B enrollment monthly indicator (August)",
        hkebi9 label = "Part A and B enrollment monthly indicator (September)",
        hkebi10 label = "Part A and B enrollment monthly indicator (October)",
        hkebi11 label = "Part A and B enrollment monthly indicator (November)",
        hkebi12 label = "Part A and B enrollment monthly indicator (December)",
        hkdcontr1 label = "Part D monthly enrollment indicator (January)",
        hkdcontr2 label = "Part D monthly enrollment indicator (February)",
        hkdcontr3 label = "Part D monthly enrollment indicator (March)",
        hkdcontr4 label = "Part D monthly enrollment indicator (April)",
        hkdcontr5 label = "Part D monthly enrollment indicator (May)",
        hkdcontr6 label = "Part D monthly enrollment indicator (June)",
        hkdcontr7 label = "Part D monthly enrollment indicator (July)",
        hkdcontr8 label = "Part D monthly enrollment indicator (August)",
        hkdcontr9 label = "Part D monthly enrollment indicator (September)",
        hkdcontr10 label = "Part D monthly enrollment indicator (October)",
        hkdcontr11 label = "Part D monthly enrollment indicator (November)",
        hkdcontr12 label = "Part D monthly enrollment indicator (December)",
        hksex label = "Sex",
        hkrtrace label = "Race",
        hkstate
    from 
        claims.Hk100ann_bid_&mediyr
    where 
        bene_id_18900 in (select bene_id_18900 from &bene_dataset.)
    order by 
        bene_id_18900;
quit;

%mend medicare;

***********************************************************
Note:
We use the entry-anchored NH episode cohort here 
because it has all beneficiary IDs we could possibly need.

Also, we include an additional year of enrollment lookback 
before the start of the study period. This is not strictly
necessary for identifying drug observable time during the 
study period, but may be desired in later analyses  if 
they require a lookback period (for example, if are studying
new use episodes of a medication and need to include a 
washout period). You can adjust the length of the lookback
to suite your specific study needs. 
***********************************************************;

*Run the macro for each year of interest using the entry-anchored NH episode dataset;
%medicare (mediyr=2012, bene_dataset=&EntryAnchCohort.); 
%medicare (mediyr=2013, bene_dataset=&EntryAnchCohort.);
%medicare (mediyr=2014, bene_dataset=&EntryAnchCohort.);
%medicare (mediyr=2015, bene_dataset=&EntryAnchCohort.);
%medicare (mediyr=2016, bene_dataset=&EntryAnchCohort.);
%medicare (mediyr=2017, bene_dataset=&EntryAnchCohort.);
%medicare (mediyr=2018, bene_dataset=&EntryAnchCohort.); 
%medicare (mediyr=2019, bene_dataset=&EntryAnchCohort.); 
%medicare (mediyr=2020, bene_dataset=&EntryAnchCohort.); 


/*STEP 4: IDENTIFY PERSON-MONTHS IN WHICH ALL ENROLLMENT CRITERIA ARE MET (I.E., ENROLLED IN PART A, B AND D FEE-FOR-SERVICE)*/

%macro medicare2 (mediyr);

/************************************************************
*															*
*			CREATE RESTRICTED DATASETS CONTAINING           *
*			MONTHLY ENROLLMENT INFORMATION 					*
*			(restricting to months meeting eligibilty		*
*			requirements and joined together)				*
*															*
************************************************************/

*Transpose the monthly fee-for-service (FFS) variables to the long format;
	*Note: hkhmo1-hkhmo12 represent FFS enrollment for January to December, respectively.
		   Transposed values are stored in the variable hkhmo1 in the output dataset;
proc transpose data=enroll.mediann_&mediyr out=medihkhmolong_&mediyr prefix=hkhmo;
	by bene_id_18900 hkdod ;
	var hkhmo1-hkhmo12;
run;

*Transpose the monthly Part A and B variables to the long format;
	*Note: hkebi1-hkebi12 represent Part A and B enrollment for January to December, respectively.
		   Transposed values are stored in the variable hkebi1 in the output dataset;
proc transpose data=enroll.mediann_&mediyr  out=medihkbeilong_&mediyr prefix=hkebi;
	by bene_id_18900 hkdod ;
	var hkebi1-hkebi12;
run;

*Transpose the monthly Part D variables to the long format;
	*Note: hkdcontr1-hkdcontr12 represent Part D enrollment for January to December, respectively.
	       Transposed values are stored in the variable hkcontr1 in the output dataset;
proc transpose data=enroll.mediann_&mediyr out=meditrlong_&mediyr prefix=hkcontr;
	by bene_id_18900 hkdod;
	var hkdcontr1-hkdcontr12;
run;

****************************************************************************************************************;
*Variable: hkhmo1 (FFS enrollment variable)
Code	Code value
0	    Not a member of an HMO (Health Maintenance Organization)
1	    Non-lock-in, CMS to process provider claims
2	    Non-lock-in, group health organization (GHO, MA plan) to process in plan Part A and in area Part B claims
4	    Fee-for-service participant in case or disease management demonstration project
A	    Lock-in, CMS to process provider claims
B	    Lock-in, GHO to process in plan Part A and in area Part B claims
C	    Lock-in, GHO to process all provider claims
****************************************************************************************************************;

*Create an FFS enrollment indicator (1=Enrolled, 0=Not enrolled), accounting for the date of death; 
data hmoflag_&mediyr;
set medihkhmolong_&mediyr;

	*Extract the month from the variable _name_, and create a SAS date for the first day of the month and year (i.e., date of enrollment);
	month=input(substr(_name_, 6, 2), 8.);
	year=&mediyr;
	month_year=mdy(month, 1, year); /*SAS date for the first day of the month and year (i.e., date of enrollment)*/

	*Assign FFS enrollment status based on HMO membership and date of death;
	if hkhmo1 in ("0") and hkdod=. then ffs=1; /*No HMO membership and no date of death: FFS = 1*/
	else if hkhmo1 in ("0") and hkdod~=. and hkdod>=month_year then ffs=1; /*No HMO membership with date of death on or after the first of the month: FFS = 1*/
	else ffs=0; /*Otherwise, not enrolled in FFS during the month: FFS = 0*/
	 
	drop month ;
	format month_year date9.;
	label ffs="Fee-for-service enrollment indicator (1=Enrolled, 0=Not enrolled)" month_year="Date of enrollment (set to the first of the month)";

run;

*******************************************************;
*Variable: hkebi1 (Part A and B enrollment variable)
Code	Code value
0	    Not entitled
1	    Part A only
2	    Part B only
3	    Part A and Part B
A	    Part A state buy-in
B	    Part B state buy-in
C	    Part A and Part B state buy-in
********************************************************;

*Create a Part A and B enrollment indicator (1=Enrolled, 0=Not enrolled), accounting for the date of death; 
data hkebiflag_&mediyr;
	set medihkbeilong_&mediyr;

	*Extract the month from the variable _name_, and create a SAS date for the first day of the month and year (i.e., date of enrollment);
	month=input(substr(_name_, 6, 2), 8.);
	year=&mediyr;
	month_year=mdy(month, 1, year); /*SAS date for the first day of the month and year (i.e., date of enrollment)*/

	*Assign Part A and B enrollment status, accounting for the date of death;
	if hkebi1 in ("3", "C") and hkdod=. then hkebi_ind=1; /*Enrollment is "Part A and Part B" or "Part A and Part B state buy-in" AND no date of death: hkebi_ind=1*/
	else if hkebi1 in ("3", "C") and hkdod~=. and hkdod>=month_year then hkebi_ind=1; /*Enrollment is "Part A and Part B" or "Part A and Part B state buy-in" AND date of death on or after the first of the month: hkebi_ind=1*/
	else hkebi_ind=0; /*Otherwise, not enrolled in Part A and B during the month: hkebi_ind=0*/

	drop month ;
	format month_year date9.;
	label hkebi_ind="Part A and B enrollment indicator (1=Enrolled, 0=Not enrolled)" month_year="Date of enrollment (set to the first of the month)";

run;

*************************************************************************************************************************************************;
*Variable: hkcontr1 (Part D (PTD) enrollment variable)
Code	        Code value
E	            Employer direct plan (starting January 2007)
H	            Managed care organizations (MCO) other than a regional PPO (i.e., local MA-PDs, 1876 cost
                plans, Program of All-Inclusive Care for the Elderly (PACE) plans, private fee-forservice plans, or demonstration organization plans)
R	            Regional preferred provider organization (PPO)
S	            Stand-alone prescription drug plan (PDP)
X	            Limited Income Newly Eligible Transition plan (LINET)
N	            Not Part D Enrolled
0	            Not Medicare enrolled for the month
Null/missing	Enrolled in Medicare A and/or B, but no Part D enrollment data for the
                beneficiary.
*************************************************************************************************************************************************;

*Create a Part D enrollment indicator (1=Enrolled, 0=Not enrolled), accounting for the date of death;
data hkdcontrflag_&mediyr ;
	length ptd $20.;
	set meditrlong_&mediyr;

	*Extract the month from the variable _name_, and create a SAS date for the first day of the month and year (i.e., date of enrollment);
	month= input(substr(_name_, 9, 2), 8.) ;
	year=&mediyr;
	month_year=mdy(month, 1, year);

	*Determine Part D (PTD) status based on the first character of hkcontr1;
	if hkcontr1 =: 'E' then ptd = 'Employer';
	else if hkcontr1 =: 'H' then ptd = 'MCO non PPO';
	else if hkcontr1 =: 'R' then ptd = 'PPO';
	else if hkcontr1 =: 'S' then ptd = 'PDP';
	else if hkcontr1 =: 'X' and hkcontr1 ne 'X' then ptd = 'LINET'; 
	else if hkcontr1 =: 'N' or hkcontr1 eq 'X' then ptd = 'No PtD';
	else if hkcontr1 =: '0' then ptd = 'Not Medicare';
	else if hkcontr1 eq '' then ptd = 'PtD status ???'; *Unknown Part D status;
	else ptd = '???'; *Unknown Part D status;

	*Assign Part D enrollment status based on ptd value and date of death;
	if ptd in ('Employer', 'MCO non PPO', 'PPO', 'PDP', 'LINET') and hkdod=. then ptd_contr=1; /*Enrolled in Part D AND no date of death: ptd_contr=1*/
	else if ptd in ('Employer', 'MCO non PPO', 'PPO', 'PDP', 'LINET') and hkdod>=month_year then ptd_contr=1; /*Enrolled in Part D AND date of death on or after the first of the month: ptd_contr=1*/
	else ptd_contr=0; /* Otherwise, not enrolled in Part D during the month: ptd_contr=0*/

	drop month _label_ _name_;
	format month_year date9.;
	label ptd_contr="Part D enrollment indicator (1=Enrolled, 0=Not enrolled)" month_year="Date of enrollment (set to the first of the month)";
run;

*Limit each dataset to rows (months) in which the specific enrollment criteria is met;
	*Filter dataset to months where beneficiaries are enrolled in FFS;
	data hmoflag_&mediyr.; set hmoflag_&mediyr.;
		keep bene_id_18900 month_year ffs;
		if ffs=1;
	run; 

	*Filter dataset to months where beneficiaries are enrolled in Part D;
	data hkdcontrflag_&mediyr.; set hkdcontrflag_&mediyr.;
		keep bene_id_18900 month_year ptd_contr;
		if ptd_contr=1;
	run; 

	*Filter dataset to months where beneficaries are enrolled in Part A and B;
	data hkebiflag_&mediyr.; set hkebiflag_&mediyr.;
		keep bene_id_18900 month_year hkebi_ind;
		if hkebi_ind=1;
	run; 

*Identify months where all 3 enrollment criteria are met with an inner join;
	*Join FFS enrollment dataset with Part D enrollment dataset by beneficiary ID and month_year;
	proc sql;
		create table merge_ffs_d_&mediyr. as 
			select a.*, b.*
			from hmoflag_&mediyr. as a
			inner join hkdcontrflag_&mediyr. as b
			on a.bene_id_18900=b.bene_id_18900 and a.month_year=b.month_year;
	quit;
	run; 

	*Join the resulting dataset with Part A and B enrollment dataset by beneficiary ID and month_year;
	proc sql;
		create table enroll.merge_ffs_abd_&mediyr. as 
			select a.*, b.*
			from merge_ffs_d_&mediyr. as a
			inner join hkebiflag_&mediyr. as b
			on a.bene_id_18900=b.bene_id_18900 and a.month_year=b.month_year; 
	quit;
	run;

*Delete intermediary temporary datasets;
proc datasets nolist library=work kill  ;
run;
quit;

%mend medicare2;

*Run macro for each year of interest;
	*Note: It takes about 1 hour to run all of these;
%medicare2 (2012); 
%medicare2 (2013);
%medicare2 (2014); 
%medicare2 (2015); 
%medicare2 (2016);
%medicare2 (2017); 
%medicare2 (2018); 
%medicare2 (2019); 
%medicare2 (2020); 


/*STEP 5: CONCATENATE PERSON-MONTHS THAT MEET CRITERIA ACROSS ALL YEARS*/

data enroll.concatenate_ffs_abd (keep=bene_id_18900 month_year);
	set ENROLL.MERGE_FFS_ABD_2012
		ENROLL.MERGE_FFS_ABD_2013
		ENROLL.MERGE_FFS_ABD_2014
		ENROLL.MERGE_FFS_ABD_2015
		ENROLL.MERGE_FFS_ABD_2016
		ENROLL.MERGE_FFS_ABD_2017
		ENROLL.MERGE_FFS_ABD_2018
		ENROLL.MERGE_FFS_ABD_2019
		ENROLL.MERGE_FFS_ABD_2020;
run; 

*Delete intermediary datasets for space;
proc datasets library=ENROLL nolist;
    delete MERGE_FFS_ABD_2012
           MERGE_FFS_ABD_2013
           MERGE_FFS_ABD_2014
           MERGE_FFS_ABD_2015
           MERGE_FFS_ABD_2016
           MERGE_FFS_ABD_2017
           MERGE_FFS_ABD_2018
           MERGE_FFS_ABD_2019
           MERGE_FFS_ABD_2020;
quit;


/*STEP 6: ROLL UP CONTINUOUS ENROLLMENT PERIODS INTO A SINGLE ENROLLMENT EPISODE*/

*Sort by beneficiary ID and date of enrollment (month_year);
proc sort data=ENROLL.concatenate_ffs_abd;
	by bene_id_18900 month_year;
run;

*Create enrollment episode start and end date variables that are accurate for the last month_year of an enrollment episode;
data enroll_rollup_a;
	set ENROLL.concatenate_ffs_abd ;

	by bene_id_18900 month_year ;

	retain enroll_start enroll_end;

	*For the first record of each beneficiary, set both enrollment start and end dates to month_year;
	if first.bene_id_18900 then do; 
		enroll_start = month_year;
		enroll_end = month_year ;
	end;

	*For subsequent records, update enrollment dates based on the month_year;
	if first.bene_id_18900 ne 1 then do; 
		*If month_year is 1 month after the previous record's enrollment episode end date, extend the enrollment end date to this record's month_year value;
		if month_year = intnx("month",enroll_end,+1,"b") then 
			enroll_end = month_year ;
		*Otherwise, start a new enrollment episode with the current month_year;
		else do ; 
			enroll_start = month_year;
			enroll_end = month_year ;
		end;
	end;

	format enroll_start enroll_end mmddyy10. ;
run;

*Take the last month-year of each enrollment episode;
	*Sort by beneficiary ID and enrollment start date;
	proc sort data = enroll_rollup_a;
		by bene_id_18900 enroll_start;
	run;

	data enroll_rollup_b ;
		set enroll_rollup_a ;
		by bene_id_18900 enroll_start ;

		if last.enroll_start ; /*Keep the last month-year of the enrollment episode*/
	run;


/*STEP 7: SET ENROLLMENT END DATE TO THE LAST DAY OF THE MONTH AND CREATE AN ENROLLMENT ID VARIABLE*/

proc sort data = enroll_rollup_b; 
	by bene_id_18900 enroll_start enroll_end;
run;

data enroll.enroll_eps_a;
	set enroll_rollup_b;

	enroll_id +1; 
	enroll_end=intnx("month",enroll_end,0,"e"); /*Sets enrollment end date to the last day of the month*/

	label enroll_id="Enrollment (A/B, D, FFS) Episode ID";
	drop month_year;
run; 

*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This section is specific to the cohort (entry- or admission-anchored)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~;

%macro predayprocessing(x, dataset); 
*x represents the prefix 
	adm for admission-anchored NH episodes and 
	ent for entry-anchored NH episodes;
*dataset is either 
	&AdmissAnchCohort. or 
	&EntryAnchCohort.;

/*STEP 8: CATEGORIZE NH EPISODES AS FULLY ENROLLED, FULLY NOT ENROLLED, OR PARTIALLY ENROLLED*/

	*****************************************************************************************
	Note:
	We do this to improve computational efficiency. Only those episodes that have
	a mix of enrolled and not enrolled days will need day-level processing. For fully enrolled
    and fully not enrolled episodes, we can assign day-level enrollement based on solely 
	on the episode-level category we determine here. 
	*****************************************************************************************;

*Identify NH episodes without any enrollment time (i.e., beneficiary never meets the enrollment criteria during the NH episode);
proc sql;
	create table &x.NH_no_enrollment as
		select a.*, b.enroll_id, b.enroll_start, b.enroll_end
		from &dataset. as a  
		left join enroll.enroll_eps_a as b
		on 	a.bene_id_18900=b.bene_id_18900 and (
			 (b.enroll_start <= a.&x.NH_entry_date     <=  b.enroll_end and /*Fully enrolled: NH entry date is within enrollment episode AND NH discharge date is within enrollment episode*/
		 	  b.enroll_start <= a.&x.NH_discharge_date <=  b.enroll_end	)   
				or 
			 (a.&x.NH_entry_date <=  b.enroll_start    <=  a.&x.NH_discharge_date or /*Partially enrolled: Start of enrollment episode is within NH episode OR end of enrollment is within NH episode*/
			  a.&x.NH_entry_date <=  b.enroll_end      <=  a.&x.NH_discharge_date   ) )  
		where b.enroll_id is null; /*Only include records with no matching enrollment ID based on the criteria above*/
 quit; 

*Identify NH episodes with complete enrollment time (i.e., beneficiary meets enrollment criteria for entire NH episode);
proc sql;
	create table &x.NH_full_enrollment as
		select a.*, b.enroll_id, b.enroll_start, b.enroll_end
		from &dataset. as a  
		inner join enroll.enroll_eps_a as b
		on 	a.bene_id_18900=b.bene_id_18900 and 
			b.enroll_start <= a.&x.NH_entry_date     <=  b.enroll_end and /*NH entry date is within enrollment episode AND NH discharge is within enrollment episode*/
		 	b.enroll_start <= a.&x.NH_discharge_date <=  b.enroll_end	; 
 quit; 

*Identify NH episodes with partial enrollment time (i.e., beneficiary meets enrollment criteria during a portion of the NH episode);
 	*Create a dataset of NH episodes NOT categorized as "Fully enrolled" or "Fully not enrolled";
	proc sql;
		create table &x.NH_remaining_eps as
			select * 
			from &dataset.
			where 	&x.NH_id not in (select &x.NH_id from &x.NH_no_enrollment)
			and 	&x.NH_id not in (select &x.NH_id from &x.NH_full_enrollment);
	*Use the remaining NH episodes to identify partial enrollment;
		create table mdstime.&x.NH_partial_enrollment as
			select a.*, b.enroll_id, b.enroll_start, b.enroll_end
			from &x.NH_remaining_eps as a  
			inner join enroll.enroll_eps_a as b
			on 	a.bene_id_18900=b.bene_id_18900 and 
			   (a.&x.NH_entry_date 	<=  b.enroll_start    <=  a.&x.NH_discharge_date or /*Start of enrollment episode is within NH episode OR end of enrollment episode is within NH episode*/
				a.&x.NH_entry_date 	<=  b.enroll_end      <=  a.&x.NH_discharge_date);
	quit; 

	*Note: mdstime.&x.NH_partial_enrollment should contain all the overlapping enrollment episodes
	that require day-level processing;

*Bring partial enrollment dataset to the NH episode level and set all NH episode-level enrollment variables to null;
	data &x.NH_partial_enrollment2; set mdstime.&x.NH_partial_enrollment;
		enroll_id=.;
		enroll_start=.;
		enroll_end=.;
	run;

	*Remove duplicates at the NH episode level;
	proc sort data = &x.NH_partial_enrollment2 nodupkey out=&x.NH_partial_enrollment3;
		by &x.NH_id;
	run; 

*Concatenate the enrollment datasets (i.e., "Fully enrolled," "Fully not enrolled," "Partially enrolled") to recreate the &x.NH dataset with an enrollment category variable;
proc format;
    value enrollcatf
        0 = '0. No Enrollment'
        1 = '1. Full Enrollment'
        2 = '2. Partial Enrollment';
run;

data mdstime.&x.NH_w_enrollcat; 
	set &x.NH_no_enrollment(in=a) &x.NH_full_enrollment(in=b) &x.NH_partial_enrollment3(in=c);

	if a=1 then &x.NH_enrollcat=0; *No enrollment;
	if b=1 then &x.NH_enrollcat=1; *Full enrollment;
	if c=1 then &x.NH_enrollcat=2; *Partial enrollment;

	format &x.NH_enrollcat enrollcatf.;
run;

	*Note: This dataset (mdstime.&x.NH_w_enrollcat) should have the same N as the original NH episode-level dataset (&AdmissAnchCohort. or &EntryAnchCohort.);


/*STEP 9: PARTITION DATA INTO 100 SMALLER DATASETS FOR PARALLEL PROCESSING AT THE DAY LEVEL*/

	***********************************************************************************************************
	Note:
	This is necessary given the number of records that need to be processed at the day level.
	The process below ensures that the partitions will be roughly equal regardless of the dataset N.
	Depending on the N and resources available, it may be necessary to increase the number of partitions;
	***********************************************************************************************************;

*Sort by beneficiary ID and NH episode start date;
proc sort data = mdstime.&x.NH_w_enrollcat;
	by bene_id_18900 &x.NH_entry_date;
run;

*Determine the number of observations and calculate partition size as a macro variable;
proc sql noprint;
    select count(*) into :num_obs from mdstime.&x.NH_w_enrollcat;
quit;

%let partition_size = %sysevalf(&num_obs / 100, ceil); /*ceil (i.e., ceiling) rounds up to the nearest integer*/

*Add a partition identifier to the dataset 
 (increments if the previous observation number was evenly divisible by the partition size);
data mdstime.&x.NH_w_enrollcat2;
    set mdstime.&x.NH_w_enrollcat;
    retain partition_num 1; 
	if mod(_N_ - 1, &partition_size) = 0 and _N_ > 1 then partition_num + 1;
run;

*Split the dataset into 100 partitions based on the partition number;
%macro create_partitions;
    %do i = 1 %to 100;
        data partin.&x.NH_partition_&i;
            set mdstime.&x.NH_w_enrollcat2;
            where partition_num = &i; *Select records belonging to partition &i;
        run;
    %end;
%mend create_partitions;

*Run macro;
%create_partitions ;

*End larger macro;
%mend predayprocessing; 

*Process both the admission-anchored and entry-anchored NH episode datasets;
options mlogic mprint;
%predayprocessing(x=adm, dataset=&AdmissAnchCohort.);
%predayprocessing(x=ent, dataset=&EntryAnchCohort.);

/*END OF PROGRAM*/
