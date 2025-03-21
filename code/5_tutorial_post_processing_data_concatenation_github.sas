/*
Overall Project: Drug Observable Nursing Home Time algorithm

	Description: To identify episodes of person-time in the Nursing Home (NH) population 
	in which Medicare Part D Fee-for-Service drug dispensings are observable. Nursing Home time is 
	defined using MDS data. Drug observable time is defined by enrollment in Parts A, B, and D FFS, 
	and outside of periods of hospitalization and post-acute care (SNF), both of which are covered
	by bundled payments under Part A and cannot be observed in Part D claims data.

Program: 5_tutorial_post_processing_data_concatenation

	Description: This code concatenates the 100 output datasets generated from the parallel
	processing in Programs 3 and 4, producing a final dataset of drug observable Nursing Home time. 

Programmer: Adam DAmico

Date: 19Dec2024

Version History:
*/

/*Set location to save log file*/
proc printto log="P:\nhddi\a5d\Measurable_drug_time_in_NH_tutorial\output\Publication_version\Program_5_log_file_&sysdate..log" new;
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


/*STEP 1: COMBINE ALL DATASETS GENERATED FROM THE PARALLEL PROCESSING*/

%macro combine(x,y);
*x refers to the prefix designating the datasets/variables as admission-anchored (adm) or entry-anchored (ent);
*y refers to the longer word "admission" and "entry" used in generating variable labels;

*Concatenate the individual beneficiary files into a single omnibus dataset;
data partout.&x.DONH_eps_all;
	set 
partout.&x.DONH_eps_1	partout.&x.DONH_eps_2	partout.&x.DONH_eps_3	partout.&x.DONH_eps_4	partout.&x.DONH_eps_5	partout.&x.DONH_eps_6	partout.&x.DONH_eps_7	partout.&x.DONH_eps_8	partout.&x.DONH_eps_9	partout.&x.DONH_eps_10
partout.&x.DONH_eps_11	partout.&x.DONH_eps_12	partout.&x.DONH_eps_13	partout.&x.DONH_eps_14	partout.&x.DONH_eps_15	partout.&x.DONH_eps_16	partout.&x.DONH_eps_17	partout.&x.DONH_eps_18	partout.&x.DONH_eps_19	partout.&x.DONH_eps_20
partout.&x.DONH_eps_21	partout.&x.DONH_eps_22	partout.&x.DONH_eps_23	partout.&x.DONH_eps_24	partout.&x.DONH_eps_25	partout.&x.DONH_eps_26	partout.&x.DONH_eps_27	partout.&x.DONH_eps_28	partout.&x.DONH_eps_29	partout.&x.DONH_eps_30
partout.&x.DONH_eps_31	partout.&x.DONH_eps_32	partout.&x.DONH_eps_33	partout.&x.DONH_eps_34	partout.&x.DONH_eps_35	partout.&x.DONH_eps_36	partout.&x.DONH_eps_37	partout.&x.DONH_eps_38	partout.&x.DONH_eps_39	partout.&x.DONH_eps_40
partout.&x.DONH_eps_41	partout.&x.DONH_eps_42	partout.&x.DONH_eps_43	partout.&x.DONH_eps_44	partout.&x.DONH_eps_45	partout.&x.DONH_eps_46	partout.&x.DONH_eps_47	partout.&x.DONH_eps_48	partout.&x.DONH_eps_49	partout.&x.DONH_eps_50
partout.&x.DONH_eps_51	partout.&x.DONH_eps_52	partout.&x.DONH_eps_53	partout.&x.DONH_eps_54	partout.&x.DONH_eps_55	partout.&x.DONH_eps_56	partout.&x.DONH_eps_57	partout.&x.DONH_eps_58	partout.&x.DONH_eps_59	partout.&x.DONH_eps_60
partout.&x.DONH_eps_61	partout.&x.DONH_eps_62	partout.&x.DONH_eps_63	partout.&x.DONH_eps_64	partout.&x.DONH_eps_65	partout.&x.DONH_eps_66	partout.&x.DONH_eps_67	partout.&x.DONH_eps_68	partout.&x.DONH_eps_69	partout.&x.DONH_eps_70
partout.&x.DONH_eps_71	partout.&x.DONH_eps_72	partout.&x.DONH_eps_73	partout.&x.DONH_eps_74	partout.&x.DONH_eps_75	partout.&x.DONH_eps_76	partout.&x.DONH_eps_77	partout.&x.DONH_eps_78	partout.&x.DONH_eps_79	partout.&x.DONH_eps_80
partout.&x.DONH_eps_81	partout.&x.DONH_eps_82	partout.&x.DONH_eps_83	partout.&x.DONH_eps_84	partout.&x.DONH_eps_85	partout.&x.DONH_eps_86	partout.&x.DONH_eps_87	partout.&x.DONH_eps_88	partout.&x.DONH_eps_89	partout.&x.DONH_eps_90
partout.&x.DONH_eps_91	partout.&x.DONH_eps_92	partout.&x.DONH_eps_93	partout.&x.DONH_eps_94	partout.&x.DONH_eps_95	
partout.&x.DONH_eps_96	partout.&x.DONH_eps_97	partout.&x.DONH_eps_98	partout.&x.DONH_eps_99	partout.&x.DONH_eps_100;
run; 

*Sort the dataset by beneficiary ID, NH episode ID, and drug observable NH episode start date;
proc sort data=partout.&x.DONH_eps_all;
	by bene_id_18900 &x.NH_id &x.DONH_start;
run;

%mend combine;

options mprint;

*Run the macro for the admission-anchored drug observable NH time dataset 
AND the entry-anchored drug observable NH time dataset;
%combine(x=adm, y=admission);
%combine(x=ent, y=entry);

/*STEP 2: DROP IRRELEVANT VARIABLES AND SAVE FINAL VERSIONS OF THE DATASETS*/

	*Admission-anchored NH episode dataset;
	data final.admiss_anchored_NHeps_v&version.; set mdstime.admiss_anchored_NHeps_nearfinal;
		drop 
			admNH_return_anticipated  /*Not relevant to final dataset*/
			admNH_id 				  /*Drop temporary id variable, as it is not compatible across versions. Records are uniquely identifiable by bene_id_18900 and entry date*/
			admNH_admiss_assess_date; /*Can be pulled in from MDS admission assessment via admNH_admiss_ID variable - presence in final dataset may confuse some users*/
	run;


	*Admission-anchored drug observable NH episode dataset;
	data final.adm_drug_obs_NHeps_v&version.; set partout.admDONH_eps_all;
		label 
			admDONH_end="Admission-Anchored Drug Observable NH Episode: end date"
			admDONH_start="Admission-Anchored Drug Observable NH Episode: start date"
			enroll_end="Enrollment episode: Last Date of current episode of continuous Medicare enrollment (FFS Parts A, B and D, no Medicare Advantage)"
			enroll_start="Enrollment episode: First Date of current episode of continuous Medicare enrollment (FFS Parts A, B and D, no Medicare Advantage)";
		drop
			admNH_return_anticipated  /*Not relevant to final dataset*/
			partition_num			  /*Not relevant to final dataset*/
			admNH_enrollcat			  /*Not relevant to final dataset*/
			enroll_id			  	  /*Drop temporary enrollment id variable*/
			admNH_id 				  /*Drop temporary id variable, as it is not compatible across versions. Records are uniquely identifiable by bene_id_18900 and entry date*/
			admNH_admiss_assess_date; /*Can be pulled in from MDS admission assessment via admNH_admiss_ID variable - presence in final dataset may confuse some users*/
	run;


	*Entry-anchored NH episode dataset;
	data final.entry_anchored_NHeps_v&version.; set mdstime.entry_anchored_NHeps_nearfinal;
		drop 
			entNH_return_anticipated  	/*Not relevant to final dataset*/
			entNH_id 	   				/*Drop temporary id variable, as it is not compatible across versions. Records are uniquely identifiable by bene_id_18900 and entry date*/
			entNH_admiss_id 			/*Drop all admission assessment related variables, as they are better attained from admission-anchored datasets*/
			entNH_admiss_assess_date; 	/*Drop all admission assessment related variables, as they are better attained from admission-anchored datasets*/
	run;

	*Entry-anchored drug observable NH episode dataset;
	data final.ent_drug_obs_NHeps_v&version.; set partout.entDONH_eps_all;
		label 
			entDONH_end="Entry-Anchored Drug Observable NH Episode: end date"
			entDONH_start="Entry-Anchored Drug Observable NH Episode: start date"
			enroll_end="Enrollment episode: Last Date of current episode of continuous Medicare enrollment (FFS Parts A, B and D, no Medicare Advantage)"
			enroll_start="Enrollment episode: First Date of current episode of continuous Medicare enrollment (FFS Parts A, B and D, no Medicare Advantage)";
		drop
			entNH_return_anticipated  	/*Not relevant to final dataset*/
			partition_num			  	/*Not relevant to final dataset*/
			entNH_enrollcat			  	/*Not relevant to final dataset*/
			enroll_id			  	  	/*Drop temporary enrollment id variable*/
			entNH_id 	   			  	/*Drop temporary id variable, as it is not compatible across versions. Records are uniquely identifiable by bene_id_18900 and entry date*/
			entNH_admiss_id 			/*Drop all admission assessment related variables, as they are better attained from admission-anchored datasets*/
			entNH_admiss_assess_date; 	/*Drop all admission assessment related variables, as they are better attained from admission-anchored datasets*/
	run;

/*END OF PROGRAM*/
