/* Problem appears to be how a conflict is resolved between two matching records that both have an attempt indicator set for the same subject. 
    Maybe with how the Delete_list_ macro variable is set...
*/

%macro ComplementaryMerge(mergespec =,
DS = , 
DS_out=,
force_one_only = no, 
similarity_measure =,
mode=);
    %if (&DS eq) %then %do;
        %put ERROR: DS macro parameter cannot be blank;
        %abort;
    %end;
    %if %upcase(&mode) ne CREATE %then %do;
        %if %upcase(&mode) ne RUN  %then %do;
        %put ERROR: Mode macro parameter can have a value of CREATE or RUN. It cannot be blank;
        %abort;
        %end;
    %end;

    /* Checking for the existence of the input datasets*/
    %dsexist(&DS)
    /* End:Checking for the existence of the input datasets*/


    /* Creation of default mergespec file*/
     %if %upcase(&mode)=CREATE %then %do;

        %if %sysfunc(fileexist("&mergespec")) %then %do;
             %put ERROR: %upcase(&mergespec) already exists.Change the mode parameter to have a value of 'RUN';
             %abort;
         %end;

        Proc Contents data=&DS out=contents_DS(keep=Name) noprint;
        run;

        Data contents_DS(Keep=Variable_Name Keys Rule);
            retain Variable_Name Keys Resolution_Rule;
            set contents_DS;
            Length Keys Resolution_Rule $50.;
            Variable_Name=Name;
        run;

        Proc Export data= contents_DS
                    Outfile= "&MergeSpec" 
                    DBMS=EXCEL Replace;
             Sheet="file1"; 
        run;
      %end;
    /*End: Creation of default mergespec file*/
    /*Checking for the valid values of  macro parameters*/
    %if %upcase(&mode)=RUN %then %do;

         %if (&DS_out eq) %then %do;
            %put ERROR: DS_out macro parameter cannot be blank;
            %abort;
        %end;

        /* Reading the mergespec file*/

        %safeexcelread(filename=&MergeSpec,sheetname=file1,DS_out=mergespec_file, err_var_name=my_err);

        Proc Contents data=&DS out=contents_DS(keep=Name type) noprint;
        run;

        %let missing_vars_DS=;

        Proc Sql noprint;
            Select upcase(Name) into : missing_vars_DS separated by " "
            from contents_DS
            Where upcase(Name) not in (Select upcase(strip(Variable_Name)) from mergespec_file);
        quit;

         %if (&missing_vars_DS ne) %then %do;
            %put &missing_vars_DS is missing from the mergespec sheet file1;
            %abort;
        %end;

        Proc sort data=mergespec_file;
            by keys;
        run;

        Data mergespec_file(drop=tmp);
            set Mergespec_file;
            Resolution_Rule=strip(compress(upcase(resolution_rule)));
            Variable_Name=strip(compress(upcase(Variable_Name)));
            Keys=strip(compress(upcase(Keys)));

            retain tmp 0;
            fuzzy_count=0;
            by keys;
             if keys ne "" then do;
                if substr(strip(keys),1,length(strip(keys))-1) eq "FUZZY" then do;
                    if first.keys then tmp=1;
                    fuzzy_count=tmp;
                    tmp=tmp +1;
                    if last.keys then tmp=0;
                end;
            end;
        run;

        Proc sort data=mergespec_file;
            by keys fuzzy_count;
        run;

        %let recordpriorityvar=;
        Data mergespec_file;
            set mergespec_file;
            retain unit_count var_count missing_variable_name_count wrong_key_value_count wrong_rule_value_count 
                    PK_count SK_count FK_count sortkey_count  common_variables_count Variable_priority_count
                    record_priority_count 0;

            if variable_name ne "" then var_count=var_count + 1;
            else missing_variable_name_count=missing_variable_name_count + 1;

            if  resolution_rule not in ("OR" "RECORDPRIORITY" "VARIABLEPRIORITY" "RECORDPRIORITY,VARIABLEPRIORITY"  
                             "VARIABLEPRIORITY,RECORDPRIORITY" "COMMON" "COMMON,NON-MISSING") then do;
                if substr(resolution_rule,1,4) not in  ("TEST" "UNIT") then do;
                    wrong_rule_value_count=wrong_rule_value_count + 1;
                     call symput ("wrong_rule_value_"||strip(wrong_rule_value_count),variable_name);
                end;

                else if substr(resolution_rule,1,4) eq ("TEST")then do;
                    if anydigit(substr(resolution_rule,9,1))=0  or substr(resolution_rule,5,4) ne "UNIT" then do;
                        wrong_rule_value_count=wrong_rule_value_count + 1;
                        call symput ("wrong_rule_value_"||strip(wrong_rule_value_count),variable_name);
                    end;
                end;
                else if substr(resolution_rule,1,4) eq ("UNIT")then do;
                    if anydigit(substr(resolution_rule,5,1))=0  or substr(resolution_rule,6,9) ne "INDICATOR" then do;
                         wrong_rule_value_count=wrong_rule_value_count + 1;
                         call symput ("wrong_rule_value_"||strip(wrong_rule_value_count),variable_name);
                    end;
                    unit_count=unit_count + 1;
                     call symput("Var_unit_" || strip(substr(resolution_rule,5,1)),strip(Variable_Name));
                end;
            end;

            if Keys not in ("PRIMARY" "SECONDARY" "SORT" "") then do;
                if substr(Keys,1,5) ne "FUZZY" and anydigit(substr(Keys,6,1))=0 then do;
                 wrong_key_value_count=wrong_key_value_count+1;
                 call symput ("wrong_key_value_"||strip( wrong_key_value_count),variable_name);
                end;
            end;

            if resolution_rule in ("COMMON" "COMMON,NON-MISSING") then do;
                common_variables_count=common_variables_count +1;
                call symput ("common_variables_count",strip(common_variables_count));
            end;
            if resolution_rule in ("VARIABLEPRIORITY") then do;
                Variable_priority_count=Variable_priority_count +1;
                call symput ("Variable_priority_count",strip(Variable_priority_count));
            end;
             if resolution_rule in ("RECORDPRIORITY") then do;
                record_priority_count=record_priority_count +1;
                call symput ("record_priority_count",strip(record_priority_count));
            end;
                
                


             if keys ne "" then do;

             if keys eq "PRIMARY" then do;
                 PK_count=PK_count + 1;
                 call symput ("PK_var_"||strip(PK_count),variable_name);
                 
             end;

             if keys eq "SECONDARY" then do;
                 SK_count=SK_count + 1;
                 call symput ("SK_var_"||strip(SK_count),variable_name);
             end;


             if keys eq "SORT" then do;
                 sortkey_count=sortkey_count + 1;
                 call symput ("SortVar",variable_name);
                 call symput ("Sortkey_count",strip(Sortkey_count));
             end;

             if substr(keys,1,length(keys)-1) eq "FUZZY" then do;
                if Fuzzy_count=1 then do;
                 FK_count=FK_count + 1;
                 call symput ("FK_var1_"||strip(FK_count),variable_name);
                 call symput ("FK_sub_count" ||strip(FK_count),strip(fuzzy_count));
                end;

                if Fuzzy_count=2 then do;
                 call symput ("FK_var2_"||strip(FK_count),variable_name);
                 call symput ("FK_sub_count" ||strip(FK_count),strip(fuzzy_count));
                end;

             end;
            end;
            call symput("Var_name_" || strip(var_count),strip(Variable_Name));
            call symput("Var_rule_" || strip(var_count),strip(Resolution_Rule));

            if index(Resolution_Rule,"RECORDPRIORITY") then call symput ('Recordpriorityvar',Variable_Name);
            if index(Resolution_Rule,"VARIABLEPRIORITY") then call symput ('Variablepriorityvar',Variable_Name);
        
            Call symput ("unit_count",strip(unit_count));
            Call symput ("var_count",strip(var_count));
            call symput("missing_variable_name_count",strip(missing_variable_name_count));
            call symput("wrong_key_value_count",strip(wrong_key_value_count));
            call symput("wrong_rule_value_count",strip(wrong_rule_value_count));
            Call symput ("PK_count",strip(PK_count));
            Call symput ("SK_count",strip(SK_count));
            Call symput ("FK_count",strip(FK_count));

        run;
       

         %if  %symexist(SortKey_count) ne 1 %then %do;
            %put ERROR: Sort Key is missing.; 
            %abort;
        %end;
         %if (&SortKey_count ne 1)  %then %do;
            %put ERROR: There is more than one sort key; 
            %abort;
        %end;

        %let x=1;

        %do %while(%scan(&similarity_measure,&x," ") ne);
            %let x=%eval(&x + 1);
        %end;

        %if &FK_count ne 0 %then %do;
            %if %eval(&x -1) ne &FK_count %then %do;
                %put ERROR: The number of similarity measures and the number of fuzzy keys are not equal.;
                 %abort;
            %end;
        %end;
        %if %symexist(common_variables_count) %then %do;
                %if  %symexist(Variable_priority_count) ne 1 %then %do;
                    %put ERROR: A variable should be specified as "VARIABLE PRIORITY" to resolve common variables;
                    %abort;
                %end;

                %if &Variable_priority_count ne 1  %then %do;
                    %put ERROR:More than one variable has been speified as "VARIABLE PRIORITY";
                     %abort;
                %end;
            
        %end;

        %if %upcase(&force_one_only) eq YES %then %do;
                %if  %symexist(record_priority_count) ne 1 %then %do;
                    %put ERROR: A variable should be specified as "RECORD PRIORITY" ,if force_one_only option is "YES";
                    %abort;
                %end;

                %if &record_priority_count ne 1  %then %do;
                    %put ERROR:More than one variable has been speified as "RECORD PRIORITY";
                     %abort;
                %end;
            
        %end;


         %if (&missing_variable_name_count ne 0) %then %do;
            %put ERROR: Variable_Name cannot be missing.; 
            %abort;
        %end;

        %if (&wrong_key_value_count ne 0) %then %do;
            %put ERROR: There are &wrong_key_value_count wrong key values. The key can have values 
                    PRIMARY,SECONDARY,SORT or FUZZY<N>(where N can be a number);Check the following variable names
            %do i=1 %to &wrong_key_value_count;
                %put  &&wrong_key_value_&i;
            %end;
            %abort;
        %end;

         %if (&wrong_rule_value_count ne 0) %then %do;
            %put ERROR: There are &wrong_rule_value_count. rule values. The rule can have values 
                    VARIABLE PRIORITY| RECORD PRIORITY|COMMON|COMMON,NON-MISSING| OR | TEST UNIT<N> | UNIT <N> INDICATOR.
                Check the following variable names;
            %do i=1 %to &wrong_rule_value_count;
                %put  &&wrong_rule_value_&i;
            %end;
            %abort;
        %end;
        Data _null_;
            set contents_ds;
            retain type_error_count 0;
            %do i=1 %to &var_count;
                 %if (%quote(&&Var_rule_&i) eq %str(OR))or  (%quote(&&Var_rule_&i) eq VARIABLEPRIORITY) or 
                 (%quote(&&Var_rule_&i) eq RECORDPRIORITY) or  (%quote(&&Var_rule_&i) eq %quote(VARIABLEPRIORITY,RECORDPRIORITY)) or
                 (%quote(&&Var_rule_&i) eq %quote(RECORDPRIORITY,VARIABLEPRIORITY)) %then %do;
                 if upcase(Name)="&&Var_name_&i" then do;
                    if type ne 1 then do;
                        Type_error_count=Type_error_count + 1;
                        call symput("type_error_"||strip(type_error_count),upcase(Name));
                    end;
                end;
                %end;
            %end;
            Call symput ("type_error_count",strip(type_error_count));
        run;

         %if (&type_error_count ne 0) %then %do;
            %put ERROR: There are type_error_count. variables which should be numeric in the data . The 
                    VARIABLE PRIORITY| RECORD PRIORITY| OR | variables should be numeric.
                Check the following variable names;
            %do i=1 %to &type_error_count;
                %put  &&type_error_&i;
            %end;
            %abort;
        %end;

     Proc Sort data=&ds. out=tmp_&ds. dupout=dup_&ds. nodupkey;
        by &sortvar;
     run;

    %let dsid = %sysfunc(open(dup_&ds.));
    %let ndups = %sysfunc(attrn(&dsid,nobs)); 
    %let rc = %sysfunc(close(&dsid));
    %if &ndups ne 0 %then %do;
        %put ERROR: The sort variable is not unique in the input dataset.;
        %abort;
    %end;



    %let Resolved_count=1;
    %let x=1;
    %let count_resolved_pairs=0;
    %do %while(&Resolved_count ne 0);
        Proc Sql noprint;
            Create table compmerge&x as
                Select %do i=1 %to &var_count;
                    A.&&var_name_&i as tmp1&&var_name_&i,B.&&var_name_&i as tmp2&&var_name_&i,
                    %end;
                    "" as tmp
                from &ds. A inner join &DS B on ((A.&sortvar ne B.&sortvar) and 
                                                  %do i=1 %to &PK_count;
                                                  (A.&&PK_var_&i = B.&&PK_var_&i
                                                  and missing(A.&&PK_var_&i) =0 and missing(B.&&PK_var_&i) =0) and
                                                  %end; 
                                                  %do i=1 %to &SK_count;
                                                  (A.&&SK_var_&i = B.&&SK_var_&i
                                                  or missing(A.&&SK_var_&i) =1 or missing(B.&&sK_var_&i) =1) and
                                                  %end; 
                                                  A.&sortvar lt  B.&sortvar)
                                                  order by A.&sortvar,B.&sortvar;
        quit;
        %let count_pairs=;
    %let dsid = %sysfunc(open(compmerge&x));
    %let nobs = %sysfunc(attrn(&dsid,nobs)); 
    %let rc = %sysfunc(close(&dsid));
    %if &nobs eq 0 %then %let resolved_count=0;
    %else %do; 
        Data compmerge&x(drop=fk_flag count_pairs);
            set compmerge&x;
            retain count_pairs 0;
            FK_flag=1;
            %do i=1 %to &FK_count;
                if %do j=1 %to &&FK_sub_count&i; 
                    missing(tmp1&&FK_var&j._&i)=0 or
                    %end;
                    tmp ne "" then do;
                    if %do j=1 %to &&FK_sub_count&i; 
                    missing(tmp2&&FK_var&j._&i)=0 or
                    %end;
                    tmp ne "" then do;

                    %if &&FK_sub_count&i=1 %then %do;
                        tmpstring1=tmp1&&FK_var1_&i;
                        tmpstring2=tmp2&&FK_var1_&i;
                        %StringSimilarity(String1 = strip(compbl(tmpstring1)), String2 = strip(compbl(tmpstring2)),
                              SimilarityIndex = tmpsim&i, MaxStringLength = tmplen1);
                    %end;
                    
                     %if &&FK_sub_count&i=2 %then %do;
                        tmpstring1=strip(tmp1&&FK_var1_&i) || strip(tmp1&&FK_var2_&i);
                        tmpstring2=strip(tmp2&&FK_var1_&i) || strip(tmp2&&FK_var2_&i);
                        %StringSimilarity(String1 = strip(compbl(tmpstring1)), String2 = strip(compbl(tmpstring2)),
                              SimilarityIndex = tmpsimA, MaxStringLength = tmplen1);
                        tmpstring2=strip(tmp2&&FK_var2_&i) || strip(tmp2&&FK_var1_&i);
                        %StringSimilarity(String1 = strip(compbl(tmpstring1)), String2 = strip(compbl(tmpstring2)),
                              SimilarityIndex = tmpsimB, MaxStringLength = tmplen2);
                        tmpstring1=strip(tmp1&&FK_var2_&i) || strip(tmp1&&FK_var1_&i);
                         tmpstring2=strip(tmp2&&FK_var1_&i) || strip(tmp2&&FK_var2_&i);
                        %StringSimilarity(String1 = strip(compbl(tmpstring1)), String2 = strip(compbl(tmpstring2)),
                              SimilarityIndex = tmpsimC, MaxStringLength = tmplen3);
                        tmpstring2=strip(tmp2&&FK_var2_&i) || strip(tmp2&&FK_var1_&i);
                        %StringSimilarity(String1 = strip(compbl(tmpstring1)), String2 = strip(compbl(tmpstring2)),
                              SimilarityIndex = tmpsimD, MaxStringLength = tmplen4);

                            tmpsim&i=max(tmpsimA,tmpsimB,tmpsimC,tmpsimD);
                      %end;
                          if tmpsim&i lt %scan(&similarity_measure,&i," ") then do;
                            FK_Flag=0;
                          end;
                    
                    end;
                end;
            %end;
            if FK_Flag=1;
            count_pairs=count_pairs + 1;
            call symput("sortvar1_" || strip(count_pairs),strip(tmp1&sortvar));
            call symput("sortvar2_" || strip(count_pairs),strip(tmp2&sortvar));
            call symput("count_pairs" ,strip(count_pairs));

        run;

         %if &x eq 1 %then %do;
          Data &ds_out DS_complimentary;
            set &Ds;
            if strip(&sortvar) in (  %do i=1 %to &count_pairs; "&&sortvar1_&i" "&&sortvar2_&i" %end;)
            then output DS_complimentary;
            else output &ds_out;
         run;
        %end;
                      
        %let delete_list_count=;

        Data Resolved_set&x(drop=tmp: delete_count resolved_count resolved_list resolved_flag);
            set compmerge&x;
            tmp="";
            length resolved_list $ 5000;
            retain resolved_list "*";
            retain delete_count  resolved_count 0;
            if index(Resolved_list,'*' || strip(tmp1&sortvar) || '*') >0 then delete;
            Resolved_flag=0;
           
            %if (&recordpriorityvar ne) %then %do;
                if tmp1&recordpriorityvar ge 1000 or tmp2&recordpriorityvar ge 1000 then do;
                    if tmp2&recordpriorityvar >  tmp1&recordpriorityvar then do;
                        %do i=1 %to &var_count;
                        &&Var_name_&i=tmp2&&Var_name_&i;
                        %end;
                    end;
                    else do;
                        %do i=1 %to &var_count;
                        &&Var_name_&i=tmp1&&Var_name_&i;
                        %end;
                    end;
                    Resolved_count=Resolved_count + 1;
                    Resolved_flag=1;
                    delete_count=delete_count+1;
                    call symput ("Delete_list_"||strip(delete_count),strip(tmp1&sortvar.));
                    delete_count=delete_count+1;
                    call symput ("Delete_list_"||strip(delete_count),strip(tmp2&sortvar.));
                    output;
                end;
            %end;
            if resolved_flag=0 then do;
                if %do i=1 %to &unit_count; (tmp1&&Var_unit_&i=1 and tmp2&&Var_unit_&i=1) or %end; tmp ne "" then do;
                     %if %upcase(&force_one_only)=YES %then %do;

                         if tmp2&recordpriorityvar >  tmp1&recordpriorityvar then do;
                            %do i=1 %to &var_count;
                            &&Var_name_&i=tmp2&&Var_name_&i;
                            %end;
                        end;
                        else do;
                            %do i=1 %to &var_count;
                            &&Var_name_&i=tmp1&&Var_name_&i;
                            %end;
                        end;
                    Resolved_count=Resolved_count + 1;
                    Resolved_flag=1;
                    delete_count=delete_count+1;
                    call symput ("Delete_list_"||strip(delete_count),strip(tmp1&sortvar.));
                    delete_count=delete_count+1;
                    call symput ("Delete_list_"||strip(delete_count),strip(tmp2&sortvar.));
                    output;
                    %end;
                end;
                else do;
                    /* I think the problem is in this block. Never checks if both have the same attempt indicator. */
               
                    %do i=1 %to &var_count;
               
                        %if (%quote(&&Var_rule_&i) eq %str(OR)) %then %do;
                             if tmp1&&Var_name_&i=1 or tmp2&&Var_name_&i=1 then do;
                                &&Var_name_&i=1;
                             end;
                             else  &&Var_name_&i=0;
                        %end;
                        %else %if (%quote(&&Var_rule_&i) eq COMMON) or  (%quote(&&Var_rule_&i) eq VARIABLEPRIORITY) or 
                               (%quote(&&Var_rule_&i) eq RECORDPRIORITY) or  (%quote(&&Var_rule_&i) eq %quote(VARIABLEPRIORITY,RECORDPRIORITY)) or
                              (%quote(&&Var_rule_&i) eq %quote(RECORDPRIORITY,VARIABLEPRIORITY)) %then %do;
                             if tmp2&variablepriorityvar > tmp1&variablepriorityvar then
                                &&Var_name_&i=tmp2&&Var_name_&i;
                             else &&Var_name_&i=tmp1&&Var_name_&i;
                        %end;
                        %else %if (%quote(&&Var_rule_&i) eq %quote(COMMON,NON-MISSING)) %then %do;
                            if missing(tmp2&&Var_name_&i)=1 then  &&Var_name_&i=tmp1&&Var_name_&i;
                            if missing(tmp1&&Var_name_&i)=1 then  &&Var_name_&i=tmp2&&Var_name_&i;
                            if missing(tmp2&&Var_name_&i)=0 and  missing(tmp1&&Var_name_&i)=0 then do;
                                if tmp2&variablepriorityvar > tmp1&variablepriorityvar then &&Var_name_&i=tmp2&&Var_name_&i;
                                else &&Var_name_&i=tmp1&&Var_name_&i;
                            end;
                        %end;
                        %else %if %substr(%quote(&&Var_rule_&i),1,8)=TESTUNIT %then %do;
                            %let tmpunitnumber=%substr(&&Var_rule_&i,9,1);
                            if tmp2&&Var_unit_&tmpunitnumber=1 then &&Var_name_&i=tmp2&&Var_name_&i;
                            else &&Var_name_&i=tmp1&&Var_name_&i;
                        %end;
                        %else %if %substr(%quote(&&Var_rule_&i),1,4)=UNIT %then %do;
                            %let tmpunitnumber=%substr(%quote(&&Var_rule_&i),5,1);
                            if tmp2&&Var_unit_&tmpunitnumber=1 then &&Var_name_&i=tmp2&&Var_name_&i;
                            else &&Var_name_&i=tmp1&&Var_name_&i;
                        %end;
                    %end;
                     delete_count=delete_count + 1;
                     call symput ("Delete_list_"||strip(delete_count),strip(tmp1&sortvar.));
                     delete_count=delete_count + 1;
                     call symput ("Delete_list_"||strip(delete_count),strip(tmp2&sortvar.));
                     Resolved_count=Resolved_count + 1;
                     Resolved_flag=1;
                    output;
                end;
            end;
            if resolved_flag=1 then Resolved_list= strip(Resolved_list)||"*" || strip(tmp1&sortvar.) ||"*" || strip(tmp2&sortvar.)||"*" ;
            call symput ("Resolved_count",strip(Resolved_count));
            call symput ("delete_count",strip(delete_count));
        run;

         Data DS_complimentary;
            set DS_complimentary;
            %if (&delete_count ne 0) %then %do;
             if strip(&sortvar) in (  %do i=1 %to &delete_count; "&&Delete_list_&i" %end;)
            then delete;
            %end;
        run;

        Proc Append base=DS_complimentary data=Resolved_set&x force;
        run;

        %if &x eq 1 %then %let ds=Ds_complimentary;
    %end;
        %let x=%eval(&x + 1);
%end;

Proc Append base=&ds_out. data=&ds force;
run;
Proc datasets lib=work nolist;
    delete %do i=1 %to %eval(&x - 1); Resolved_Set&i Compmerge&i %end; ds_complimentary contents_ds mergespec_file dup_&ds.;
quit;

%end;


%mend;

%macro dsexist(ds);
    %if %sysfunc(exist(&ds))= 0 %then %do;
        %put ERROR: %upcase(&ds) DOES NOT EXIST;
        %abort;
    %end;
%mend;
