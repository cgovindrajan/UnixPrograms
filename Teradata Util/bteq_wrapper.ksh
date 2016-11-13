#!/bin/ksh
export PATH=$PATH:`dirname $0`
echo $PATH
export BIN_DIR=`dirname $0`
typeset -i BATCH_ID
typeset -i WF_ID
typeset -i CYCLE_ID
####################################################################
# Step:0 Call Environment file                                     #
####################################################################
if [[ ! -f ${BIN_DIR}/edw_scripts_set_env.ksh ]]
then
  echo "[ERROR]:[`date`]:Environment File Missing"
  exit 1
else 
. ${BIN_DIR}/edw_scripts_set_env.ksh
fi
echo ${EDW_FH_LOG}
####################################################################
# Step:1 Execute  Functions                                        #
####################################################################
if [[ ! -f ${BIN_DIR}/edw_script_func_env.ksh ]]
then
  echo "[ERROR]:[`date`]:Functions File Missing"
  exit 1
else
. ${BIN_DIR}/edw_script_func_env.ksh
fi
####################################################################
# Step:2 Assign Variables Values                                   #
####################################################################
export SCRIPT_NAME=$0
export WF_TIER=$1
export CTL_NM=$2
export WF_NAME=$3
####################################################################
# Step:3 Validating the Input Variable Values                      #
####################################################################
export INPUT[0]=$WF_TIER
export INPUT[1]=$CTL_NM

i=0;
export ERROR=0;

while (( $i < ${#INPUT[@]} )); do

if [[ -n $INPUT[$i] ]] then
     let i=$i+1
      continue
fi
        export ERROR=1
        export INPUT[$i]="UNKNOWN"
        let i=$i+1

done

if [[ $ERROR -eq 1  ]] then
_log "[ERROR]:Input Parameters supplied are not correct:- WF_TIER  : ${INPUT[0]}, CTL_NM  : ${INPUT[1]}"
        WF_FAILED_OBJ_NAME="[ERROR]:Input Parameters supplied are not correct:- WF_TIER  :  ${INPUT[0]}, CTL_NM  : ${INPUT[1]}"
        export x=-99
        _exit $x
fi

####################################################################
# Step:4 Touch Log File                                            #
####################################################################
export LOG_FILE_NM=${EDW_FH_LOG}/edw_script_bteq_wrapper_${CTL_NM}.log
touch $LOG_FILE_NM
if [[ $? -eq 0 ]] then
_log "[INFO]:Touch File has been created successfully"
else
echo "[ERROR]:Failed to Create Log File"
WF_FAILED_OBJ_NAME="[ERROR]:Failed to Create Log File"
        export x=-1
        exit $x
fi
_log "[INFO]:Script:$SCRIPT_NAME is started "
_log "[INFO]:WF Tier:$WF_TIER and Script Name :$CTL_NM"
_log "[INFO]:$LOG_FILE_NM: Log File is found"

####################################################################
# Step:6 Export Teradata Variables                                 #
####################################################################
export LOGONFILE="$EDW_BTEQ_LOGON_SCRIPT_PATH/$LOGON_FILE" 
####################################################################
# Step:Get Cycle ID for the Tier                                   #
####################################################################
export EXPORTFILE="${EDW_FH_LOG}/bteq_${CTL_NM}.txt"
_bteq ${LOGONFILE} ${EDW_VIEW_DB_USER} "
.os rm ${EXPORTFILE}
.EXPORT REPORT FILE = ${EXPORTFILE}
SEL TRIM(MAX(CYCLE_ID))||'|'||TRIM(MAX(RUN_ID)) (TITLE '') FROM ETL_CYCLE WHERE  TIER_ID='${WF_TIER}';
.EXPORT RESET
" "Info: Exporting Cycle ID successful"
_log "[INFO]:${EXPORTFILE} Export is successful"

CYCLE_ID=`grep -v $^ ${EXPORTFILE}|cut -f1 -d '|'`
RUN_ID=`grep -v $^ ${EXPORTFILE}|cut -f2 -d '|'`
_log "[INFO]:CYCLE_ID=${CYCLE_ID}"
####################################################################
#Step:7 Genarate Batch_id and update Details to ETL_WORKFLOWS Table #
####################################################################
_bteq ${LOGONFILE} ${EDW_VIEW_DB_USER} "INSERT INTO ETL_WORKFLOWS
(CYCLE_ID,
BATCH_ID,
FILE_BATCH_ID,
WF_ID,
WF_NAME,
RUN_ID,
WF_START_DATE,
WF_STATUS,
WF_STATUS_REASON,
WF_STATUS_CHG_DATE
)
SELECT
'${CYCLE_ID}' AS CYCLE_ID,
COALESCE(MAX(BATCH_ID), 0) +1,
'0' AS FILE_BATCH_ID,
(SELECT WF_ID FROM AC_WORKFLOWS WHERE WF_NAME='${CTL_NM}') AS WF_ID,
'${CTL_NM}' AS WF_NAME,
'${RUN_ID}' as RUN_ID,
CURRENT_TIMESTAMP(0) AS WF_START_DATE,
'RUNNING' AS WF_STATUS,
'INITIATED' AS WF_STATUS_REASON,
CURRENT_TIMESTAMP(0) AS WF_STATUS_CHG_DATE
from ETL_WORKFLOWS;" "Info: Batch_id generation for script is successfull and updated the script details to ETL_WORKFLOWS"
#######################################################################
#Step:8 Export Load Variable Values  to file                          #
#######################################################################
export EXPORTFILE="${EDW_FH_LOG}/bteq_${CTL_NM}.txt"
_bteq ${LOGONFILE} ${EDW_VIEW_DB_USER} "
.os rm ${EXPORTFILE}
.EXPORT REPORT FILE = ${EXPORTFILE}
SEL TRIM(BATCH_ID)||'|'||TRIM(WF_ID)||'|'||TRIM(CYCLE_ID)||'|'||TRIM(CYCLE_GROUP_ID)  (TITLE '') FROM
(
SELECT 
EW.BATCH_ID,
EW.WF_ID,
EW.CYCLE_ID,
AW.CYCLE_GROUP_ID
FROM 
AC_WORKFLOWS AW 
INNER JOIN ETL_WORKFLOWS  EW
ON AW.WF_ID = EW.WF_ID
INNER JOIN
ETL_CYCLE EC
ON AW.WF_TIER=EC.TIER_ID
WHERE
AW.WF_NAME = TRIM('${CTL_NM}')
AND
EW.CYCLE_ID='${CYCLE_ID}'
AND
EW.BATCH_ID = (SELECT MAX(BATCH_ID) FROM ETL_WORKFLOWS WHERE CYCLE_ID ='${CYCLE_ID}' AND WF_NAME ='${CTL_NM}')
) a ;  
.EXPORT RESET
" "Info: Exporting Variable values are successful"
_log "[INFO]:${EXPORTFILE} Export is successful"
#######################################################################
#Extract Variable Values from Exported File ${EXPORTFILE} to Variables#
#######################################################################
export BATCH_ID=`cut -f 1 -d '|' ${EXPORTFILE}|head -1`
export WF_ID=`cut -f 2 -d '|' ${EXPORTFILE}|head -1`
export CYCLE_ID=`cut -f 3 -d '|' ${EXPORTFILE}|head -1`
export CYCLE_GROUP_ID=`cut -f 4 -d '|' ${EXPORTFILE}|head -1`
#######################################################################
#       Write Variable Information to Log                             #
#######################################################################
_log "[INFO]:Export Variable values is successful"
_log "[INFO]:Variable Values"
_log "[INFO]:BATCH_ID:$BATCH_ID"
_log "[INFO]:WF_ID:$WF_ID"
_log "[INFO]:CYCLE_ID:$CYCLE_ID"
_log "[INFO]:CYCLE_GROUP_ID:$CYCLE_GROUP_ID"
#######################################################################
#       Call the CTL to load the target table                         #
#######################################################################
ksh ${CTL_DIR}/${CTL_NM} ${WF_NAME}
RCWF=$?
if [[ $RCWF -ne 0 ]] then
_log "[ERROR]:Script:$CTL_NM:Execution Failed"
WF_FAILED_OBJ_NAME="[ERROR]:Script:$CTL_NM:Execution Failed"
_exit $RCWF
else 
_log "[INFO]:Script:$CTL_NM:Execution Succeeded"
fi
#######################################################################
#   Update Workflow Status to ETL_WORKFLOWS                           #
#######################################################################
_log "[INFO]:Updating the Workflow Audit"

_bteq ${LOGONFILE} ${EDW_VIEW_DB_USER} "
UPDATE ETL_WORKFLOWS
SET WF_STATUS='Succeeded',
WF_STATUS_REASON='Succeeded',
WF_STATUS_CHG_DATE=CURRENT_TIMESTAMP(0),
WF_FAILED_OBJ_NAME='NA',
WF_FAILED_OBJ_STS_CD='0',
WF_DURATION=SUBSTR(CAST((CURRENT_TIMESTAMP(0) - WF_START_DATE HOUR(4) TO SECOND) AS CHAR(18)),1,5) * 3600 +SUBSTR(CAST((CURRENT_TIMESTAMP(0) - WF_START_DATE HOUR(4) TO SECOND) AS CHAR(18)),7,2) * 60 + SUBSTR(CAST((CURRENT_TIMESTAMP(0) - WF_START_DATE HOUR(4) TO SECOND) AS CHAR(18)),10), 
WF_END_DATE=CURRENT_TIMESTAMP(0)
where BATCH_ID=$BATCH_ID AND CYCLE_ID=$CYCLE_ID AND WF_ID=(select WF_ID from AC_WORKFLOWS  where WF_NAME='${CTL_NM}');
" "[INFO]:Workflow Audit Successful" 

#######################################################################
#  Insert Delta in ETL_DELTA_EXTRACTION"                              #
#######################################################################
_log "[INFO]:Updating the Delta in ETL_DELTA_EXTRACTION"

_bteq ${LOGONFILE} ${EDW_VIEW_DB_USER} "
INSERT INTO ETL_DELTA_EXTRACTION
(
CYCLE_ID
,BATCH_ID
,WF_ID
,SESSION_ID
,EXTRACTION_FROM_DT
,EXTRACTION_TO_DT
)
SELECT
${CYCLE_ID}
,${BATCH_ID}
,(select WF_ID from AC_WORKFLOWS  where WF_NAME='${CTL_NM}') AS WF_ID
,(select SESSION_ID from AC_SESSIONS  where SESSION_NAME='${CTL_NM}') AS SESSION_ID
,${BTEQ_END_DATE} AS EXTRACTION_FROM_DT
,CURRENT_DATE EXTRACTION_TO_DT;
" "[INFO] :Delta insertion Successful"
#######################################################################
#   Rename Logfile                                                    #
#######################################################################
LOG_FILE_NM_FNL="${EDW_FH_LOG}/edw_script_bteq_wrapper_${CTL_NM}_${BATCH_ID}.log"
if [[ -f $LOG_FILE_NM ]] then
mv $LOG_FILE_NM $LOG_FILE_NM_FNL
if [[ $? -ne 0 ]] then
_log "[ERROR]:LOG_FILE_NM:$LOG_FILE_NM_FNL:Creation Failed"
 WF_FAILED_OBJ_NAME="[ERROR]:LOG_FILE_NM:$LOG_FILE_NM_FNL:Creation Failed"
_exit -1
fi
export LOG_FILE_NM=$LOG_FILE_NM_FNL
else
_log "[ERROR]:$LOG_FILE_NM:Is not Found"
WF_FAILED_OBJ_NAME="[ERROR]:$LOG_FILE_NM:Is not Found"
 export x=-1
 _exit $x
fi
_log "[INFO]:Log file name :${LOG_FILE_NM_FNL}"
#######################################################################
#    Exit Script for Successful Completion                            #
#######################################################################
_log "[INFO]:$CTL_NM:Variable Info After File handling"
_log "[INFO]:BATCH_ID:$BATCH_ID"
_log "[INFO]:CTL_NM:$CTL_NM" 
_log "[INFO]:LOG_FILE_NM:$LOG_FILE_NM_FNL"
_log "[INFO]:###########################################"
_log "[INFO]:$CTL_NM:$SCRIPT_NAME:Completed Successfully"
_log "[INFO]:###########################################"
exit 0
