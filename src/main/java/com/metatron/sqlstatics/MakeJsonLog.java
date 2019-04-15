package com.metatron.sqlstatics;

import org.json.simple.JSONObject;;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;

import java.util.ArrayList;
import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;
import java.util.List;

public class MakeJsonLog {

    public void makeSample() {
        FileWriter writer=null;

         try {
             SQLConfiguration sqlConfiguration = new SQLConfiguration();

             File file = new File(sqlConfiguration.get("input_filename"));
             writer = new FileWriter(file,true);

             //make jsonString
             for(String query : getORACLEQueryList()) {
                 JSONObject log = new JSONObject();

                 log.put("hostname","localhost");
                 log.put("createTime",System.currentTimeMillis());
                 log.put("sqlId",UUID.randomUUID().toString());
                 log.put("engineType",sqlConfiguration.get("engineType"));
                 log.put("sql",query);

                 writer.write(log.toString());
                 writer.write("\n");

                 writer.flush();

             }

         }catch(Exception e){

         }finally {
             try {
                 if(writer != null) {
                     writer.close();
                 }

             }catch(IOException e){
                 System.out.println(e);
             }
         }
    }

    //Input File Read
    public ArrayList<JSONObject> readJsonFile(){

        BufferedReader br = null;

        List<String> list = new ArrayList<String>();
        ArrayList<JSONObject> logs = new ArrayList<JSONObject>();
        JSONParser jsonParser = new JSONParser();

        try {
            SQLConfiguration sqlConfiguration = new SQLConfiguration();
            File file = new File(sqlConfiguration.get("input_filename"));

            br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                logs.add((JSONObject)jsonParser.parse(line));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (ParseException e) {
            e.printStackTrace();
        }
        finally {
            if(br != null) try {br.close(); } catch (IOException e) {}
        }
//        try (FileReader reader = new FileReader(file))
//        {
//            //Read JSON file
//            Object obj = jsonParser.parse(reader);
//
//            JSONArray employeeList = (JSONArray) obj;
//            System.out.println(employeeList);
//
//            //Iterate over employee array
//            employeeList.forEach( emp -> parseEmployeeObject( (JSONObject) emp ) );
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }



        return logs;
    }


    private  ArrayList<String> getORACLEQueryList() {
        ArrayList <String> queryList = new ArrayList <String>();

        String sql1 = "Select 1 from sk_wip_hst_r2r   Where lot_id = :v_lotid     and   msg_id = :v_msg_id";
        String sql2 = "Select * from output_ext_hst_r2r   where output_hst_rawid= :RAWID AND ITEM_NAME = 'METROLOGY_PRIORITY'";
        String sql3 = "UPDATE SK_WAFER_HST_R2R SET QTY = :1,LOT_START_DTTS = :2,LOT_END_DTTS = :3,SLOT_ID = :4,WAFER_START_DTTS = :5" +
                ",WAFER_END_DTTS = :6,RECIPE_ID = :7,RETICLE_ID = :8,MODULE_ID = :9,EQP_RECIPE_ID = :10,EQP_PPID = :11" +
                ",RESOURCE_TYPE = :12,PORT_ID = :13,BATCH_ID = :14,BATCH_ZONE = :15,BOAT_ZONE = :16,BOAT_SLOT_ID = :17" +
                ",BOAT_LOCATION = :18,LAST_UPDATE_BY = :19,LAST_UPDATE_DTTS=SYSTIMESTAMP  WHERE LOT_ID = :20    " +
                "AND CARRIER_ID = :21  AND WF_ID = :22       AND PRODUCT_ID = :23  AND PROCESS_ID = :24  " +
                "AND OPERATION_ID = :25  AND EQP_ID = :26        AND MODULE_NAME = :27";

        String sql4 = "INSERT INTO SK_WAFER_HST_R2R (RAWID,LINE,LOT_ID,CARRIER_ID,QTY,LOT_START_DTTS,LOT_END_DTTS,WF_ID,SLOT_ID,WAFER_START_DTTS" +
                ",WAFER_END_DTTS,PRODUCT_ID,PROCESS_ID,OPERATION_ID,EQP_ID,RECIPE_ID,RETICLE_ID,MODULE_NAME,MODULE_ID,EQP_RECIPE_ID,EQP_PPID" +
                ",RESOURCE_TYPE,PORT_ID,BATCH_ID,BATCH_ZONE,BOAT_ZONE,BOAT_SLOT_ID,BOAT_LOCATION,CREATE_BY,CREATE_DTTS)" +
                " values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,SYSTIMESTAMP)";

        String sql5 ="SELECT * FROM (" +
                "SELECT BASE_PRODUCT_ID,BASE_PROCESS_ID,BASE_OPERATION_ID,BASE_EQP_ID,BASE_RECIPE_ID " +
                "FROM SK_REF_MAP_MST_R2R A,         AREA_MST_PP B,EQP_MST_PP C" +
                " WHERE     A.LINE = :LINE AND A.AREA_RAWID = B.RAWID  AND A.SUB_EQP_RAWID = C.RAWID AND A.SUB_PRODUCT_ID IN ('*', :PRODUCT) " +
                " AND A.SUB_PROCESS_ID IN ('*', :PROCESS) AND A.SUB_OPERATION_ID IN ('*', :OPERATION) AND A.SUB_RECIPE_ID IN ('*', :RECIPE)" +
                " AND B.AREA = :AREA AND C.EQP_ID = :EQP ORDER BY DECODE( SUB_PRODUCT_ID, '*', SUB_PRODUCT_ID) ,DECODE( SUB_PROCESS_ID, '*', SUB_PROCESS_ID) " +
                ",DECODE( SUB_OPERATION_ID, '*', SUB_OPERATION_ID),DECODE( SUB_RECIPE_ID, '*', SUB_RECIPE_ID) ) WHERE ROWNUM=1";

        String sql6 = "Select 1 from lot_hst_r2r   Where lot_id = :v_lotid     and   eqp_id = :v_eqp_id    and   operation_id = :v_step_id   and   line= :v_fromFab";
        String sql7 = "/* R2RTransportExecuterNew.java-isExistWipData-20180524 */  SELECT 1 FROM SK_WIP_HST_R2R" +
                "  WHERE LOT_ID = :V_LOT_ID  AND EVENT_NAME = :V_EVENT_NAME  AND EVENT_DTTS = :V_EVENT_DTTS";
        String sql8 = "/* APCAPP_BISTel.PeakPerformance.R2R.Custom.API.Common.Def_20180109_01 */ SELECT  /*+ NO_MERGE(RMS_RECIPE_MST_VW.EVR)  */           PPID                                        FROM RMS_RECIPE_MST_VW                           WHERE RM_MODEL IN (:EQP_ID, 'MODEL')                AND CHAMBER_ID = :CHAMBER_ID           AND RECIPE_ID = :RECIPE_ID             AND PPID = :PROCESS_RECIPEID"
                ;
        String sql9 = "SELECT LINE,                   LOT_ID,                  CARRIER_ID,              QTY,                     LOT_START_DTTS,          LOT_END_DTTS,            WF_ID,                   SLOT_ID,                 WAFER_START_DTTS,        WAFER_END_dTTS,          PRODUCT_ID,              PROCESS_ID,              OPERATION_ID,            EQP_ID,                  RECIPE_ID,               RETICLE_ID,              MODULE_NAME,             MODULE_ID,               EQP_RECIPE_ID,           EQP_PPID,                RESOURCE_TYPE,           PORT_ID,                 BATCH_ID,                BATCH_ZONE,              BOAT_ZONE,               BOAT_SLOT_ID,            BOAT_LOCATION,           LAST_UPDATE_BY,          LAST_UPDATE_DTTS,        CREATE_BY,               CREATE_DTTS         FROM SK_WAFER_HST_R2R   WHERE LOT_ID = :LOT_ID    AND CARRIER_ID = :CARRIER_ID  AND PROCESS_ID = :PROCESS_ID  AND PRODUCT_ID = :PRODUCT_ID  AND OPERATION_ID = :OPERATION_ID  AND EQP_ID = :EQP_ID        AND LINE = :LINE";
        String sql10 = "SELECT E.EQP_TYPE_CD, EQ.EQP_ID, X.ITEM_NAME, X.ITEM_VALUE    FROM EQP_STATE_TRX_R2R E    INNER JOIN EQP_STATE_EXT_TRX_R2R X ON E.RAWID = X.EQP_STATE_RAWID   INNER JOIN EQP_MST_PP EQ ON EQ.RAWID = E.EQP_RAWID   WHERE E.START_CHNAGE_DTTS <= :v_time     AND E.END_CHANGE_DTTS >= :v_time1     AND E.STATE_TYPE_CD = :v_statetype  ORDER BY E.EQP_TYPE_CD, EQ.EQP_ID, X.ITEM_NAME";
        String sql11 = "SELECT /*+ OPT_PARAM('_B_TREE_BITMAP_PLANS','FALSE') \"APCAPP_ETCH_NORMAL_MODEL_20170718\" */                                LOT_ID AS PLASMA_LOT                                                                                                   , INPUT_NAME AS PLASMA_PARAM                                                                                            , INPUT_VALUE AS PLASMA_VALUE                                                                                           , ROUND (AVG (INPUT_VALUE) OVER (PARTITION BY INPUT_NAME), 2) AS INPUT_VALUE                                         FROM (  SELECT ROW_NUMBER () OVER (ORDER BY LOT.TRACK_IN_DTTS DESC) SK                                                               , LOT.LOT_ID AS LOT_ID                                                                                                  , INPUT.INPUT_NAME AS INPUT_NAME                                                                                        , ROUND (AVG (TO_NUMBER (INPUT.INPUT_VALUE)), 2) AS INPUT_VALUE                                                      FROM (SELECT /*+ INDEX(LOT IDX_LOT_HST_R2R_02) */                                                                                   RAWID                                                                                                                  , LOT_ID                                                                                                                , TRACK_IN_DTTS                                                                                                      FROM LOT_HST_R2R                                                                                                       WHERE EQP_ID = :V_EQID                                                                                                    AND CREATE_DTTS BETWEEN SYSDATE - 1 AND SYSDATE                                                                         AND ( (SYSDATE - 1 > TO_TIMESTAMP (:V_RESETTIME1, 'YYYYMMDD HH24MISS.FF')                                                  AND TRACK_IN_DTTS > SYSDATE - 1)                                                                                       OR (SYSDATE - 1 < TO_TIMESTAMP (:V_RESETTIME2, 'YYYYMMDD HH24MISS.FF')                                                  AND TRACK_IN_DTTS > TO_TIMESTAMP (:V_RESETTIME3, 'YYYYMMDD HH24MISS.FF')))) LOT                                   JOIN INPUT_HST_R2R INPUT                                                                                                    ON (LOT.RAWID = INPUT.LOT_HST_RAWID                                                                                     AND INPUT.SUBSTRATE_ID <> '-'                                                                                           AND INPUT.INPUT_NAME = REPLACE (:V_INPUT1, REGEXP_SUBSTR (:V_INPUT2, '[^/]*$'), 'Plasma_On_Time')                       AND INPUT.CREATE_DTTS BETWEEN SYSDATE - 1 AND SYSDATE)                                                     GROUP BY LOT.LOT_ID                                                                                                            , LOT.TRACK_IN_DTTS                                                                                                     , INPUT.INPUT_NAME)                                                                                       WHERE SK < 6";
        String sql12 = "SELECT NVL (SUM(WF_QTY), 0) AS WFQTY  FROM MES_LOT_VW  WHERE EQP_ID = :l_eqpId  AND LOT_STAT_TYP = 'Released'  AND MES_PROC_STAT_CD IN ('LOAD', 'PROC')  AND LOT_ID != :l_lotId";

        String sql13 = "SELECT C.RECIPE_PARA, C.LOT_COUNT , C.USE_LOT AS MAX_LOT_COUNT , C.USE_LOT_ACTION , C.USE_FIRST_SETMONITOR , C.USE_DAYS AS MAX_DAYS, " +
                "CASE WHEN NVL (D.ENGR_TIME, TO_TIMESTAMP (:RESET_TIME1, 'YYYYMMDD HH24MISS.FF3')) > C.ENGR_TIME THEN  TO_CHAR(NVL (D.ENGR_TIME, TO_TIMESTAMP (:RESET_TIME2, 'YYYYMMDD HH24MISS.FF3')), 'YYYYMMDD HH24MISS.FF3')" +
                " ELSE  TO_CHAR(C.ENGR_TIME, 'YYYYMMDD HH24MISS.FF3')  END AS ENGR_TIME" +
                " , CASE   " +
                " WHEN NVL (D.ENGR_TIME, TO_TIMESTAMP (:RESET_TIME3, 'YYYYMMDD HH24MISS.FF3')) > C.ENGR_TIME THEN D.ENGR_VALUE " +
                " ELSE C.ENGR_VALUE END AS ENGR_VALUE" +
                " , D.ENGR_VALUE AS DEV, C.ENGR_VALUE AS CEV , TO_CHAR(NVL (D.ENGR_TIME, TO_TIMESTAMP (:RESET_TIME4, 'YYYYMMDD HH24MISS.FF3')), 'YYYYMMDD HH24MISS.FF3') AS DET" +
                "  , TO_CHAR(C.ENGR_TIME, 'YYYYMMDD HH24MISS.FF3') AS CET " +
                "  FROM (SELECT A.RECIPE_PARA , A.LOT_COUNT , B.USE_LOT , B.USE_LOT_ACTION , B.USE_FIRST_SETMONITOR , B.USE_DAYS , B.ENGR_VALUE" +
                " , NVL(B.ENGR_TIME, TO_TIMESTAMP (:RESET_TIME5, 'YYYYMMDD HH24MISS.FF3')) AS ENGR_TIME " +
                " , B.RAWID , A.STATE_KEY  " +
                " FROM (  SELECT REGEXP_SUBSTR (REGEXP_SUBSTR (INQ_EXT.ITEM_VALUE, '(EQP_ID=)[^;]+', 1, 1), '[^=]+', 1, 2) AS EQP_ID ," +
                " REGEXP_SUBSTR (REGEXP_SUBSTR (INQ_EXT.ITEM_VALUE, '(PROCESS_ID=)[^;]+', 1, 1), '[^=]+', 1, 2) AS PROCESS_ID ," +
                " REGEXP_SUBSTR (REGEXP_SUBSTR (INQ_EXT.ITEM_VALUE, '(OPERATION_ID=)[^;]+', 1, 1), '[^=]+', 1, 2) AS OPERATION_ID " +
                " , REGEXP_SUBSTR (REGEXP_SUBSTR (INQ_EXT.ITEM_VALUE, '(RECIPE_ID=)[^;]+', 1, 1), '[^=]+', 1, 2) AS RECIPE_ID" +
                " , REGEXP_SUBSTR (REGEXP_SUBSTR (INQ_EXT.ITEM_VALUE, '(RECIPE_PARA=)[^;]+', 1, 1), '[^=]+', 1, 2) AS RECIPE_PARA" +
                " , COUNT (DISTINCT LOT_ID) AS LOT_COUNT, INQ_EXT.ITEM_VALUE AS STATE_KEY FROM LOT_HST_R2R LOT " +
                " , INQUIRY_HST_R2R INQ  , INQUIRY_EXT_HST_R2R INQ_EXT  WHERE LOT.EQP_ID = :EQP_ID " +
                " AND NVL (LOT.LAST_UPDATE_DTTS, LOT.CREATE_DTTS) BETWEEN TO_TIMESTAMP (:RESET_TIME6, 'YYYYMMDD HH24MISS.FF3') AND SYSDATE + 0.001 AND INQ.LOT_HST_RAWID = LOT.RAWID" +
                "  AND INQ.INPUT_NAME = :INPUT_NAME AND INQ.JOB_PRE_DTTS BETWEEN TO_TIMESTAMP (:RESET_TIME7, 'YYYYMMDD HH24MISS.FF3') AND SYSDATE + 0.001 " +
                "  AND INQ.JOB_PRE_VALUE IS NOT NULL AND INQ.R2R_STATUS = 'TFIX' AND INQ.CREATE_DTTS BETWEEN LOT.CREATE_DTTS AND SYSDATE + 0.001" +
                "  AND INQ_EXT.INQUIRY_HST_RAWID = INQ.RAWID AND INQ_EXT.ITEM_NAME = 'STATEKEY' AND INQ_EXT.ITEM_VALUE = :STATE_KEY" +
                "  AND INQ_EXT.CREATE_DTTS BETWEEN INQ.CREATE_DTTS AND SYSDATE + 0.001 AND LOT.CREATE_DTTS >= TO_TIMESTAMP (:RESET_TIME8, 'YYYYMMDD HH24MISS.FF3')-1" +
                "  GROUP BY INQ_EXT.ITEM_VALUE) A, SK_PARAM_SPEC_RECIPE_R2R B WHERE B.EQP_ID = A.EQP_ID" +
                "  AND B.PROCESS_ID = A.PROCESS_ID AND B.OPERATION_ID = A.OPERATION_ID AND B.RECIPE_ID = A.RECIPE_ID" +
                "  AND B.CONTROL_PARAM_NAME = A.RECIPE_PARA AND B.USE_FLAG = 'Y') C " +
                "  ,SK_PARAM_SPEC_RECIPE_EXT_R2R D WHERE D.PARAM_SPEC_RECIPE_RAWID(+) = C.RAWID AND D.STATE_KEY(+) = C.STATE_KEY" ;
        //"FFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFF\uFFFFÿ";
        String sql14 = "SELECT  HST.STATE_RAWID, HST.RAWID, HST.LOT_ID, TO_CHAR(HST.PROCESS_DTTS, 'YYYYMMDD HH24MISS.FF3') AS PROCESS_DTTS, HST.STATE_HST_VALUE, HST.LOT_TYPE_CD, HST.LOT_HST_RAWID, TRX.STATE_KEY_VALUE  FROM STATE_TRX_R2R TRX  INNER JOIN FEEDBACK_STATE_TRX_R2R HST  ON TRX.RAWID = HST.STATE_RAWID    AND TRX.MODEL_NAME = :V_MODELNAME    AND TRX.ACTION_MODE_CD = :V_ACTION_MODE_CD    AND TRX.STATE_TYPE_CD = :V_STATE_TYPE_CD    AND NOT EXISTS ( SELECT EXL.LOT_ID FROM SK_EXCLUDE_WF_TRX_R2R EXL, LOT_HST_R2R LOT , AREA_MST_PP AMP WHERE        EXL.LOT_ID = LOT.LOT_ID AND LOT.RAWID = HST.LOT_HST_RAWID AND NVL(EXL.WF_ID, HST.LOT_ID) = HST.LOT_ID    AND EXL.ACTIVE_YN = :ACTIVE_YN1 AND EXL.FEEDBACK_YN = :FEEDBACK_YN1 AND EXL.OPERATION_ID = LOT.OPERATION_ID AND NVL (EXL.LAST_UPDATE_DTTS, EXL.CREATE_DTTS) BETWEEN SYSDATE - :EXCLUDE_TIME AND SYSDATE    AND AMP.RAWID = EXL.AREA_RAWID AND AMP.AREA = LOT.AREA AND LOT.CREATE_DTTS <= HST.CREATE_DTTS )    AND HST.LOT_TYPE_CD = :V_LOT_TYPE_CD  AND HST.PROCESS_DTTS > TO_TIMESTAMP(:V_TRACK_IN_TIME, 'YYYYMMDD HH24MISS.FF3')  AND HST.LOT_TYPE_CD = :V_LOT_TYPE  AND (  TRX.STATE_KEY_VALUE = :V_KEY0 )  ORDER BY HST.PROCESS_DTTS DESC, HST.LOT_ID";
        String sql15 = "SELECT DISTINCT LOT_ID                  FROM MES_LOTMOVE_VW l, MES_OPERATION_VW s   WHERE     l.TIMEKEY >= :v_fromtime                AND l.TIMEKEY < :v_totime                   AND l.FAC_ID = :fromFab                      AND L.MOVE_FAC_ID = :toFab1                  AND L.OPER_ID = S.OPERATION_ID               AND S.LINE = :toFab2";


        //:RECIPE_ID
        String sql16 = "SELECT LOT_ID, INPUT_NAME , JOB_PRE_VALUE, R2R_VALUE , INPUT_VALUE FROM " +
                "(SELECT ROW_NUMBER () OVER (PARTITION BY IQH.INPUT_NAME ORDER BY IQH.CREATE_DTTS DESC) SK" +
                " , LHR.LOT_ID AS LOT_ID , IQH.INPUT_NAME , TO_NUMBER (IQH.JOB_PRE_VALUE) AS JOB_PRE_VALUE, TO_NUMBER (IQH.R2R_VALUE) AS R2R_VALUE " +
                ", TO_NUMBER (IQH.INPUT_VALUE) AS INPUT_VALUE FROM (SELECT RAWID, LOT_ID FROM LOT_HST_R2R WHERE EQP_ID = :EQP_ID" +
                " AND RECIPE_ID IN (SELECT :RECIPE_ID FROM DUAL UNION ALL SELECT SUB_RECIPE_ID FROM SK_REF_MAP_MST_R2R WHERE BASE_EQP_ID = :EQP_ID1" +
                " AND BASE_RECIPE_ID = :RECIPE_ID1) AND CREATE_DTTS > TO_TIMESTAMP (V_RESETTIME1, 'YYYYMMDD HH24MISS.FF')) LHR JOIN INQUIRY_HST_R2R IQH ON IQH.LOT_HST_RAWID = LHR.RAWID" +
                " AND IQH.CREATE_DTTS > TO_TIMESTAMP (V_RESETTIME2, 'YYYYMMDD HH24MISS.FF') AND IQH.PROCESS_USE_YN = 'Y' WHERE 1 = 1 " +
                " AND IQH.INPUT_NAME IS NOT NULL AND IQH.INPUT_VALUE IS NOT NULL AND IQH.JOB_PRE_VALUE IS NOT NULL )" +
                " WHERE SK = 1";

        String sql17 = "SELECT  HST.STATE_RAWID, HST.RAWID, HST.LOT_ID, TO_CHAR(HST.PROCESS_DTTS, 'YYYYMMDD HH24MISS.FF3') AS PROCESS_DTTS," +
                " HST.STATE_HST_VALUE, HST.LOT_TYPE_CD, HST.LOT_HST_RAWID, TRX.STATE_KEY_VALUE  FROM" +
                " STATE_TRX_R2R TRX  INNER JOIN FEEDBACK_STATE_TRX_R2R HST  ON TRX.RAWID = HST.STATE_RAWID    AND TRX.MODEL_NAME = :V_MODELNAME" +
                " AND TRX.ACTION_MODE_CD = :V_ACTION_MODE_CD    AND TRX.STATE_TYPE_CD = :V_STATE_TYPE_CD    AND NOT EXISTS" +
                " ( SELECT EXL.LOT_ID FROM SK_EXCLUDE_WF_TRX_R2R EXL, LOT_HST_R2R LOT , AREA_MST_PP AMP " +
                "WHERE EXL.LOT_ID = LOT.LOT_ID AND LOT.RAWID = HST.LOT_HST_RAWID AND NVL(EXL.WF_ID, HST.LOT_ID) = HST.LOT_ID" +
                "    AND EXL.ACTIVE_YN = :ACTIVE_YN1 AND EXL.FEEDBACK_YN = :FEEDBACK_YN1 AND NVL (EXL.LAST_UPDATE_DTTS, EXL.CREATE_DTTS)" +
                " BETWEEN SYSDATE - :EXCLUDE_TIME AND SYSDATE    AND AMP.RAWID = EXL.AREA_RAWID AND AMP.AREA = LOT.AREA AND LOT.CREATE_DTTS <= HST.CREATE_DTTS )" +
                "  AND HST.PROCESS_DTTS > SYSTIMESTAMP - 45  AND (  TRX.STATE_KEY_VALUE = :V_KEY0 )  ORDER BY HST.PROCESS_DTTS DESC, HST.LOT_ID";

        String sql18 = "SELECT EQP_ID, EQP_STAT_CD, MES_STAT_TYP, DECODE(EVENT_DTTS, NULL, LAST_EVENT_TIMEKEY,  TO_CHAR(EVENT_DTTS, 'YYYYMMDDHH24MISS'))" +
                " AS LAST_EVENT_TIMEKEY from (  SELECT ROW_NUMBER() over (order by B.EVENT_DTTS desc) AS RK" +
                " , A.* , B.EVENT_DTTS FROM " +
                "( SELECT EQP_ID, EQP_STAT_CD, MES_STAT_TYP,  LAST_EVENT_TIMEKEY FROM MES_EQPMASEXT_VW " +
                "WHERE 1=1 AND EQP_ID = :V_EQP_ID ) A" +
                ", SK_EQP_HST_R2R B WHERE A.EQP_ID = B.EQP_ID(+) AND A.EQP_STAT_CD = B.EVENT_NAME(+)" +
                " ) WHERE RK = 1";
        String sql19 = "SELECT MAIN_PROCESS_ID               FROM MES_PRODUCT_VW               WHERE PRODUCT_ID = :PRODUCT_ID       AND PRODUCT_VER = :PRODUCT_VER     AND LINE = :LINE";


        String sql20 = "SELECT /*+ INDEX(D IDX_SETUP_DATA_TRX_R2R_01)*/ REGEXP_SUBSTR (REGEXP_SUBSTR (B.SETUP_KEY_VALUE , '(PROCESS_ID=)[^;]+', 1, 1), '[^=]+', 2, 2) AS PROCESS_ID" +
                " FROM SETUP_KEY_MST_R2R A , SETUP_KEY_VALUE_MST_R2R B , SETUP_SCHEMA_MST_R2R C  , SETUP_DATA_TRX_R2R D , SETUP_TRX_R2R E WHERE A.SETUP_KEY_NAME = :SETUP_KEY_NAME" +
                " AND B.SETUP_KEY_VALUE LIKE REPLACE(:SETUP_KEY_VALUE, '_', ':') ESCAPE ':'    AND B.SETUP_KEY_RAWID = A.RAWID AND C.SETUP_KEY_RAWID = A.RAWID " +
                " AND C.SETUP_SCHEMA_NAME = :SETUP_SCHEMA_NAME AND D.SETUP_KEY_VALUE_RAWID = B.RAWID AND D.SETUP_SCHEMA_RAWID = C.RAWID AND " +
                " NVL(REGEXP_SUBSTR (REGEXP_SUBSTR (D.SETUP_DATA_VALUE, '(DEFAULT_FLAG=)[^;]+', 1, 1), '[^=]+', 2, 2), 'N') = :P_DEFAULT_FLAG " +
                " AND E.SETUP_SCHEMA_RAWID = D.SETUP_SCHEMA_RAWID  AND E.CATEGORY_VALUE = :CATEGORY_VALUE ORDER BY " +
                " REGEXP_SUBSTR (REGEXP_SUBSTR (B.SETUP_KEY_VALUE , '(PROCESS_ID=)[^;]+', 1, 1), '[^=]+', 2, 2) ASC";


        String sql21 = "SELECT /*+ USE_NL(INQUIRY EX) */       inquiry.rawid, inquiry.lot_hst_rawid, inquiry.model_name, inquiry.model_status, inquiry.request_dtts, inquiry.unit,        inquiry.reply_dtts, inquiry.time_interval, inquiry.substrate_id, inquiry.unit_id, inquiry.input_name,        inquiry.r2r_value, inquiry.r2r_dtts, inquiry.r2r_default_value, inquiry.r2r_upper_limit,        inquiry.r2r_lower_limit, inquiry.r2r_status, inquiry.process_use_yn, inquiry.reply_yn, inquiry.job_pre_value,        inquiry.job_pre_dtts, inquiry.user_message, inquiry.system_message, inquiry.exception_message,        inquiry.input_value,        inquiry.last_update_by, inquiry.last_update_dtts, inquiry.create_by, inquiry.create_dtts,        ex.rawid exrawid, ex.item_name, ex.item_value  FROM INQUIRY_HST_R2R inquiry   LEFT OUTER JOIN INQUIRY_EXT_HST_R2R ex     ON inquiry.rawid = ex.inquiry_hst_rawid AND (ex.CREATE_DTTS BETWEEN SYSDATE - 85 AND SYSDATE + 0.001)  WHERE inquiry.lot_hst_rawid = :1 AND (inquiry.CREATE_DTTS BETWEEN SYSDATE - 85 AND SYSDATE + 0.001)  ORDER BY input_name, model_name, substrate_id, inquiry.create_dtts DESC";


        //test sql
        String sql30 = "select * from ( select a , b from test1, test2 ) k";

        queryList.add(sql1);
        queryList.add(sql2);
        queryList.add(sql3);
        queryList.add(sql4);
        queryList.add(sql5);
        queryList.add(sql6);
        queryList.add(sql7);
        queryList.add(sql8);
        queryList.add(sql9);
        queryList.add(sql10);
        queryList.add(sql11);
        queryList.add(sql12);
        queryList.add(sql13);
        queryList.add(sql14);
        queryList.add(sql15);
        queryList.add(sql16);
        queryList.add(sql17);
        queryList.add(sql18);
        queryList.add(sql19);
        queryList.add(sql20);
        queryList.add(sql21);

        queryList.add(sql30);



        return queryList;
    }


}
