CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_CD2_CREATIVE()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_creative_id, v_creative_nm, v_actor_id, v_body_txt, v_call_to_action_type_dc, v_object_type_dc, v_status_dc, v_title_txt;
  var v_row_dc;

  try {
    var cur_creative = snowflake.execute(
      {sqlText: `SELECT ROW_CHANGE_DC, CREATIVE_ID, CREATIVE_NM, ACTOR_ID, BODY_TXT, CALL_TO_ACTION_TYPE_DC, OBJECT_TYPE_DC, STATUS_DC, TITLE_TXT FROM ADDATAFUSION.FACEBOOK_ADS.VW_CHANGES_TO_CD2_CREATIVE`}
    );

    while (cur_creative.next()) {
      v_row_dc = cur_creative.getColumnValue(1);
      v_creative_id = cur_creative.getColumnValue(2);
      v_creative_nm = cur_creative.getColumnValue(3);
      v_actor_id = cur_creative.getColumnValue(4);
      v_body_txt = cur_creative.getColumnValue(5);
      v_call_to_action_type_dc = cur_creative.getColumnValue(6);
      v_object_type_dc = cur_creative.getColumnValue(7);
      v_status_dc = cur_creative.getColumnValue(8);
      v_title_txt = cur_creative.getColumnValue(9);
      

      if (v_row_dc === "UPDATE") { //there is a previous record and we need update it and insert the new one
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_CREATIVE SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE CREATIVE_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_creative_id]
        });
        stmt.execute();
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_CREATIVE (CREATIVE_ID, CREATIVE_NM, ACTOR_ID, BODY_TXT, CALL_TO_ACTION_TYPE_DC, OBJECT_TYPE_DC, STATUS_DC, TITLE_TXT, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_creative_id, v_creative_nm, v_actor_id, v_body_txt, v_call_to_action_type_dc, v_object_type_dc, v_status_dc, v_title_txt]
        });
        stmt.execute();
      }

      else if (v_row_dc === "APPEND") { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_CREATIVE (CREATIVE_ID, CREATIVE_NM, ACTOR_ID, BODY_TXT, CALL_TO_ACTION_TYPE_DC, OBJECT_TYPE_DC, STATUS_DC, TITLE_TXT, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_creative_id, v_creative_nm, v_actor_id, v_body_txt, v_call_to_action_type_dc, v_object_type_dc, v_status_dc, v_title_txt]
        });
        stmt.execute();
      }  
      else if (v_row_dc === "FINAL") { // The record was removed from de Landing zone, and we need to deactivate it
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_CREATIVE SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE CREATIVE_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_creative_id]
        });
        stmt.execute();
      }
    }

    return "Procedure completed successfully.";
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;
